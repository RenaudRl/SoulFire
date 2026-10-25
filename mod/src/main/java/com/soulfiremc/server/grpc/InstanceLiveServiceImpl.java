/*
 * SoulFire
 * Copyright (C) 2026  AlexProgrammerDE
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.soulfiremc.server.grpc;

import com.soulfiremc.grpc.generated.*;
import com.soulfiremc.server.SoulFireServer;
import com.soulfiremc.server.user.PermissionContext;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/// Multiplexes the complete per-bot event contract onto one instance stream.
///
/// The internal child observers reuse BotLiveService's snapshot, filtering,
/// sequencing, backpressure, and cleanup behavior. One public gRPC stream can
/// therefore observe a fleet without maintaining a second event mapper.
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class InstanceLiveServiceImpl extends InstanceLiveServiceGrpc.InstanceLiveServiceImplBase {
  private final SoulFireServer soulFireServer;
  private final BotLiveServiceImpl botLiveService;

  public InstanceLiveServiceImpl(SoulFireServer soulFireServer) {
    this(soulFireServer, new BotLiveServiceImpl(soulFireServer));
  }

  @Override
  public void watchInstanceEvents(
    WatchInstanceEventsRequest request,
    StreamObserver<InstanceEvent> responseObserver
  ) {
    var instanceId = UUID.fromString(request.getInstanceId());
    var user = ServerRPCConstants.USER_CONTEXT_KEY.get();
    var permission = PermissionContext.instance(InstancePermission.READ_BOT_INFO, instanceId);
    user.hasPermissionOrThrow(permission);
    var instance = soulFireServer.getInstance(instanceId)
      .orElseThrow(() -> Status.NOT_FOUND
        .withDescription("Instance '%s' not found".formatted(instanceId))
        .asRuntimeException());
    var parent = (ServerCallStreamObserver<InstanceEvent>) responseObserver;
    var closed = new AtomicBoolean(false);
    var children = new ConcurrentHashMap<UUID, MultiplexingObserver>();
    var readySignals = new AtomicInteger();
    var selectedBotIds = parseBotIds(request.getFilter().getBotIdsList());
    var botFilter = botFilter(request.getFilter());
    var legacyEnvelope = !request.getFilter().hasBotEvents();

    var subscription = new AuthorizedStream(soulFireServer, user, permission, parent, closed);
    subscription.addCleanup(() -> {
      synchronized (children) {
        children.values().forEach(MultiplexingObserver::cancel);
        children.clear();
      }
    });
    Consumer<UUID> addBot = botId -> {
      if (closed.get() || parent.isCancelled()
        || (!selectedBotIds.isEmpty() && !selectedBotIds.contains(botId))) {
        return;
      }
      var previous = children.get(botId);
      if (previous != null && !previous.isCancelled()) {
        return;
      }
      var account = instance.settingsSource().accounts().get(botId);
      if (account == null) {
        return;
      }
      var child = new MultiplexingObserver(parent, closed, botId, account.lastKnownName(), legacyEnvelope);
      children.put(botId, child);
      try {
        botLiveService.watchBotEvents(
          WatchBotEventsRequest.newBuilder()
            .setInstanceId(request.getInstanceId())
            .setBotId(botId.toString())
            .setFilter(botFilter)
            .build(),
          child);
      } catch (Throwable t) {
        children.remove(botId, child);
        child.cancel();
        throw t;
      }
    };

    try {
      parent.setOnReadyHandler(() -> {
        if (closed.get() || parent.isCancelled() || readySignals.getAndIncrement() != 0) return;
        // A transport may invoke onReady inside onNext while the parent monitor is held.
        // Fan out asynchronously so a write never acquires another child's monitor.
        soulFireServer.scheduler().schedule(() -> {
          var pending = readySignals.get();
          try {
            do {
              if (!closed.get() && !parent.isCancelled()) {
                children.values().forEach(MultiplexingObserver::notifyReady);
              }
              pending = readySignals.addAndGet(-pending);
            } while (pending != 0);
          } catch (RuntimeException | Error error) {
            subscription.fail(Status.INTERNAL.withDescription("Failed to resume bot event streams").withCause(error));
            throw error;
          }
        });
      });
      parent.setOnCancelHandler(subscription::close);
      if (!subscription.start()) return;
      synchronized (children) {
        if (closed.get() || parent.isCancelled()) {
          return;
        }
        var removeStatusListener = instance.botStateManager().addStatusListener(event -> {
          synchronized (children) {
            if (closed.get()) {
              return;
            }
            if (event.removedBotId() != null) {
              var child = children.remove(event.removedBotId());
              if (child != null) {
                child.cancel();
              }
            } else if (event.status() != null) {
              addBot.accept(UUID.fromString(event.status().getProfileId()));
            }
          }
        });
        subscription.addCleanup(removeStatusListener);
        if (closed.get()) return;
        // Listen before taking the snapshot so account changes cannot fall between them.
        instance.settingsSource().accounts().keySet().forEach(addBot);
      }
    } catch (RuntimeException | Error error) {
      subscription.close();
      throw error;
    }
  }

  private static Set<UUID> parseBotIds(List<String> botIds) {
    try {
      return botIds.stream()
        .map(UUID::fromString)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT
        .withDescription("Instance event bot_ids must contain UUIDs")
        .withCause(e)
        .asRuntimeException();
    }
  }

  private static BotEventFilter botFilter(InstanceEventFilter filter) {
    if (filter.hasBotEvents()) {
      return filter.getBotEvents();
    }
    return BotEventFilter.newBuilder()
      .setIncludeChat(filter.getIncludeChat())
      .setIncludeLifecycle(filter.getIncludeLifecycle())
      .build();
  }

  private static final class MultiplexingObserver extends ServerCallStreamObserver<BotEvent> {
    private final ServerCallStreamObserver<InstanceEvent> parent;
    private final AtomicBoolean parentClosed;
    private final String botId;
    private final String botName;
    private final boolean legacyEnvelope;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private volatile Runnable cancelHandler = () -> {};
    private volatile Runnable readyHandler = () -> {};

    private MultiplexingObserver(
      ServerCallStreamObserver<InstanceEvent> parent,
      AtomicBoolean parentClosed,
      UUID botId,
      String botName,
      boolean legacyEnvelope
    ) {
      this.parent = parent;
      this.parentClosed = parentClosed;
      this.botId = botId.toString();
      this.botName = botName;
      this.legacyEnvelope = legacyEnvelope;
    }

    @Override
    public void onNext(BotEvent event) {
      var result = InstanceEvent.newBuilder()
        .setBotProfileId(botId);
      if (botName != null) {
        result.setBotName(botName);
      }
      if (legacyEnvelope && event.getEventCase() == BotEvent.EventCase.CHAT) {
        result.setChat(event.getChat());
      } else if (legacyEnvelope && event.getEventCase() == BotEvent.EventCase.LIFECYCLE) {
        result.setLifecycle(event.getLifecycle());
      } else {
        result.setBotEvent(event);
      }
      synchronized (parent) {
        if (!isCancelled() && parent.isReady()) {
          parent.onNext(result.build());
        }
      }
    }

    @Override
    public void onError(Throwable error) {
      log.debug("Bot event child stream failed for {}", botId, error);
      cancel();
    }

    @Override
    public void onCompleted() {
      cancel();
    }

    @Override
    public boolean isCancelled() {
      return parentClosed.get() || cancelled.get() || parent.isCancelled();
    }

    @Override
    public synchronized void setOnCancelHandler(Runnable handler) {
      cancelHandler = handler;
      if (cancelled.get()) {
        handler.run();
      }
    }

    @Override
    public void setCompression(String compression) {
      parent.setCompression(compression);
    }

    @Override
    public boolean isReady() {
      return !isCancelled() && parent.isReady();
    }

    @Override
    public void setOnReadyHandler(Runnable handler) {
      readyHandler = handler;
    }

    @Override
    public void disableAutoInboundFlowControl() {
      // This observer only receives server output.
    }

    @Override
    public void request(int count) {
      // This observer only receives server output.
    }

    @Override
    public void setMessageCompression(boolean enable) {
      parent.setMessageCompression(enable);
    }

    private void notifyReady() {
      if (isReady()) {
        readyHandler.run();
      }
    }

    private void cancel() {
      final Runnable handler;
      synchronized (this) {
        if (!cancelled.compareAndSet(false, true)) {
          return;
        }
        handler = cancelHandler;
      }
      handler.run();
    }
  }
}
