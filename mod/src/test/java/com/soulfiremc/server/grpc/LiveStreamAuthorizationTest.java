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

import com.soulfiremc.grpc.generated.BotEvent;
import com.soulfiremc.grpc.generated.BotEventFilter;
import com.soulfiremc.grpc.generated.BotStatus;
import com.soulfiremc.grpc.generated.WatchBotEventsRequest;
import com.soulfiremc.grpc.generated.WatchBotStatusesRequest;
import com.soulfiremc.grpc.generated.WatchBotStatusesResponse;
import com.soulfiremc.server.BotStateManager;
import com.soulfiremc.server.InstanceManager;
import com.soulfiremc.server.SoulFireScheduler;
import com.soulfiremc.server.SoulFireServer;
import com.soulfiremc.server.account.MinecraftAccount;
import com.soulfiremc.server.api.SoulFireAPI;
import com.soulfiremc.server.api.event.UserPermissionsChangedEvent;
import com.soulfiremc.server.api.event.UserSessionsInvalidatedEvent;
import com.soulfiremc.server.settings.lib.InstanceSettingsDelegate;
import com.soulfiremc.server.user.AuthSystem;
import com.soulfiremc.server.user.SoulFireUser;
import com.soulfiremc.test.utils.TestBootstrap;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class LiveStreamAuthorizationTest {
  @BeforeAll
  static void bootstrap() { TestBootstrap.bootstrapForTest(); }

  @Test
  void statusSubscriptionInstalledAfterRevocationIsImmediatelyRemoved() {
    try (var fixture = new Fixture()) {
      var out = fixture.<WatchBotStatusesResponse>observe();
      var listener = new AtomicReference<Consumer<BotStateManager.StatusEvent>>();
      var removed = new AtomicInteger();
      when(fixture.states.subscribe(any())).thenAnswer(call -> {
        listener.set(call.getArgument(0));
        fixture.revoke();
        return new BotStateManager.StatusSubscription(List.of(), removed::incrementAndGet);
      });
      fixture.watchStatuses(out);
      assertTrue(out.received.isEmpty());
      assertEquals(List.of(Status.Code.UNAUTHENTICATED), out.errors);
      assertEquals(1, removed.get());
      listener.get().accept(BotStateManager.StatusEvent.updated(fixture.status));
      assertTrue(out.received.isEmpty());
      out.close();
      assertEquals(1, removed.get());
    }
  }

  @Test
  void statusSnapshotPrecedesQueuedUpdatesAndRevocationStopsFurtherUpdates() {
    try (var fixture = new Fixture()) {
      var out = fixture.<WatchBotStatusesResponse>observe();
      var listener = new AtomicReference<Consumer<BotStateManager.StatusEvent>>();
      when(fixture.states.subscribe(any())).thenAnswer(call -> {
        Consumer<BotStateManager.StatusEvent> value = call.getArgument(0);
        listener.set(value);
        value.accept(BotStateManager.StatusEvent.updated(fixture.status));
        return new BotStateManager.StatusSubscription(List.of(), () -> listener.set(null));
      });
      fixture.watchStatuses(out);
      assertEquals(2, out.received.size());
      assertTrue(out.received.getFirst().hasSnapshot());
      assertEquals(fixture.status, out.received.getLast().getUpdate());
      var stale = listener.get();
      fixture.revoke();
      stale.accept(BotStateManager.StatusEvent.removed(fixture.botId));
      assertEquals(2, out.received.size());
      assertEquals(List.of(Status.Code.UNAUTHENTICATED), out.errors);
    }
  }

  @Test
  void botEventRevocationDuringListenerSetupRemovesLateResources() {
    try (var fixture = new Fixture()) {
      var out = fixture.<BotEvent>observe();
      var removed = new AtomicInteger();
      var listener = new AtomicReference<Consumer<BotStateManager.StatusEvent>>();
      when(fixture.states.addStatusListener(any())).thenAnswer(call -> {
        listener.set(call.getArgument(0));
        fixture.revoke();
        return (Runnable) removed::incrementAndGet;
      });
      fixture.watchEvents(out);
      var received = out.received.size();
      listener.get().accept(BotStateManager.StatusEvent.updated(fixture.status));
      SoulFireAPI.postEvent(new UserPermissionsChangedEvent(fixture.server, fixture.owner));
      assertEquals(received, out.received.size());
      assertEquals(1, removed.get());
      assertEquals(1, fixture.lookups.get());
      assertEquals(List.of(Status.Code.UNAUTHENTICATED), out.errors);
    }
  }

  @Test
  void cancellationBeforeBotEventSetupAvoidsAuthAndListenerInstallation() {
    try (var fixture = new Fixture()) {
      var out = fixture.<BotEvent>observe();
      doAnswer(call -> { call.<Runnable>getArgument(0).run(); return null; })
        .when(out.observer).setOnCancelHandler(any());
      fixture.watchEvents(out);
      assertEquals(0, fixture.lookups.get());
      assertTrue(out.received.isEmpty());
      assertTrue(out.errors.isEmpty());
      verify(fixture.states, never()).addStatusListener(any());
    }
  }

  private static final class Observed<T> implements AutoCloseable {
    final ServerCallStreamObserver<T> observer = mock();
    final List<T> received = new ArrayList<>();
    final List<Status.Code> errors = new ArrayList<>();
    Runnable cancel = () -> {};

    Observed() {
      when(observer.isReady()).thenReturn(true);
      doAnswer(call -> { received.add(call.getArgument(0)); return null; }).when(observer).onNext(any());
      doAnswer(call -> { errors.add(Status.fromThrowable(call.getArgument(0)).getCode()); return null; }).when(observer).onError(any());
      doAnswer(call -> { cancel = call.getArgument(0); return null; }).when(observer).setOnCancelHandler(any());
    }

    @Override
    public void close() { cancel.run(); }
  }

  private static final class Fixture implements AutoCloseable {
    final UUID instanceId = UUID.randomUUID();
    final UUID botId = UUID.randomUUID();
    final UUID owner = UUID.randomUUID();
    final Instant issuedAt = Instant.now();
    final SoulFireServer server = mock();
    final AuthSystem auth = mock();
    final SoulFireUser user = mock();
    final InstanceManager instance = mock();
    final BotStateManager states = mock();
    final BotStatus status = BotStatus.newBuilder().setProfileId(botId.toString()).build();
    final AtomicInteger lookups = new AtomicInteger();
    final List<Observed<?>> outputs = new ArrayList<>();
    final Context context;

    Fixture() {
      when(server.authSystem()).thenReturn(auth);
      when(server.scheduler()).thenReturn(mock(SoulFireScheduler.class));
      when(server.getInstance(instanceId)).thenReturn(Optional.of(instance));
      when(instance.botStateManager()).thenReturn(states);
      when(instance.botConnections()).thenReturn(Map.of());
      var settings = mock(InstanceSettingsDelegate.class);
      when(settings.accounts()).thenReturn(Map.of(botId, mock(MinecraftAccount.class)));
      when(instance.settingsSource()).thenReturn(settings);
      when(states.status(botId)).thenReturn(status);
      when(user.getUniqueId()).thenReturn(owner);
      when(user.getIssuedAt()).thenReturn(issuedAt);
      when(user.hasPermission(any())).thenReturn(true);
      when(auth.authenticateBySubject(owner, issuedAt, 0L)).thenAnswer(_ -> { lookups.incrementAndGet(); return Optional.of(user); });
      context = Context.current().withValue(ServerRPCConstants.USER_CONTEXT_KEY, user);
    }

    <T> Observed<T> observe() {
      var out = new Observed<T>();
      outputs.add(out);
      return out;
    }

    void revoke() { SoulFireAPI.postEvent(new UserSessionsInvalidatedEvent(server, owner, 1L)); }

    void watchStatuses(Observed<WatchBotStatusesResponse> out) {
      context.run(() -> new BotServiceImpl(server).watchBotStatuses(WatchBotStatusesRequest.newBuilder()
        .setInstanceId(instanceId.toString()).build(), out.observer));
    }

    void watchEvents(Observed<BotEvent> out) {
      context.run(() -> new BotLiveServiceImpl(server).watchBotEvents(WatchBotEventsRequest.newBuilder()
        .setInstanceId(instanceId.toString()).setBotId(botId.toString())
        .setFilter(BotEventFilter.newBuilder().setIncludeChat(true).setIncludeLifecycle(true)).build(), out.observer));
    }

    @Override
    public void close() { outputs.forEach(Observed::close); }
  }
}
