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
package com.soulfiremc.server.api;

import com.soulfiremc.grpc.generated.PluginApiServiceGrpc;
import com.soulfiremc.grpc.generated.PluginEvent;
import com.soulfiremc.grpc.generated.PluginEventKind;
import com.soulfiremc.grpc.generated.WatchPluginEventsRequest;
import com.soulfiremc.grpc.generated.plugin.example.v1.EchoResponse;
import com.soulfiremc.grpc.generated.plugin.example.v1.Tick;
import com.soulfiremc.server.grpc.PluginApiServiceImpl;
import com.soulfiremc.server.grpc.ServerRPCConstants;
import com.soulfiremc.server.user.PermissionContext;
import com.soulfiremc.server.user.SoulFireUser;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class PluginEventStreamTest {
  @Test
  void pausesPreserveEventsUntilTheClientRequestsMore() throws Exception {
    try (var fixture = new Fixture()) {
      var stream = fixture.subscribe();
      stream.call.request(1);
      fixture.publishTicks(5);
      assertEquals(1, stream.messages.size());

      stream.call.request(5);

      assertEquals(List.of(1L, 2L, 3L, 4L, 5L), stream.data().stream().map(PluginEvent::getSequence).toList());
      assertTrue(stream.data().stream().allMatch(event -> event.getDroppedBefore() == 0));
    }
  }

  @Test
  void eventsPublishedBeforeReadyFollowTheSnapshot() throws Exception {
    try (var fixture = new Fixture()) {
      var stream = fixture.subscribe();
      fixture.publishTicks(3);

      stream.call.request(4);

      assertEquals(4, stream.messages.size());
      assertEquals(PluginEventKind.PLUGIN_EVENT_KIND_READY, stream.messages.getFirst().getKind());
      assertEquals(0, stream.messages.getFirst().getSequence());
      assertEquals(List.of(1L, 2L, 3L), stream.data().stream().map(PluginEvent::getSequence).toList());
    }
  }

  @Test
  void eventCapacityRetainsTheNewestEventsAndReportsTheEvictedPrefix() throws Exception {
    try (var fixture = new Fixture()) {
      var stream = fixture.subscribe();
      fixture.publishTicks(400);

      stream.call.request(401);

      var data = stream.data();
      assertTrue(!data.isEmpty() && data.size() < 400);
      assertEquals(400, data.getLast().getSequence());
      assertEquals(400 - data.size(), data.getFirst().getDroppedBefore());
      assertEquals(data.getFirst().getSequence() - 1, data.getFirst().getDroppedBefore());
      assertTrue(data.subList(1, data.size()).stream().allMatch(event -> event.getDroppedBefore() == 0));
    }
  }

  @Test
  void byteCapacityAlsoBoundsLargePayloads() throws Exception {
    try (var fixture = new Fixture()) {
      var stream = fixture.subscribe();
      var payload = "x".repeat(200_000);
      for (var i = 0; i < 10; i++) {
        fixture.large.publish(PluginEventTarget.instance(fixture.instanceId), EchoResponse.newBuilder().setMessage(payload).build());
      }

      stream.call.request(11);

      var data = stream.data();
      assertTrue(!data.isEmpty() && data.size() < 10);
      assertEquals(10, data.getLast().getSequence());
      assertEquals(10 - data.size(), data.getFirst().getDroppedBefore());
    }
  }

  @Test
  void oversizedWaitingEventsReportTheirLossAfterOlderQueuedEvents() throws Exception {
    try (var fixture = new Fixture()) {
      var stream = fixture.subscribe();
      fixture.publishTicks(3);
      fixture.large.publish(PluginEventTarget.instance(fixture.instanceId), EchoResponse.newBuilder().setMessage("x".repeat(1_100_000)).build());
      fixture.publishTicks(1);

      stream.call.request(6);

      assertEquals(List.of(1L, 2L, 3L, 5L), stream.data().stream().map(PluginEvent::getSequence).toList());
      assertEquals(List.of(0L, 0L, 0L, 1L), stream.data().stream().map(PluginEvent::getDroppedBefore).toList());
    }
  }

  @Test
  void largeEventsCanStillBeSentDirectlyToReadyClients() throws Exception {
    try (var fixture = new Fixture()) {
      var stream = fixture.subscribe();
      stream.call.request(2);

      fixture.large.publish(PluginEventTarget.instance(fixture.instanceId), EchoResponse.newBuilder().setMessage("x".repeat(1_100_000)).build());

      assertEquals(1, stream.data().size());
      assertEquals(0, stream.data().getFirst().getDroppedBefore());
    }
  }

  @Test
  void queuedEventsAreNotSentAfterPermissionRevocation() throws Exception {
    try (var fixture = new Fixture()) {
      var stream = fixture.subscribe();
      stream.call.request(1);
      fixture.publishTicks(3);
      fixture.allowed.set(false);
      stream.call.request(4);
      assertEquals(1, stream.messages.size());

      fixture.allowed.set(true);
      fixture.publishTicks(1);

      assertEquals(List.of(4L), stream.data().stream().map(PluginEvent::getSequence).toList());
      assertEquals(0, stream.data().getFirst().getDroppedBefore());
    }
  }

  @Test
  void filteredScopesDoNotEnterTheQueueOrInflateDropCounts() throws Exception {
    try (var fixture = new Fixture()) {
      var stream = fixture.subscribe();
      fixture.ticks.publish(PluginEventTarget.instance(UUID.randomUUID()), Tick.getDefaultInstance());
      fixture.publishTicks(1);

      stream.call.request(2);

      assertEquals(List.of(2L), stream.data().stream().map(PluginEvent::getSequence).toList());
      assertEquals(0, stream.data().getFirst().getDroppedBefore());
    }
  }

  @Test
  void reentrantRequestsDrainPendingEventsOnceAndInOrder() throws Exception {
    try (var fixture = new Fixture()) {
      var stream = fixture.subscribe();
      fixture.publishTicks(5);
      stream.onMessage = _ -> stream.call.request(1);

      stream.call.request(1);

      assertEquals(6, stream.messages.size());
      assertEquals(List.of(1L, 2L, 3L, 4L, 5L), stream.data().stream().map(PluginEvent::getSequence).toList());
    }
  }

  @Test
  void cancellationOfAPausedSubscriberDoesNotAffectOtherClients() throws Exception {
    try (var fixture = new Fixture()) {
      var paused = fixture.subscribe();
      var healthy = fixture.subscribe();
      healthy.call.request(10);
      fixture.publishTicks(3);

      paused.call.cancel("finished", null);
      fixture.publishTicks(2);

      assertTrue(paused.messages.isEmpty());
      assertEquals(List.of(1L, 2L, 3L, 4L, 5L), healthy.data().stream().map(PluginEvent::getSequence).toList());
    }
  }

  @Test
  void readyClientsReceiveEventsWithoutWaitingForAnotherReadinessCallback() throws Exception {
    try (var fixture = new Fixture()) {
      var stream = fixture.subscribe();
      stream.call.request(10);

      fixture.publishTicks(5);

      assertEquals(5, stream.data().size());
      assertEquals(0, stream.data().getLast().getDroppedBefore());
    }
  }

  private static final class Subscription implements ClientResponseObserver<WatchPluginEventsRequest, PluginEvent> {
    final List<PluginEvent> messages = new CopyOnWriteArrayList<>();
    ClientCallStreamObserver<WatchPluginEventsRequest> call;
    Consumer<PluginEvent> onMessage = _ -> {};

    @Override public void beforeStart(ClientCallStreamObserver<WatchPluginEventsRequest> stream) { call = stream; stream.disableAutoRequestWithInitial(0); }
    @Override public void onNext(PluginEvent value) { messages.add(value); onMessage.accept(value); }
    @Override public void onError(Throwable error) { }
    @Override public void onCompleted() { }
    List<PluginEvent> data() { return messages.stream().filter(event -> event.getKind() == PluginEventKind.PLUGIN_EVENT_KIND_DATA).toList(); }
  }

  private static final class Fixture implements AutoCloseable {
    final UUID instanceId = UUID.randomUUID();
    final AtomicBoolean allowed = new AtomicBoolean(true);
    final PluginEventRegistration<Tick> ticks;
    final PluginEventRegistration<EchoResponse> large;
    final Server server;
    final ManagedChannel channel;

    Fixture() throws Exception {
      var registry = new PluginApiRegistry();
      var context = registry.createContext(new PluginInfo("example", "1.0.0", "Example", "SoulFireMC", "AGPL-3.0", ""));
      var permission = context.permissions().register(PluginPermission.instance("read", "Read", "Read events", PluginPermission.Risk.READ));
      ticks = context.events().register(Tick.getDefaultInstance(), permission);
      large = context.events().register(EchoResponse.getDefaultInstance(), permission);
      var user = mock(SoulFireUser.class);
      when(user.hasPermission(any(PermissionContext.class))).thenAnswer(_ -> allowed.get());
      var name = InProcessServerBuilder.generateName();
      server = InProcessServerBuilder.forName(name).directExecutor().addService(ServerInterceptors.intercept(new PluginApiServiceImpl(registry), new ServerInterceptor() {
        @Override public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
          return Contexts.interceptCall(Context.current().withValue(ServerRPCConstants.USER_CONTEXT_KEY, user), call, headers, next);
        }
      })).build().start();
      channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    Subscription subscribe() {
      var stream = new Subscription();
      PluginApiServiceGrpc.newStub(channel).watchPluginEvents(WatchPluginEventsRequest.newBuilder().setInstanceId(instanceId.toString()).build(), stream);
      return stream;
    }

    void publishTicks(int count) {
      for (var i = 1; i <= count; i++) ticks.publish(PluginEventTarget.instance(instanceId), Tick.newBuilder().setSequence(i).build());
    }

    @Override public void close() throws Exception {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
