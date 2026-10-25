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

import com.soulfiremc.grpc.generated.AcquireBotControlRequest;
import com.soulfiremc.grpc.generated.AcquireBotControlResponse;
import com.soulfiremc.grpc.generated.BotControlLease;
import com.soulfiremc.grpc.generated.BotEvent;
import com.soulfiremc.grpc.generated.BotStatus;
import com.soulfiremc.grpc.generated.RenewBotControlRequest;
import com.soulfiremc.grpc.generated.RenewBotControlResponse;
import com.soulfiremc.grpc.generated.WatchBotEventsRequest;
import com.soulfiremc.server.BotStateManager;
import com.soulfiremc.server.InstanceManager;
import com.soulfiremc.server.SoulFireScheduler;
import com.soulfiremc.server.SoulFireServer;
import com.soulfiremc.server.account.MinecraftAccount;
import com.soulfiremc.server.bot.BotControlLeaseManager;
import com.soulfiremc.server.settings.lib.InstanceSettingsDelegate;
import com.soulfiremc.server.user.AuthSystem;
import com.soulfiremc.server.user.SoulFireUser;
import com.soulfiremc.test.utils.TestBootstrap;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class BotLiveTimingTest {
  @BeforeAll
  static void bootstrap() { TestBootstrap.bootstrapForTest(); }

  static Stream<Arguments> leaseDurations() {
    return Stream.of(Arguments.of(0L, 30), Arguments.of(1L, 5), Arguments.of(6L, 6),
      Arguments.of(300L, 300), Arguments.of(0x7fffffffL, 300),
      Arguments.of(0x80000000L, 300), Arguments.of(0xffffffffL, 300));
  }

  @ParameterizedTest
  @MethodSource("leaseDurations")
  void acquisitionAndRenewalClampTheUnsignedLifetime(long requested, int expectedSeconds) {
    var fixture = new Fixture();
    var acquired = new ArrayList<AcquireBotControlResponse>();
    ServerCallStreamObserver<AcquireBotControlResponse> acquireObserver = mock();
    doAnswer(call -> { acquired.add(call.getArgument(0)); return null; }).when(acquireObserver).onNext(any());
    var before = Instant.now();
    fixture.context.run(() -> fixture.service.acquireBotControl(AcquireBotControlRequest.newBuilder()
      .setInstanceId(fixture.instanceId.toString()).setBotId(fixture.botId.toString())
      .setTtlSeconds((int) requested).build(), acquireObserver));
    assertEquals(1, acquired.size());
    var lease = acquired.getFirst().getLease();
    assertExpiry(lease, before, Instant.now(), expectedSeconds);
    // Reset the existing lease to the minimum so renewal must apply the requested lifetime.
    fixture.leases.renew(fixture.botId, fixture.owner, lease.getToken(), Duration.ofSeconds(5));
    var renewed = new ArrayList<RenewBotControlResponse>();
    ServerCallStreamObserver<RenewBotControlResponse> renewObserver = mock();
    doAnswer(call -> { renewed.add(call.getArgument(0)); return null; }).when(renewObserver).onNext(any());
    before = Instant.now();
    fixture.context.run(() -> fixture.service.renewBotControl(RenewBotControlRequest.newBuilder()
      .setInstanceId(fixture.instanceId.toString()).setBotId(fixture.botId.toString())
      .setToken(lease.getToken()).setTtlSeconds((int) requested).build(), renewObserver));
    assertEquals(1, renewed.size());
    assertEquals(lease.getToken(), renewed.getFirst().getLease().getToken());
    assertExpiry(renewed.getFirst().getLease(), before, Instant.now(), expectedSeconds);
  }

  static Stream<Arguments> heartbeatIntervals() {
    return Stream.of(Arguments.of(0L, 15), Arguments.of(1L, 5), Arguments.of(6L, 6),
      Arguments.of(60L, 60), Arguments.of(61L, 60), Arguments.of(0x7fffffffL, 60),
      Arguments.of(0x80000000L, 60), Arguments.of(0xffffffffL, 60));
  }

  @ParameterizedTest
  @MethodSource("heartbeatIntervals")
  void heartbeatSchedulingKeepsTheUnsignedIntervalOnSubsequentTicks(long requested, int expectedSeconds) {
    var fixture = new Fixture();
    ServerCallStreamObserver<BotEvent> observer = mock();
    var received = new ArrayList<BotEvent>();
    var cancellation = new ArrayList<Runnable>();
    when(observer.isReady()).thenReturn(true);
    doAnswer(call -> { received.add(call.getArgument(0)); return null; }).when(observer).onNext(any());
    doAnswer(call -> { cancellation.add(call.getArgument(0)); return null; }).when(observer).setOnCancelHandler(any());
    try {
      fixture.context.run(() -> fixture.service.watchBotEvents(WatchBotEventsRequest.newBuilder()
        .setInstanceId(fixture.instanceId.toString()).setBotId(fixture.botId.toString())
        .setHeartbeatIntervalSeconds((int) requested).build(), observer));
      var heartbeat = ArgumentCaptor.forClass(Runnable.class);
      verify(fixture.scheduler).schedule(heartbeat.capture(), eq((long) expectedSeconds), eq(TimeUnit.SECONDS));
      heartbeat.getValue().run();
      assertTrue(received.getLast().hasHeartbeat());
      verify(fixture.scheduler, times(2)).schedule(any(Runnable.class), eq((long) expectedSeconds), eq(TimeUnit.SECONDS));
    } finally {
      cancellation.forEach(Runnable::run);
    }
  }

  private static void assertExpiry(BotControlLease lease, Instant before, Instant after, int expectedSeconds) {
    var expiry = Instant.ofEpochSecond(lease.getExpiresAt().getSeconds(), lease.getExpiresAt().getNanos());
    assertFalse(expiry.isBefore(before.plusSeconds(expectedSeconds)));
    assertFalse(expiry.isAfter(after.plusSeconds(expectedSeconds)));
  }

  private static final class Fixture {
    final UUID instanceId = UUID.randomUUID();
    final UUID botId = UUID.randomUUID();
    final UUID owner = UUID.randomUUID();
    final SoulFireServer server = mock();
    final SoulFireScheduler scheduler = mock();
    final BotControlLeaseManager leases = new BotControlLeaseManager();
    final BotLiveServiceImpl service = new BotLiveServiceImpl(server);
    final Context context;

    Fixture() {
      var instance = mock(InstanceManager.class);
      var settings = mock(InstanceSettingsDelegate.class);
      var states = mock(BotStateManager.class);
      when(server.getInstance(instanceId)).thenReturn(Optional.of(instance));
      when(server.scheduler()).thenReturn(scheduler);
      when(instance.settingsSource()).thenReturn(settings);
      when(instance.botStateManager()).thenReturn(states);
      when(instance.botConnections()).thenReturn(Map.of());
      when(instance.botControlLeaseManager()).thenReturn(leases);
      when(settings.accounts()).thenReturn(Map.of(botId, mock(MinecraftAccount.class)));
      when(states.status(botId)).thenReturn(BotStatus.newBuilder().setProfileId(botId.toString()).build());
      when(states.addStatusListener(any())).thenReturn(() -> {});
      var user = mock(SoulFireUser.class);
      var auth = mock(AuthSystem.class);
      var issuedAt = Instant.now();
      when(server.authSystem()).thenReturn(auth);
      when(user.getUniqueId()).thenReturn(owner);
      when(user.getIssuedAt()).thenReturn(issuedAt);
      when(user.hasPermission(any())).thenReturn(true);
      when(auth.authenticateBySubject(owner, issuedAt, 0L)).thenReturn(Optional.of(user));
      context = Context.current().withValue(ServerRPCConstants.USER_CONTEXT_KEY, user);
    }
  }
}
