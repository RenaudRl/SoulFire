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

import com.mojang.authlib.GameProfile;
import com.soulfiremc.grpc.generated.BotEvent;
import com.soulfiremc.grpc.generated.BotEventFilter;
import com.soulfiremc.grpc.generated.BotStatus;
import com.soulfiremc.grpc.generated.WatchBotEventsRequest;
import com.soulfiremc.server.BotStateManager;
import com.soulfiremc.server.InstanceManager;
import com.soulfiremc.server.SoulFireScheduler;
import com.soulfiremc.server.SoulFireServer;
import com.soulfiremc.server.account.MinecraftAccount;
import com.soulfiremc.server.api.SoulFireAPI;
import com.soulfiremc.server.api.event.bot.BotConnectionInitEvent;
import com.soulfiremc.server.api.event.bot.BotPostTickEvent;
import com.soulfiremc.server.bot.BotConnection;
import com.soulfiremc.server.settings.lib.InstanceSettingsDelegate;
import com.soulfiremc.server.user.AuthSystem;
import com.soulfiremc.server.user.SoulFireUser;
import com.soulfiremc.test.utils.TestBootstrap;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.client.multiplayer.ClientPacketListener;
import net.minecraft.client.multiplayer.PlayerInfo;
import net.minecraft.world.level.GameType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class BotEventRecoveryTest {
  @BeforeAll
  static void bootstrap() { TestBootstrap.bootstrapForTest(); }

  @Test
  void recoveryResendsDroppedStatusPlayerListAndEnvironment() {
    try (var fixture = new Fixture()) {
      fixture.watch(false, true);
      assertTrue(fixture.received.isEmpty());
      fixture.resume();
      assertTrue(fixture.received.getFirst().hasResyncRequired());
      assertTrue(fixture.received.stream().anyMatch(BotEvent::hasStatus));
      fixture.tick();
      fixture.assertAuxiliarySnapshots();
      var count = fixture.received.size();
      fixture.tick();
      assertEquals(count, fixture.received.size());
    }
  }

  @Test
  void repeatedBackpressureRetriesTheWholeAuxiliarySnapshot() {
    try (var fixture = new Fixture()) {
      fixture.watch(false, true);
      fixture.resume();
      fixture.pauseDuringEnvironment = true;
      fixture.tick();
      assertFalse(fixture.ready.get());
      assertFalse(fixture.received.stream().anyMatch(BotEvent::hasPlayerList));
      fixture.received.clear();
      fixture.resume();
      fixture.tick();
      fixture.assertAuxiliarySnapshots();
    }
  }

  @Test
  void reconnectPublishesAuxiliarySnapshotsForTheNewEpoch() {
    try (var fixture = new Fixture()) {
      fixture.watch(true, true);
      var epoch = fixture.received.getLast().getEnvelope().getStreamEpoch();
      fixture.received.clear();
      SoulFireAPI.postEvent(new BotConnectionInitEvent(fixture.bot));
      fixture.tick();
      fixture.assertAuxiliarySnapshots();
      assertTrue(fixture.received.stream().allMatch(event ->
        !event.getEnvelope().getStreamEpoch().equals(epoch)));
    }
  }

  @Test
  void recoveryRespectsFilters() {
    try (var fixture = new Fixture()) {
      fixture.watch(false, false);
      fixture.resume();
      fixture.tick();
      assertFalse(fixture.received.stream().anyMatch(event -> event.hasEnvironment() || event.hasPlayerList()));
    }
  }

  @Test
  void cancellationDiscardsPendingRecovery() {
    try (var fixture = new Fixture()) {
      fixture.watch(false, true);
      fixture.resume();
      var count = fixture.received.size();
      fixture.close();
      fixture.tick();
      assertEquals(count, fixture.received.size());
    }
  }

  @Test
  void recoveryWaitsUntilTheClientWorldIsAvailable() {
    try (var fixture = new Fixture()) {
      fixture.watch(false, true);
      fixture.resume();
      var level = fixture.minecraft.level;
      fixture.minecraft.level = null;
      fixture.tick();
      assertFalse(fixture.received.stream().anyMatch(BotEvent::hasPlayerList));
      fixture.minecraft.level = level;
      fixture.tick();
      fixture.assertAuxiliarySnapshots();
    }
  }

  private static final class Fixture implements AutoCloseable {
    final UUID instanceId = UUID.randomUUID();
    final UUID botId = UUID.randomUUID();
    final SoulFireServer server = mock();
    final InstanceManager instance = mock();
    final BotConnection bot = mock();
    final Minecraft minecraft = mock();
    final ServerCallStreamObserver<BotEvent> observer = mock();
    final List<BotEvent> received = new ArrayList<>();
    final AtomicBoolean ready = new AtomicBoolean();
    final Context context;
    Runnable onReady;
    Runnable cancel;
    boolean pauseDuringEnvironment;

    Fixture() {
      var states = mock(BotStateManager.class);
      var settings = mock(InstanceSettingsDelegate.class);
      var listener = mock(ClientPacketListener.class);
      var info = mock(PlayerInfo.class);
      minecraft.level = mock(ClientLevel.class);
      when(minecraft.level.getGameTime()).thenReturn(42L);
      when(minecraft.getConnection()).thenReturn(listener);
      when(listener.getOnlinePlayers()).thenReturn(List.of(info));
      when(listener.getListedOnlinePlayers()).thenReturn(List.of(info));
      when(info.getProfile()).thenReturn(new GameProfile(botId, "SnapshotPlayer"));
      when(info.getGameMode()).thenReturn(GameType.SURVIVAL);
      when(bot.minecraft()).thenReturn(minecraft);
      when(bot.runnableWrapper()).thenReturn((SoulFireScheduler.RunnableWrapper) runnable -> runnable);
      when(bot.instanceManager()).thenReturn(instance);
      when(bot.accountProfileId()).thenReturn(botId);
      when(server.getInstance(instanceId)).thenReturn(Optional.of(instance));
      when(server.scheduler()).thenReturn(mock(SoulFireScheduler.class));
      when(instance.botConnections()).thenReturn(Map.of(botId, bot));
      when(instance.settingsSource()).thenReturn(settings);
      when(instance.botStateManager()).thenReturn(states);
      when(settings.accounts()).thenReturn(Map.of(botId, mock(MinecraftAccount.class)));
      when(states.status(botId)).thenReturn(BotStatus.newBuilder().setProfileId(botId.toString()).build());
      when(states.addStatusListener(any())).thenReturn(() -> {});
      var user = mock(SoulFireUser.class);
      var auth = mock(AuthSystem.class);
      var owner = UUID.randomUUID();
      var issuedAt = Instant.now();
      when(server.authSystem()).thenReturn(auth);
      when(user.getUniqueId()).thenReturn(owner);
      when(user.getIssuedAt()).thenReturn(issuedAt);
      when(user.hasPermission(any())).thenReturn(true);
      when(auth.authenticateBySubject(owner, issuedAt, 0L)).thenReturn(Optional.of(user));
      context = Context.current().withValue(ServerRPCConstants.USER_CONTEXT_KEY, user);
      when(observer.isReady()).thenAnswer(_ -> ready.get());
      doAnswer(call -> { onReady = call.getArgument(0); return null; }).when(observer).setOnReadyHandler(any());
      doAnswer(call -> { cancel = call.getArgument(0); return null; }).when(observer).setOnCancelHandler(any());
      doAnswer(call -> {
        BotEvent event = call.getArgument(0);
        received.add(event);
        if (pauseDuringEnvironment && event.hasEnvironment()) {
          pauseDuringEnvironment = false;
          ready.set(false);
        }
        return null;
      }).when(observer).onNext(any());
    }

    void watch(boolean initiallyReady, boolean includeAuxiliary) {
      ready.set(initiallyReady);
      context.run(() -> new BotLiveServiceImpl(server).watchBotEvents(WatchBotEventsRequest.newBuilder()
        .setInstanceId(instanceId.toString()).setBotId(botId.toString())
        .setFilter(BotEventFilter.newBuilder().setIncludeEnvironment(includeAuxiliary).setIncludePlayerList(includeAuxiliary))
        .build(), observer));
    }

    void resume() { ready.set(true); onReady.run(); }

    void tick() { SoulFireAPI.postEvent(new BotPostTickEvent(bot)); }

    void assertAuxiliarySnapshots() {
      var playerLists = received.stream().filter(BotEvent::hasPlayerList).toList();
      assertEquals(1, playerLists.size());
      assertEquals(botId.toString(), playerLists.getFirst().getPlayerList().getEntries(0).getProfileId());
      assertTrue(playerLists.getFirst().getPlayerList().getEntries(0).getListed());
      assertEquals(4, received.stream().filter(BotEvent::hasEnvironment).count());
      assertTrue(received.stream().anyMatch(event -> event.hasEnvironment()
        && event.getEnvironment().hasTime() && event.getEnvironment().getTime().getGameTime() == 42));
    }

    @Override
    public void close() { if (cancel != null) cancel.run(); }
  }
}
