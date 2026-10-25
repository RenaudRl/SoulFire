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
package com.soulfiremc.server.task;

import com.soulfiremc.grpc.generated.BotTaskProgress;
import com.soulfiremc.grpc.generated.FishCompletionReason;
import com.soulfiremc.grpc.generated.FishTask;
import com.soulfiremc.grpc.generated.FishTaskResult;
import com.soulfiremc.mod.mixin.soulfire.FishingHookAccessor;
import com.soulfiremc.server.api.BotTaskExecution;
import com.soulfiremc.server.bot.BotConnection;
import com.soulfiremc.server.bot.ControlStopReason;
import com.soulfiremc.test.utils.TestBootstrap;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.MultiPlayerGameMode;
import net.minecraft.client.player.LocalPlayer;
import net.minecraft.world.InteractionHand;
import net.minecraft.world.InteractionResult;
import net.minecraft.world.entity.player.Inventory;
import net.minecraft.world.entity.projectile.FishingHook;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.item.Items;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class FishTaskProviderTest {
  @BeforeAll
  static void bootstrapMinecraft() {
    TestBootstrap.bootstrapForTest();
  }

  @ParameterizedTest
  @ValueSource(ints = {Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -1})
  void retainsUnsignedCatchLimitsInProgress(int maximumCatches) {
    var fixture = new Fixture(FishTask.newBuilder().setMaximumCatches(maximumCatches).build());
    fixture.execution.control().onStarted();
    fixture.tick(1);
    assertEquals(0, fixture.progress.getCurrent());
    assertEquals(Integer.toUnsignedLong(maximumCatches), fixture.progress.getTotal());
  }

  @ParameterizedTest
  @CsvSource({"0,100", "20,20", "1201,1200", "2147483647,1200", "-2147483648,1200", "-1,1200"})
  void waitsForCastConfirmationUntilTheUnsignedCappedTimeout(int timeout, int expectedTicks) {
    var fixture = new Fixture(FishTask.newBuilder().setCastTimeoutTicks(timeout)
      .setMaximumFailedCasts(1).build());
    fixture.player.fishing = null;
    fixture.execution.control().onStarted();
    fixture.tick(2);
    fixture.tick(expectedTicks - 1);
    assertFalse(fixture.execution.result().isDone());
    verify(fixture.gameMode, times(1)).useItem(fixture.player, InteractionHand.MAIN_HAND);
    fixture.tick(1);
    var result = (FishTaskResult) fixture.execution.result().join();
    assertEquals(FishCompletionReason.FISH_COMPLETION_REASON_FAILED_CAST_LIMIT_REACHED, result.getReason());
    assertEquals(1, result.getFailedCasts());
  }

  @ParameterizedTest
  @CsvSource({"0,100", "20,20", "-2147483648,1200", "-1,1200"})
  void waitsForRetrievalUntilTheUnsignedCappedTimeout(int timeout, int expectedTicks) {
    var fixture = new Fixture(FishTask.newBuilder().setCastTimeoutTicks(timeout)
      .setBiteTimeoutTicks(1).setMaximumFailedCasts(1).build());
    fixture.execution.control().onStarted();
    fixture.tick(3);
    fixture.tick(expectedTicks - 1);
    assertFalse(fixture.execution.result().isDone());
    fixture.tick(1);
    var failure = assertThrows(CompletionException.class, () -> fixture.execution.result().join());
    assertEquals(Status.Code.DEADLINE_EXCEEDED,
      ((StatusRuntimeException) failure.getCause()).getStatus().getCode());
  }

  @ParameterizedTest
  @CsvSource({"0,12000", "4,4", "72001,72000", "2147483647,72000", "-2147483648,72000", "-1,72000"})
  void waitsForBitesUntilTheUnsignedCappedTimeout(int timeout, int expectedTicks) {
    var fixture = new Fixture(FishTask.newBuilder().setBiteTimeoutTicks(timeout)
      .setMaximumFailedCasts(1).build());
    fixture.execution.control().onStarted();
    fixture.tick(2);
    fixture.tick(expectedTicks - 1);
    verify(fixture.gameMode, never()).useItem(fixture.player, InteractionHand.MAIN_HAND);
    assertFalse(fixture.execution.result().isDone());
    fixture.tick(1);
    verify(fixture.gameMode).useItem(fixture.player, InteractionHand.MAIN_HAND);
    fixture.player.fishing = null;
    fixture.tick(1);
    assertEquals(1, ((FishTaskResult) fixture.execution.result().join()).getFailedCasts());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stoppingWithoutControlPreservesTheCurrentOwnersLineAndSelection(boolean suspended) {
    var fixture = new Fixture();
    if (suspended) {
      fixture.execution.control().onStarted();
      fixture.execution.control().onSuspended();
    }
    fixture.execution.control().onStopped(ControlStopReason.CANCELLED, null);
    verify(fixture.gameMode, never()).useItem(fixture.player, InteractionHand.MAIN_HAND);
    verify(fixture.inventory, never()).setSelectedSlot(anyInt());
    assertTrue(fixture.execution.result().isCancelled());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void activeAndResumedControlsRetractTheLineAndRestoreSelection(boolean resumed) {
    var fixture = new Fixture();
    fixture.execution.control().onStarted();
    if (resumed) {
      fixture.execution.control().onSuspended();
      fixture.execution.control().onResumed();
    }
    fixture.execution.control().onStopped(ControlStopReason.CANCELLED, null);
    verify(fixture.gameMode).useItem(fixture.player, InteractionHand.MAIN_HAND);
    verify(fixture.inventory).setSelectedSlot(0);
    assertTrue(fixture.execution.result().isCancelled());
  }

  @Test
  void capturesSelectionWhenTheQueuedControlActuallyStarts() {
    var fixture = new Fixture();
    when(fixture.inventory.getSelectedSlot()).thenReturn(3);
    fixture.execution.control().onStarted();
    fixture.execution.control().onStopped(ControlStopReason.CANCELLED, null);
    verify(fixture.inventory).setSelectedSlot(3);
  }

  private static final class Fixture {
    final LocalPlayer player = mock(LocalPlayer.class);
    final Inventory inventory = mock(Inventory.class);
    final MultiPlayerGameMode gameMode = mock(MultiPlayerGameMode.class);
    final BotTaskExecution execution;
    BotTaskProgress progress = BotTaskProgress.getDefaultInstance();

    Fixture() {
      this(FishTask.newBuilder().setRestoreSelectedSlot(true).build());
    }

    Fixture(FishTask input) {
      var minecraft = mock(Minecraft.class);
      minecraft.player = player;
      minecraft.gameMode = gameMode;
      var hook = mock(FishingHook.class, withSettings().extraInterfaces(FishingHookAccessor.class));
      when(hook.isInWater()).thenReturn(true);
      player.fishing = hook;
      when(player.getInventory()).thenReturn(inventory);
      var rod = mock(ItemStack.class);
      when(rod.getItem()).thenReturn(Items.FISHING_ROD);
      when(player.getMainHandItem()).thenReturn(rod);
      when(gameMode.useItem(player, InteractionHand.MAIN_HAND)).thenReturn(InteractionResult.SUCCESS);
      var bot = mock(BotConnection.class);
      when(bot.minecraft()).thenReturn(minecraft);
      var context = new BotTaskContext(null, bot, null, UUID.randomUUID(),
        UUID.randomUUID(), UUID.randomUUID(), Optional.empty(), value -> progress = value);
      execution = new FishTaskProvider().start(context, input);
    }

    void tick(int count) {
      for (var tick = 0; tick < count; tick++) {
        execution.control().tick();
      }
    }
  }
}
