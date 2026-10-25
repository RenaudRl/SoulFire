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

import com.soulfiremc.grpc.generated.BlockPosition;
import com.soulfiremc.grpc.generated.ExcavateTask;
import com.soulfiremc.server.bot.BotConnection;
import com.soulfiremc.test.utils.TestBootstrap;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientChunkCache;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.client.player.LocalPlayer;
import net.minecraft.core.BlockPos;
import net.minecraft.world.level.Level;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ExcavateInputTest {
  @BeforeAll
  static void bootstrapMinecraft() {
    TestBootstrap.bootstrapForTest();
  }

  @ParameterizedTest
  @MethodSource("oversizedCorners")
  void rejectsOversizedVolumesBeforeReadingChunks(BlockPos from, BlockPos to) {
    var minecraft = mock(Minecraft.class);
    var level = mock(ClientLevel.class);
    var chunks = mock(ClientChunkCache.class);
    var player = mock(LocalPlayer.class);
    when(level.dimension()).thenReturn(Level.OVERWORLD);
    when(level.getMinY()).thenReturn(-64);
    when(level.getMaxY()).thenReturn(320);
    when(level.getChunkSource()).thenReturn(chunks);
    when(player.blockPosition()).thenReturn(BlockPos.ZERO);
    minecraft.level = level;
    minecraft.player = player;
    var bot = mock(BotConnection.class);
    when(bot.minecraft()).thenReturn(minecraft);
    var context = new BotTaskContext(null, bot, null, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), Optional.empty(), _ -> {});
    var input = ExcavateTask.newBuilder().setCornerA(position(from)).setCornerB(position(to)).build();
    var error = assertThrows(StatusRuntimeException.class, () -> new ExcavateTaskProvider().start(context, input));
    assertEquals(Status.Code.RESOURCE_EXHAUSTED, error.getStatus().getCode());
    verifyNoInteractions(chunks);
  }

  static Stream<Arguments> oversizedCorners() {
    var minimum = Integer.MIN_VALUE;
    var maximum = Integer.MAX_VALUE;
    return Stream.of(
      Arguments.of(new BlockPos(0, 80, 0), new BlockPos(32768, 80, 0)),
      Arguments.of(new BlockPos(0, 80, 0), new BlockPos(31, 111, 32)),
      Arguments.of(new BlockPos(minimum, 80, minimum), new BlockPos(maximum, 80, maximum)),
      Arguments.of(new BlockPos(maximum, 80, maximum), new BlockPos(minimum, 80, minimum)),
      Arguments.of(new BlockPos(minimum, 80, minimum), new BlockPos(-1, 80, maximum)),
      Arguments.of(new BlockPos(minimum, minimum, minimum), new BlockPos(maximum, maximum, maximum))
    );
  }

  private static BlockPosition position(BlockPos position) {
    return BlockPosition.newBuilder().setX(position.getX()).setY(position.getY()).setZ(position.getZ()).build();
  }
}
