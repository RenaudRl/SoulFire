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
package com.soulfiremc.server.pathfinding;

import com.soulfiremc.grpc.generated.PathfindOptions;
import com.soulfiremc.server.bot.BotConnection;
import com.soulfiremc.server.settings.instance.PathfindingSettings;
import com.soulfiremc.server.settings.lib.BotSettingsDelegate;
import com.soulfiremc.test.utils.TestBootstrap;
import net.minecraft.client.Minecraft;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class PathfindingSupportTest {
  @BeforeAll
  static void bootstrap() { TestBootstrap.bootstrapForTest(); }

  static Stream<Arguments> searchTimeouts() {
    return Stream.of(
      new TimeoutCase(0, 17),
      new TimeoutCase(1, 1),
      new TimeoutCase(3599, 3599),
      new TimeoutCase(3600, 3600),
      new TimeoutCase(3601, 3600),
      new TimeoutCase(0x7fffffffL, 3600),
      new TimeoutCase(0x80000000L, 3600),
      new TimeoutCase(0x80000001L, 3600),
      new TimeoutCase(0xffffffffL, 3600)
    ).flatMap(value -> Stream.of(false, true)
      .map(sprint -> Arguments.of(value.requested(), sprint, value.expected())));
  }

  @ParameterizedTest
  @MethodSource("searchTimeouts")
  void unsignedSearchTimeoutOverridesTheDefaultIndependentlyOfOtherOptions(
    long requested, boolean explicitSprint, int expectedSeconds
  ) {
    var bot = mock(BotConnection.class);
    var settings = mock(BotSettingsDelegate.class);
    when(bot.minecraft()).thenReturn(mock(Minecraft.class));
    when(bot.settingsSource()).thenReturn(settings);
    when(settings.get(PathfindingSettings.EXPIRE_TIMEOUT)).thenReturn(17);
    var options = PathfindOptions.newBuilder().setSearchTimeoutSeconds((int) requested);
    if (explicitSprint) options.setSprint(false);
    var constraint = PathfindingSupport.buildConstraint(bot, options.build());
    assertEquals(expectedSeconds, constraint.expireTimeout());
    assertEquals(!explicitSprint, constraint.sprint());
  }

  private record TimeoutCase(long requested, int expected) {}
}
