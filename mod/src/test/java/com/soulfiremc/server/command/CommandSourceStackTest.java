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
package com.soulfiremc.server.command;

import com.soulfiremc.server.SoulFireServer;
import com.soulfiremc.server.user.SoulFireUser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class CommandSourceStackTest {
  private static final UUID FIRST = UUID.randomUUID();
  private static final UUID SECOND = UUID.randomUUID();
  private static final UUID OUTSIDE = UUID.randomUUID();
  private static final Set<UUID> OTHER_SCOPE = Set.of(UUID.randomUUID());
  private final SoulFireServer server = mock();
  private final SoulFireUser user = mock();

  @ParameterizedTest
  @EnumSource(Dimension.class)
  void nestedSelectionsOnlyNarrowAndNeverRestoreAccess(Dimension dimension) {
    var original = source(dimension, Set.of(FIRST, SECOND));
    var narrowed = restrict(dimension, original, Set.of(SECOND, OUTSIDE));
    assertEquals(Set.of(SECOND), selected(dimension, narrowed));
    assertEquals(OTHER_SCOPE, selected(dimension == Dimension.INSTANCE ? Dimension.BOT : Dimension.INSTANCE, narrowed));
    assertSame(server, narrowed.soulFire());
    assertSame(user, narrowed.source());
    assertEquals(Set.of(FIRST, SECOND), selected(dimension, original));
    assertEquals(Set.of(SECOND), selected(dimension,
      restrict(dimension, narrowed, Set.of(FIRST, SECOND, OUTSIDE))));
    var empty = restrict(dimension, narrowed, Set.of(FIRST));
    assertEquals(Set.of(), selected(dimension, empty));
    assertEquals(Set.of(), selected(dimension,
      restrict(dimension, empty, Set.of(FIRST, SECOND, OUTSIDE))));
  }

  @ParameterizedTest
  @EnumSource(Dimension.class)
  void unrestrictedSelectionSnapshotsItsRequestedIds(Dimension dimension) {
    var original = source(dimension, null);
    var requested = new HashSet<>(Set.of(FIRST));
    var restricted = restrict(dimension, original, requested);
    requested.add(OUTSIDE);
    assertEquals(Set.of(FIRST), selected(dimension, restricted));
    assertNull(selected(dimension, original));
    assertThrows(UnsupportedOperationException.class, () -> selected(dimension, restricted).add(OUTSIDE));
  }

  private CommandSourceStack source(Dimension dimension, Set<UUID> ids) {
    return new CommandSourceStack(server, user,
      dimension == Dimension.INSTANCE ? ids : OTHER_SCOPE,
      dimension == Dimension.BOT ? ids : OTHER_SCOPE);
  }

  private static CommandSourceStack restrict(Dimension dimension, CommandSourceStack source, Set<UUID> ids) {
    return dimension == Dimension.INSTANCE ? source.withInstanceIds(ids) : source.withBotIds(ids);
  }

  private static Set<UUID> selected(Dimension dimension, CommandSourceStack source) {
    return dimension == Dimension.INSTANCE ? source.instanceIds() : source.botIds();
  }

  private enum Dimension { INSTANCE, BOT }
}
