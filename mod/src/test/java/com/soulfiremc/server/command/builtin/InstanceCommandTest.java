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
package com.soulfiremc.server.command.builtin;

import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.StringReader;
import com.soulfiremc.server.InstanceManager;
import com.soulfiremc.server.command.CommandSourceStack;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InstanceCommandTest {
  @ParameterizedTest
  @ValueSource(strings = {"QA Alpha", "QA \"Alpha\"", "QA\\Alpha", "ordinary-name", "QA:Alpha"})
  void suggestedNamesRoundTripThroughTheArgumentParser(String name) throws Exception {
    var source = mock(CommandSourceStack.class);
    var instance = mock(InstanceManager.class);
    when(instance.friendlyName()).thenReturn(name);
    when(source.getVisibleInstances()).thenReturn(List.of(instance));
    var dispatcher = new CommandDispatcher<CommandSourceStack>();
    InstanceCommand.register(dispatcher);
    for (var command : List.of("instance ", "instance \"QA A")) {
      var suggestions = dispatcher.getCompletionSuggestions(dispatcher.parse(command, source)).join().getList();
      assertEquals(1, suggestions.size());
      var reader = new StringReader(suggestions.getFirst().getText());
      assertEquals(name, reader.readString());
      assertFalse(reader.canRead());
    }
  }
}
