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
import com.soulfiremc.grpc.generated.BotDesiredState;
import com.soulfiremc.server.BotStateManager;
import com.soulfiremc.server.InstanceManager;
import com.soulfiremc.server.SoulFireScheduler;
import com.soulfiremc.server.SoulFireServer;
import com.soulfiremc.server.account.MinecraftAccount;
import com.soulfiremc.server.command.CommandSourceStack;
import com.soulfiremc.server.settings.lib.InstanceSettingsDelegate;
import com.soulfiremc.server.user.SoulFireUser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class BotsCommandTest {
  private static final UUID FIRST = UUID.randomUUID();
  private static final UUID SECOND = UUID.randomUUID();
  private static final UUID OUTSIDE = UUID.randomUUID();
  private static final UUID UNKNOWN = UUID.randomUUID();

  @ParameterizedTest
  @MethodSource("directScopes")
  void changesOnlyConfiguredProfilesInTheCommandScope(BotDesiredState desired, Filter filter) throws Exception {
    var fixture = new Fixture();
    Set<UUID> botIds = switch (filter) {
      case ALL -> null;
      case FIRST -> Set.of(FIRST);
      case NONE -> Set.of();
      case UNKNOWN -> Set.of(UNKNOWN);
    };
    var expected = filter == Filter.ALL ? Set.of(FIRST, SECOND)
      : filter == Filter.FIRST ? Set.of(FIRST) : Set.<UUID>of();
    var result = fixture.dispatcher.execute(command(desired), fixture.source(botIds));
    assertEquals(expected.isEmpty() ? 0 : 1, result);
    fixture.verifyTargets(fixture.controller, expected, desired);
    verifyNoInteractions(fixture.otherController);
  }

  static Stream<Arguments> directScopes() {
    return states().flatMap(state -> Stream.of(Filter.values()).map(filter -> Arguments.of(state, filter)));
  }

  @ParameterizedTest
  @MethodSource("namedScopes")
  void resolvesOfflineNamesWithoutWideningTheSelection(BotDesiredState desired, Names names) throws Exception {
    var fixture = new Fixture();
    var selector = switch (names) {
      case FIRST -> "E2EBot";
      case BOTH -> "E2EBot,FleetE2EA";
      case DUPLICATE -> "E2EBot,E2EBot";
      case MISSING -> "MissingQABot";
    };
    var expected = switch (names) {
      case FIRST, DUPLICATE -> Set.of(FIRST);
      case BOTH -> Set.of(FIRST, SECOND);
      case MISSING -> Set.<UUID>of();
    };
    var result = fixture.dispatcher.execute("bot \"" + selector + "\" " + command(desired), fixture.source(null));
    assertEquals(expected.isEmpty() ? 0 : 1, result);
    fixture.verifyTargets(fixture.controller, expected, desired);
    verifyNoInteractions(fixture.otherController);
  }

  static Stream<Arguments> namedScopes() {
    return states().flatMap(state -> Stream.of(Names.values()).map(names -> Arguments.of(state, names)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void globalSelectionStillIntersectsEachInstancesConfiguredProfiles(boolean restricted) throws Exception {
    var fixture = new Fixture();
    var source = new CommandSourceStack(fixture.server, fixture.user, null, restricted ? Set.of(FIRST) : null);
    assertEquals(restricted ? 1 : 2, fixture.dispatcher.execute("bots start", source));
    fixture.verifyTargets(fixture.controller, restricted ? Set.of(FIRST) : Set.of(FIRST, SECOND),
      BotDesiredState.BOT_DESIRED_STATE_RUNNING);
    fixture.verifyTargets(fixture.otherController, restricted ? Set.of() : Set.of(OUTSIDE),
      BotDesiredState.BOT_DESIRED_STATE_RUNNING);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void suggestsConfiguredOfflineProfilesWithinTheScope(boolean restricted) {
    var fixture = new Fixture();
    var suggestions = fixture.dispatcher.getCompletionSuggestions(
      fixture.dispatcher.parse("bot ", fixture.source(restricted ? Set.of(FIRST) : null))).join();
    assertEquals(restricted ? Set.of("E2EBot") : Set.of("E2EBot", "FleetE2EA"),
      suggestions.getList().stream().map(suggestion -> suggestion.getText()).collect(Collectors.toSet()));
  }

  @ParameterizedTest
  @MethodSource("nestedScopes")
  void nestedSelectorsPreserveTheOriginalLimits(BotDesiredState desired, String prefix, Set<UUID> expected) throws Exception {
    var fixture = new Fixture();
    var result = fixture.dispatcher.execute(prefix + " " + command(desired), fixture.source(null));
    assertEquals(expected.isEmpty() ? 0 : 1, result);
    fixture.verifyTargets(fixture.controller, expected, desired);
    verifyNoInteractions(fixture.otherController);
  }

  static Stream<Arguments> nestedScopes() {
    return states().flatMap(state -> Stream.of(
      Arguments.of(state, "bot \"E2EBot,FleetE2EA\" bot E2EBot", Set.of(FIRST)),
      Arguments.of(state, "bot E2EBot bot \"E2EBot,FleetE2EA\"", Set.of(FIRST)),
      Arguments.of(state, "bot E2EBot bot MissingQABot", Set.of()),
      Arguments.of(state, "instance \"Command QA,Command QA\"", Set.of(FIRST, SECOND)),
      Arguments.of(state, "instance \"Missing QA\"", Set.of()),
      Arguments.of(state, "instance \"Missing QA\" instance \"Command QA\"", Set.of())
    ));
  }

  private static Stream<BotDesiredState> states() {
    return Stream.of(BotDesiredState.BOT_DESIRED_STATE_RUNNING, BotDesiredState.BOT_DESIRED_STATE_STOPPED);
  }

  private static String command(BotDesiredState desired) {
    return desired == BotDesiredState.BOT_DESIRED_STATE_RUNNING ? "bots start" : "bots stop";
  }

  private enum Filter { ALL, FIRST, NONE, UNKNOWN }
  private enum Names { FIRST, BOTH, DUPLICATE, MISSING }

  private static final class Fixture {
    final SoulFireServer server = mock();
    final SoulFireUser user = mock();
    final BotStateManager controller = mock();
    final BotStateManager otherController = mock();
    final UUID instanceId = UUID.randomUUID();
    final CommandDispatcher<CommandSourceStack> dispatcher = new CommandDispatcher<>();

    Fixture() {
      var instance = instance(instanceId, controller, Map.of(FIRST, account(FIRST, "E2EBot"), SECOND, account(SECOND, "FleetE2EA")));
      var otherId = UUID.randomUUID();
      var other = instance(otherId, otherController, Map.of(OUTSIDE, account(OUTSIDE, "FleetE2EB")));
      when(server.instances()).thenReturn(Map.of(instanceId, instance, otherId, other));
      BotsCommand.register(dispatcher);
      BotCommand.register(dispatcher);
      InstanceCommand.register(dispatcher);
    }

    CommandSourceStack source(Set<UUID> botIds) {
      return new CommandSourceStack(server, user, Set.of(instanceId), botIds);
    }

    private InstanceManager instance(UUID id, BotStateManager controller, Map<UUID, MinecraftAccount> accounts) {
      var instance = mock(InstanceManager.class);
      var settings = mock(InstanceSettingsDelegate.class);
      when(instance.id()).thenReturn(id);
      when(instance.friendlyName()).thenReturn("Command QA");
      when(instance.runnableWrapper()).thenReturn((SoulFireScheduler.RunnableWrapper) runnable -> runnable);
      when(instance.settingsSource()).thenReturn(settings);
      when(instance.botStateManager()).thenReturn(controller);
      when(settings.accounts()).thenReturn(accounts);
      return instance;
    }

    private MinecraftAccount account(UUID id, String name) {
      var account = mock(MinecraftAccount.class);
      when(account.profileId()).thenReturn(id);
      when(account.lastKnownName()).thenReturn(name);
      return account;
    }

    void verifyTargets(BotStateManager manager, Set<UUID> expected, BotDesiredState desired) {
      if (expected.isEmpty()) {
        verifyNoInteractions(manager);
      } else {
        verify(manager).setDesiredState(eq(user), argThat(ids -> Set.copyOf(ids).equals(expected)), eq(desired));
        verifyNoMoreInteractions(manager);
      }
    }
  }
}
