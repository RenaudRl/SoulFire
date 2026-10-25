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

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.soulfiremc.grpc.generated.BotDeleteConfigEntryRequest;
import com.soulfiremc.grpc.generated.BotDeleteConfigEntryResponse;
import com.soulfiremc.grpc.generated.InstancePermission;
import com.soulfiremc.server.InstanceManager;
import com.soulfiremc.server.SoulFireServer;
import com.soulfiremc.server.account.AuthType;
import com.soulfiremc.server.account.MinecraftAccount;
import com.soulfiremc.server.account.service.OfflineJavaData;
import com.soulfiremc.server.database.generated.Tables;
import com.soulfiremc.server.settings.lib.InstanceSettingsImpl;
import com.soulfiremc.server.user.PermissionContext;
import com.soulfiremc.server.user.SoulFireUser;
import com.soulfiremc.server.util.structs.GsonInstance;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.flywaydb.core.Flyway;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteDataSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class BotConfigServiceTest {
  private final SoulFireServer server = mock();
  private final InstanceManager instance = mock();
  private final SoulFireUser user = mock();
  private final BotServiceImpl service = new BotServiceImpl(server);
  private final Context context = Context.current().withValue(ServerRPCConstants.USER_CONTEXT_KEY, user);
  private final UUID instanceId = UUID.randomUUID();
  private final MinecraftAccount account = new MinecraftAccount(AuthType.OFFLINE, UUID.randomUUID(), "E2EBot",
    new OfflineJavaData(), Map.of("plugin", Map.of("enabled", new JsonPrimitive(true), "value", new JsonPrimitive(42)),
      "other", Map.of("value", new JsonPrimitive(7))), Map.of("metadata", Map.of("value", new JsonPrimitive(9))));
  private final MinecraftAccount other = new MinecraftAccount(AuthType.OFFLINE, UUID.randomUUID(), "FleetE2EA",
    new OfflineJavaData(), Map.of(), Map.of());
  private final InstanceSettingsImpl.Stem original = new InstanceSettingsImpl.Stem(
    Map.of("instance", Map.of("value", new JsonPrimitive(3))), List.of(account, other), List.of(), Map.of());

  @BeforeEach
  void setUp(@TempDir Path directory) {
    var dataSource = new SQLiteDataSource();
    dataSource.setUrl("jdbc:sqlite:" + directory.resolve("config.sqlite"));
    Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
    when(server.dsl()).thenReturn(DSL.using(dataSource, SQLDialect.SQLITE));
    when(server.getInstance(instanceId)).thenReturn(Optional.of(instance));
    var owner = UUID.randomUUID().toString();
    server.dsl().execute("INSERT INTO users (id, username, email, role, min_issued_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
      owner, "owner", "owner@example.com", "ADMIN");
    server.dsl().insertInto(Tables.INSTANCES)
      .set(Tables.INSTANCES.ID, instanceId.toString())
      .set(Tables.INSTANCES.FRIENDLY_NAME, "config")
      .set(Tables.INSTANCES.OWNER_ID, owner)
      .set(Tables.INSTANCES.ICON, "pickaxe")
      .set(Tables.INSTANCES.SETTINGS, GsonInstance.CONFIG_GSON.toJson(original.serializeToTree()))
      .execute();
  }

  @Test
  void deletePreservesOtherEntriesAccountsAndMetadataAndIsIdempotent() {
    delete(request(instanceId, account.profileId(), "plugin", "value"));
    delete(request(instanceId, account.profileId(), "plugin", "value"));
    var expected = account.withSettings(Map.of("plugin", Map.of("enabled", new JsonPrimitive(true)),
      "other", Map.of("value", new JsonPrimitive(7))));
    assertEquals(original.withAccounts(List.of(expected, other)), stored());
    verify(user, times(2)).hasPermissionOrThrow(PermissionContext.instance(InstancePermission.UPDATE_BOT_CONFIG, instanceId));
    verify(instance, times(2)).invalidateSettingsCache();
  }

  @Test
  void deletingLastEntryRemovesItsNamespace() {
    delete(request(instanceId, account.profileId(), "other", "value"));
    assertEquals(original.withAccounts(List.of(account.withSettings(Map.of("plugin", account.settings().get("plugin"))), other)), stored());
  }

  @Test
  void missingInstanceOrBotPreservesNotFoundAndStoredState() {
    for (var request : List.of(request(UUID.randomUUID(), account.profileId(), "plugin", "value"),
      request(instanceId, UUID.randomUUID(), "plugin", "value"))) {
      var error = assertThrows(StatusRuntimeException.class, () -> delete(request));
      assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
      assertEquals(original, stored());
    }
  }

  @Test
  void deniedConfigPermissionDoesNotChangeStoredState() {
    doThrow(Status.PERMISSION_DENIED.asRuntimeException()).when(user)
      .hasPermissionOrThrow(PermissionContext.instance(InstancePermission.UPDATE_BOT_CONFIG, instanceId));
    var error = assertThrows(StatusRuntimeException.class,
      () -> delete(request(instanceId, account.profileId(), "plugin", "value")));
    assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    assertEquals(original, stored());
  }

  private void delete(BotDeleteConfigEntryRequest request) {
    StreamObserver<BotDeleteConfigEntryResponse> observer = mock();
    context.run(() -> service.deleteBotConfigEntry(request, observer));
    verify(observer).onNext(BotDeleteConfigEntryResponse.getDefaultInstance());
    verify(observer).onCompleted();
  }

  private static BotDeleteConfigEntryRequest request(UUID instance, UUID bot, String namespace, String key) {
    return BotDeleteConfigEntryRequest.newBuilder().setInstanceId(instance.toString()).setBotId(bot.toString())
      .setNamespace(namespace).setKey(key).build();
  }

  private InstanceSettingsImpl.Stem stored() {
    return InstanceSettingsImpl.Stem.deserialize(GsonInstance.GSON.fromJson(server.dsl().select(Tables.INSTANCES.SETTINGS)
      .from(Tables.INSTANCES).where(Tables.INSTANCES.ID.eq(instanceId.toString())).fetchSingle(Tables.INSTANCES.SETTINGS), JsonElement.class));
  }
}
