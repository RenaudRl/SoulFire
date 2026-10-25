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
package com.soulfiremc.test;

import com.soulfiremc.server.SoulFireServer;
import com.soulfiremc.server.api.SoulFireAPI;
import com.soulfiremc.server.api.event.UserSessionsInvalidatedEvent;
import com.soulfiremc.server.database.generated.Tables;
import com.soulfiremc.server.util.PortHelper;
import com.soulfiremc.server.util.RPCConstants;
import com.soulfiremc.shared.SFLogAppender;
import com.soulfiremc.test.utils.TestBootstrap;
import io.jsonwebtoken.Jwts;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AuthSystemSessionInvalidationTest {
  @TempDir
  Path tempDir;

  @Test
  void publishesCommittedRevocationsAndDeletionWithoutRejectingNewTokens() {
    var previousBaseDir = System.getProperty("sf.baseDir");
    var previousUnitTest = System.getProperty("sf.unit.test");
    try {
      System.setProperty("sf.baseDir", tempDir.toAbsolutePath().toString());
      System.setProperty("sf.unit.test", "true");
      TestBootstrap.bootstrapForTest();
      SFLogAppender.INSTANCE.start();
      var server = new SoulFireServer("127.0.0.1", PortHelper.getRandomAvailablePort(), Instant.now());
      try {
        var userId = UUID.randomUUID();
        var issuedAt = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.SECONDS);
        server.dsl().insertInto(Tables.USERS)
          .set(Tables.USERS.ID, userId.toString())
          .set(Tables.USERS.USERNAME, "session-user")
          .set(Tables.USERS.EMAIL, "session-user@example.com")
          .set(Tables.USERS.ROLE, "USER")
          .set(Tables.USERS.MIN_ISSUED_AT, LocalDateTime.ofInstant(issuedAt.minusSeconds(5), ZoneOffset.UTC))
          .execute();
        var auth = server.authSystem();
        assertTrue(auth.authenticateBySubject(userId, issuedAt, 0L).isPresent());
        var originalToken = auth.generateJWT(auth.getUserData(userId).orElseThrow(), RPCConstants.API_AUDIENCE);
        var legacyToken = Jwts.builder().signWith(server.jwtSecretKey())
          .subject(userId.toString()).issuedAt(Date.from(Instant.now()))
          .audience().add(RPCConstants.API_AUDIENCE).and().compact();
        assertTrue(auth.authenticateByToken(legacyToken, RPCConstants.API_AUDIENCE).isPresent());
        assertTrue(auth.authenticateByToken(originalToken, RPCConstants.API_AUDIENCE).isPresent());
        var events = new ArrayList<UserSessionsInvalidatedEvent>();
        var authenticatedDuringEvent = new ArrayList<Boolean>();
        Consumer<UserSessionsInvalidatedEvent> listener = event -> {
          if (event.soulFireServer() != server || !event.userId().equals(userId)) return;
          events.add(event);
          authenticatedDuringEvent.add(auth.authenticateBySubject(userId, issuedAt, 0L).isPresent());
        };
        SoulFireAPI.registerListener(UserSessionsInvalidatedEvent.class, listener);
        try {
          auth.invalidateSessions(userId);
          assertFalse(auth.authenticateBySubject(userId, issuedAt, 0L).isPresent());
          assertEquals(1, events.size());
          var revoked = events.getFirst();
          assertSame(server, revoked.soulFireServer());
          assertEquals(userId, revoked.userId());
          var cutoff = auth.getUserData(userId).orElseThrow().getMinIssuedAt()
            .toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
          assertEquals(1L, revoked.minimumSessionVersion());
          assertEquals(1L, auth.getUserData(userId).orElseThrow().getSessionVersion());
          // Match JWT's issue second to the cutoff exactly, without depending on test timing.
          assertFalse(auth.authenticateBySubject(userId, cutoff, 0L).isPresent());
          assertTrue(auth.authenticateBySubject(userId, cutoff, 1L).isPresent());
          assertFalse(auth.authenticateByToken(originalToken, RPCConstants.API_AUDIENCE).isPresent());
          assertFalse(auth.authenticateByToken(legacyToken, RPCConstants.API_AUDIENCE).isPresent());
          var freshToken = auth.generateJWT(auth.getUserData(userId).orElseThrow(), RPCConstants.API_AUDIENCE);
          var freshUser = auth.authenticateByToken(freshToken, RPCConstants.API_AUDIENCE).orElseThrow();
          assertEquals(1L, freshUser.getSessionVersion());
          assertFalse(auth.authenticateByToken(freshToken, RPCConstants.WEBDAV_AUDIENCE).isPresent());

          auth.invalidateSessions(userId);
          assertEquals(2L, events.getLast().minimumSessionVersion());
          assertFalse(auth.authenticateByToken(freshToken, RPCConstants.API_AUDIENCE).isPresent());
          var newestToken = auth.generateJWT(auth.getUserData(userId).orElseThrow(), RPCConstants.API_AUDIENCE);
          assertEquals(2L, auth.authenticateByToken(newestToken, RPCConstants.API_AUDIENCE)
            .orElseThrow().getSessionVersion());

          auth.deleteUser(userId);
          assertEquals(3, events.size());
          assertEquals(Long.MAX_VALUE, events.getLast().minimumSessionVersion());
          assertFalse(auth.authenticateByToken(newestToken, RPCConstants.API_AUDIENCE).isPresent());
          assertEquals(List.of(false, false, false), authenticatedDuringEvent);
          assertThrows(IllegalArgumentException.class, () -> auth.invalidateSessions(userId));
          assertEquals(3, events.size());
        } finally {
          SoulFireAPI.unregisterListener(UserSessionsInvalidatedEvent.class, listener);
        }
      } finally {
        server.shutdownManager().shutdownSoftware(false);
        server.shutdownManager().awaitShutdown();
      }
    } finally {
      restoreProperty("sf.baseDir", previousBaseDir);
      restoreProperty("sf.unit.test", previousUnitTest);
    }
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) System.clearProperty(key);
    else System.setProperty(key, value);
  }
}
