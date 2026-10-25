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

import com.soulfiremc.grpc.generated.InstancePermission;
import com.soulfiremc.server.SoulFireServer;
import com.soulfiremc.server.api.SoulFireAPI;
import com.soulfiremc.server.api.event.UserPermissionsChangedEvent;
import com.soulfiremc.server.api.event.UserSessionsInvalidatedEvent;
import com.soulfiremc.server.user.AuthSystem;
import com.soulfiremc.server.user.PermissionContext;
import com.soulfiremc.server.user.SoulFireUser;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class AuthorizedStreamTest {
  @Test
  void invalidationUsesServerUserAndGenerationAndOnlyTerminatesOnce() {
    try (var fixture = new Fixture()) {
      assertTrue(fixture.stream.start());
      SoulFireAPI.postEvent(new UserSessionsInvalidatedEvent(mock(SoulFireServer.class), fixture.owner, 4L));
      SoulFireAPI.postEvent(new UserSessionsInvalidatedEvent(fixture.server, UUID.randomUUID(), 4L));
      SoulFireAPI.postEvent(new UserSessionsInvalidatedEvent(fixture.server, fixture.owner, 2L));
      SoulFireAPI.postEvent(new UserSessionsInvalidatedEvent(fixture.server, fixture.owner, 3L));
      assertFalse(fixture.closed.get());
      SoulFireAPI.postEvent(new UserSessionsInvalidatedEvent(fixture.server, fixture.owner, 4L));
      fixture.stream.complete();
      fixture.stream.fail(Status.INTERNAL);
      assertEquals(Status.Code.UNAUTHENTICATED, fixture.terminal.getFirst());
      assertEquals(1, fixture.terminal.size());
      assertEquals(1, fixture.cleaned.get());
      assertEquals(1, fixture.lookups.get());
    }
  }

  @Test
  void startupRevalidationRejectsARevocationBeforeListenerRegistration() {
    try (var fixture = new Fixture()) {
      fixture.authenticated = Optional.empty();
      assertFalse(fixture.stream.start());
      assertEquals(Status.Code.UNAUTHENTICATED, fixture.terminal.getFirst());
      assertEquals(1, fixture.cleaned.get());
    }
  }

  @Test
  void permissionUpdatesKeepAuthorizedOwnersAndRejectRemovedAccess() {
    try (var fixture = new Fixture()) {
      assertTrue(fixture.stream.start());
      SoulFireAPI.postEvent(new UserPermissionsChangedEvent(mock(SoulFireServer.class), fixture.owner));
      SoulFireAPI.postEvent(new UserPermissionsChangedEvent(fixture.server, UUID.randomUUID()));
      assertEquals(1, fixture.lookups.get());
      var refreshed = mock(SoulFireUser.class);
      when(refreshed.hasPermission(any())).thenReturn(true);
      fixture.authenticated = Optional.of(refreshed);
      SoulFireAPI.postEvent(new UserPermissionsChangedEvent(fixture.server, fixture.owner));
      assertFalse(fixture.closed.get());
      when(refreshed.hasPermission(any())).thenReturn(false);
      SoulFireAPI.postEvent(new UserPermissionsChangedEvent(fixture.server, fixture.owner));
      assertEquals(Status.Code.PERMISSION_DENIED, fixture.terminal.getFirst());
      assertEquals(1, fixture.cleaned.get());
    }
  }

  @Test
  void authorizationLookupFailureClosesTheSubscription() {
    try (var fixture = new Fixture()) {
      assertTrue(fixture.stream.start());
      when(fixture.auth.authenticateBySubject(fixture.owner, fixture.issuedAt, 3L)).thenThrow(new IllegalStateException());
      SoulFireAPI.postEvent(new UserPermissionsChangedEvent(fixture.server, fixture.owner));
      assertEquals(Status.Code.INTERNAL, fixture.terminal.getFirst());
      assertEquals(1, fixture.cleaned.get());
    }
  }

  @Test
  void resourcesInstalledAfterCancellationAreClosedImmediately() {
    try (var fixture = new Fixture()) {
      fixture.stream.close();
      assertFalse(fixture.stream.start());
      var later = new AtomicInteger();
      fixture.stream.addCleanup(() -> { throw new IllegalStateException(); });
      fixture.stream.addCleanup(later::incrementAndGet);
      fixture.stream.listen(UserPermissionsChangedEvent.class, _ -> later.addAndGet(100));
      SoulFireAPI.postEvent(new UserPermissionsChangedEvent(fixture.server, fixture.owner));
      assertEquals(1, fixture.cleaned.get());
      assertEquals(1, later.get());
      assertEquals(0, fixture.lookups.get());
      assertTrue(fixture.terminal.isEmpty());
    }
  }

  @Test
  void registrationsDuringCleanupReleaseEveryResourceOnceWithoutBlocking() throws Exception {
    try (var fixture = new Fixture(); var executor = Executors.newFixedThreadPool(4)) {
      assertTrue(fixture.stream.start());
      var enteredCleanup = new CountDownLatch(1);
      var resumeCleanup = new CountDownLatch(1);
      fixture.stream.addCleanup(() -> {
        enteredCleanup.countDown();
        try { resumeCleanup.await(); }
        catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new AssertionError(error); }
      });
      var closing = executor.submit(fixture.stream::close);
      try {
        assertTrue(enteredCleanup.await(5, TimeUnit.SECONDS));
        var resources = new ArrayList<AtomicInteger>();
        var futures = new ArrayList<Future<?>>();
        for (var i = 0; i < 100; i++) {
          var resource = new AtomicInteger();
          resources.add(resource);
          futures.add(executor.submit(() -> fixture.stream.addCleanup(resource::incrementAndGet)));
        }
        for (var future : futures) future.get(5, TimeUnit.SECONDS);
        assertTrue(resources.stream().allMatch(resource -> resource.get() == 1));
        assertEquals(1, fixture.cleaned.get());
      } finally {
        resumeCleanup.countDown();
        closing.get(5, TimeUnit.SECONDS);
      }
    }
  }

  private static final class Fixture implements AutoCloseable {
    final SoulFireServer server = mock();
    final AuthSystem auth = mock();
    final SoulFireUser user = mock();
    final UUID owner = UUID.randomUUID();
    final Instant issuedAt = Instant.now();
    final AtomicBoolean closed = new AtomicBoolean();
    final AtomicInteger cleaned = new AtomicInteger();
    final AtomicInteger lookups = new AtomicInteger();
    final ArrayList<Status.Code> terminal = new ArrayList<>();
    final StreamObserver<Object> observer = mock();
    final AuthorizedStream stream;
    Optional<SoulFireUser> authenticated = Optional.of(user);

    Fixture() {
      when(server.authSystem()).thenReturn(auth);
      when(user.getUniqueId()).thenReturn(owner);
      when(user.getIssuedAt()).thenReturn(issuedAt);
      when(user.getSessionVersion()).thenReturn(3L);
      when(user.hasPermission(any())).thenReturn(true);
      when(auth.authenticateBySubject(owner, issuedAt, 3L)).thenAnswer(_ -> { lookups.incrementAndGet(); return authenticated; });
      doAnswer(call -> { terminal.add(Status.fromThrowable(call.getArgument(0)).getCode()); return null; }).when(observer).onError(any());
      doAnswer(_ -> { terminal.add(Status.Code.OK); return null; }).when(observer).onCompleted();
      stream = new AuthorizedStream(server, user,
        PermissionContext.instance(InstancePermission.READ_BOT_INFO, UUID.randomUUID()), observer, closed);
      stream.addCleanup(cleaned::incrementAndGet);
    }

    @Override
    public void close() { stream.close(); }
  }
}
