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

import com.soulfiremc.server.SoulFireServer;
import com.soulfiremc.server.api.SoulFireAPI;
import com.soulfiremc.server.api.event.SoulFireEvent;
import com.soulfiremc.server.api.event.UserPermissionsChangedEvent;
import com.soulfiremc.server.api.event.UserSessionsInvalidatedEvent;
import com.soulfiremc.server.user.PermissionContext;
import com.soulfiremc.server.user.SoulFireUser;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/// Owns authorization and cleanup for a long-lived subscription.
/// Output and terminal callbacks share the observer's monitor.
@Slf4j
final class AuthorizedStream implements AutoCloseable {
  private final SoulFireServer server;
  private final SoulFireUser user;
  private final PermissionContext permission;
  private final StreamObserver<?> observer;
  private final AtomicBoolean closed;
  private final List<Runnable> cleanupActions = new ArrayList<>();

  AuthorizedStream(SoulFireServer server, SoulFireUser user, PermissionContext permission,
                   StreamObserver<?> observer, AtomicBoolean closed) {
    this.server = server;
    this.user = user;
    this.permission = permission;
    this.observer = observer;
    this.closed = closed;
  }

  boolean start() {
    if (closed.get()) return false;
    try {
      listen(UserSessionsInvalidatedEvent.class, event -> {
        if (event.soulFireServer() == server && event.userId().equals(user.getUniqueId())
          && user.getSessionVersion() < event.minimumSessionVersion()) {
          fail(Status.UNAUTHENTICATED.withDescription("Stream authorization was invalidated"));
        }
      });
      listen(UserPermissionsChangedEvent.class, event -> {
        if (event.soulFireServer() == server && event.userId().equals(user.getUniqueId())) refreshAuthorization();
      });
      // Register before checking to cover changes after request authentication.
      refreshAuthorization();
      return !closed.get();
    } catch (RuntimeException | Error error) {
      close();
      throw error;
    }
  }

  private void refreshAuthorization() {
    if (closed.get()) return;
    Status rejected = null;
    try {
      var authenticated = server.authSystem().authenticateBySubject(
        user.getUniqueId(), user.getIssuedAt(), user.getSessionVersion());
      if (authenticated.isEmpty()) {
        rejected = Status.UNAUTHENTICATED.withDescription("Stream authorization was invalidated");
      } else if (!authenticated.orElseThrow().hasPermission(permission)) {
        rejected = Status.PERMISSION_DENIED.withDescription("Stream access was revoked");
      }
    } catch (RuntimeException error) {
      rejected = Status.INTERNAL.withDescription("Stream authorization failed").withCause(error);
    }
    if (rejected != null) fail(rejected);
  }

  <E extends SoulFireEvent> void listen(Class<E> eventType, Consumer<E> listener) {
    if (closed.get()) return;
    SoulFireAPI.registerListener(eventType, listener);
    addCleanup(() -> SoulFireAPI.unregisterListener(eventType, listener));
  }

  void addCleanup(Runnable action) {
    synchronized (cleanupActions) {
      if (!closed.get()) {
        cleanupActions.add(action);
        return;
      }
    }
    runCleanup(action);
  }

  void fail(Status status) {
    finish(status);
  }

  void complete() {
    finish(null);
  }

  private void finish(Status status) {
    var won = false;
    try {
      synchronized (observer) {
        if (!closed.compareAndSet(false, true)) return;
        won = true;
        if (status == null) observer.onCompleted();
        else observer.onError(status.asRuntimeException());
      }
    } finally {
      // Child subscriptions may acquire the observer monitor while being removed.
      if (won) release();
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) release();
  }

  private void release() {
    List<Runnable> actions;
    synchronized (cleanupActions) {
      actions = List.copyOf(cleanupActions);
      cleanupActions.clear();
    }
    actions.forEach(AuthorizedStream::runCleanup);
  }

  private static void runCleanup(Runnable action) {
    try { action.run(); }
    catch (Throwable error) { log.debug("Failed to clean up stream subscription", error); }
  }
}
