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
package com.soulfiremc.launcher.bootstrap;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StartupProgressTest {
  @Test
  void countConcurrentCacheHitsDownloadsAndExtractionsWithoutLosingUpdates() throws Exception {
    var progress = new StartupProgress(120);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var work = new ArrayList<Future<?>>();
      for (var index = 0; index < 120; index++) {
        var id = index;
        work.add(executor.submit(() -> {
          if (id % 3 == 0) {
            progress.cached();
          } else {
            try (var transfer = progress.start(asset(id, id % 3 == 2))) {
              for (var chunk = 0; chunk < 16; chunk++) {
                transfer.advance(64);
              }
              transfer.complete();
            }
          }
        }));
      }
      for (var future : work) {
        future.get();
      }
    }

    var status = progress.snapshot();
    assertEquals(120, status.ready());
    assertEquals(40, status.cached());
    assertEquals(40, status.downloaded());
    assertEquals(40, status.unpacked());
    assertEquals(40 * 1024, status.downloadedBytes());
    assertEquals(40 * 1024, status.unpackedBytes());
    assertEquals(0, status.activeDownloads());
    assertEquals(0, status.activeUnpacks());
  }

  @Test
  void reportPartialTransfersWithoutMarkingAbandonedFilesReady() {
    var progress = new StartupProgress(2);
    try (var download = progress.start(asset(1, false)); var unpack = progress.start(asset(2, true))) {
      download.advance(256);
      unpack.advance(512);

      var active = progress.snapshot();
      assertEquals(0, active.ready());
      assertEquals(1, active.activeDownloads());
      assertEquals(1, active.activeUnpacks());
      assertEquals(256, active.downloadedBytes());
      assertEquals(512, active.unpackedBytes());
      unpack.advance(512);
      unpack.complete();
    }

    var finished = progress.snapshot();
    assertEquals(1, finished.ready());
    assertEquals(0, finished.downloaded());
    assertEquals(1, finished.unpacked());
    assertEquals(0, finished.activeDownloads());
    assertEquals(0, finished.activeUnpacks());
  }

  private static LauncherManifest.Asset asset(int id, boolean embedded) {
    return new LauncherManifest.Asset("artifact " + id, "libraries/test/" + id + ".jar", "library", 1024, "0".repeat(64),
      embedded ? null : "https://example.invalid/" + id + ".jar", embedded ? "META-INF/assets/" + id + ".jar" : null, null);
  }
}
