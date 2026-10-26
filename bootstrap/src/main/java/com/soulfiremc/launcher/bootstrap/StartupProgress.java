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

import java.util.Comparator;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

final class StartupProgress {
  private final int total;
  private final long started = System.nanoTime();
  private final AtomicInteger cached = new AtomicInteger();
  private final AtomicInteger downloaded = new AtomicInteger();
  private final AtomicInteger unpacked = new AtomicInteger();
  private final LongAdder downloadedBytes = new LongAdder();
  private final LongAdder unpackedBytes = new LongAdder();
  private final ConcurrentHashMap<String, Transfer> active = new ConcurrentHashMap<>();

  StartupProgress(int total) {
    this.total = total;
  }

  void cached() {
    cached.incrementAndGet();
  }

  Transfer start(LauncherManifest.Asset asset) {
    var transfer = new Transfer(asset);
    active.put(asset.path(), transfer);
    return transfer;
  }

  Snapshot snapshot() {
    var transfers = active.values().stream().toList();
    var downloads = (int) transfers.stream().filter(transfer -> transfer.asset.resource() == null).count();
    return new Snapshot(total, cached.get(), downloaded.get(), unpacked.get(), downloadedBytes.sum(), unpackedBytes.sum(),
      downloads, transfers.size() - downloads);
  }

  void report() {
    var status = snapshot();
    if (status.ready() == total) {
      return;
    }
    log("Runtime files: " + status.ready() + "/" + total + " ready (" + status.cached() + " cached, "
      + status.downloaded() + " downloaded, " + status.unpacked() + " unpacked); "
      + "active downloads: " + status.activeDownloads() + ", extractions: " + status.activeUnpacks());
    active.values().stream()
      .sorted(Comparator.comparingLong((Transfer transfer) -> transfer.asset.size()).reversed())
      .limit(2)
      .forEach(transfer -> {
        var bytes = transfer.bytes.get();
        var percent = transfer.asset.size() == 0 ? 100 : Math.min(100, bytes * 100 / transfer.asset.size());
        log((transfer.asset.resource() == null ? "Downloading " : "Unpacking ") + transfer.asset.name() + ": "
          + size(bytes) + " / " + size(transfer.asset.size()) + " (" + percent + "%)");
      });
  }

  void finish() {
    var status = snapshot();
    var seconds = String.format(Locale.ROOT, "%.1f", (System.nanoTime() - started) / 1_000_000_000.0);
    log("Runtime ready in " + seconds + " s: " + status.cached() + " cached, " + status.downloaded() + " downloaded ("
      + size(status.downloadedBytes()) + "), " + status.unpacked() + " unpacked (" + size(status.unpackedBytes()) + ")");
  }

  static void log(String message) {
    IO.println("[bootstrap] " + message);
  }

  static String size(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    }
    return bytes < 1024 * 1024
      ? String.format(Locale.ROOT, "%.1f KiB", bytes / 1024.0)
      : String.format(Locale.ROOT, "%.1f MiB", bytes / (1024.0 * 1024));
  }

  record Snapshot(int total, int cached, int downloaded, int unpacked, long downloadedBytes, long unpackedBytes,
                  int activeDownloads, int activeUnpacks) {
    int ready() {
      return cached + downloaded + unpacked;
    }
  }

  final class Transfer implements AutoCloseable {
    private final LauncherManifest.Asset asset;
    private final AtomicLong bytes = new AtomicLong();
    private boolean complete;

    private Transfer(LauncherManifest.Asset asset) {
      this.asset = asset;
    }

    void advance(int count) {
      bytes.addAndGet(count);
      (asset.resource() == null ? downloadedBytes : unpackedBytes).add(count);
    }

    void complete() {
      complete = true;
    }

    @Override
    public void close() {
      if (active.remove(asset.path(), this) && complete) {
        (asset.resource() == null ? downloaded : unpacked).incrementAndGet();
      }
    }
  }
}
