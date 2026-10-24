import { Code, ConnectError } from "@connectrpc/connect";
import { InventoryArea, InventoryService } from "@soulfiremc/sdk/generated/soulfire/inventory_pb";
import { Effect } from "effect";
import { CheckFailed, check, type E2ETest, eventually, rcon } from "../harness.ts";

const NAMES = ["FleetE2EA", "FleetE2EB"];
const CHEST = { x: 4263, y: 64, z: 8 };
const selector = { itemIds: ["minecraft:diamond"] };
const materialCount = (nbt: string) =>
  [...nbt.matchAll(/\{[^{}]+\}/g)]
    .filter(([entry]) => entry.includes('id: "minecraft:diamond"'))
    .reduce((sum, [entry]) => sum + Number(entry.match(/count: (\d+)/)?.[1] ?? 0), 0);

export default {
  name: "shared-container-confirmation",
  run: ({ client }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const online = yield* rcon("list");
        yield* check(
          NAMES.every((name) => !online.includes(name)),
          "Fixture bots already online",
        );
        const instance = yield* Effect.acquireRelease(
          client.getOrCreateInstance("e2e-shared-container-confirm", {
            server: `127.0.0.1:${process.env.SOULFIRE_E2E_MINECRAFT_PORT ?? 25566}`,
          }),
          (owned) => owned.delete().pipe(Effect.ignore),
        );
        for (const namespace of [
          "auto-eat",
          "auto-armor",
          "auto-totem",
          "auto-jump",
          "anti-afk",
          "auto-respawn",
          "auto-reconnect",
        ]) {
          yield* instance.setConfigEntry({
            namespace,
            key: "enabled",
            value: { kind: { case: "boolValue", value: false } },
          });
        }
        const bots = yield* Effect.forEach(NAMES, (name) =>
          instance.getOrCreateBot(name, { readyTimeoutMs: 60000 }),
        );
        const forced = (yield* rcon("forceload query 4256 0", { check: false })).includes(
          "not marked",
        );
        yield* rcon("forceload add 4256 0");
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            for (const [index, bot] of bots.entries()) {
              const name = NAMES[index]!;
              yield* bot.closeContainer().pipe(Effect.ignore);
              yield* Effect.sleep("200 millis");
              yield* rcon(`gamemode creative ${name}`);
              yield* rcon(`clear ${name}`, { check: false });
              yield* rcon(`effect clear ${name}`, { check: false });
              yield* bot.selectHotbar(0);
              yield* rcon(`tp ${name} 0.5 64 0.5`);
              yield* eventually(
                bot.world
                  .player()
                  .pipe(
                    Effect.tap((player) =>
                      check(
                        player.position?.x === 0.5 &&
                          player.position.y === 64 &&
                          player.position.z === 0.5,
                        "Cleanup position did not arrive",
                      ),
                    ),
                  ),
                "10 seconds",
                "100 millis",
              );
              yield* bot.stop();
            }
            yield* rcon("data modify block 4263 64 8 Items set value []", { check: false });
            yield* rcon("setblock 4263 64 8 air", { check: false });
            yield* rcon("fill 4260 63 4 4268 63 12 air");
            if (forced) yield* rcon("forceload remove 4256 0");
          }).pipe(Effect.ignore),
        );
        yield* rcon("fill 4260 63 4 4268 63 12 stone");
        yield* rcon("setblock 4263 64 8 chest");
        for (const [index, bot] of bots.entries()) {
          yield* rcon(`gamemode survival ${NAMES[index]}`);
          yield* rcon(`tp ${NAMES[index]} 4264.5 64 ${index ? 9.5 : 7.5}`);
          yield* eventually(
            bot.world
              .player()
              .pipe(
                Effect.tap((player) =>
                  check(
                    player.position?.x === 4264.5 && player.health === 20,
                    "Fixture bot did not arrive",
                  ),
                ),
              ),
            "10 seconds",
            "100 millis",
          );
          yield* bot.waitForChunks({ radiusChunks: 2, timeoutMs: 10000 });
        }
        const service = client.service(InventoryService);
        // Two independent controls, repeated full/partial withdrawals, then two
        // depositors competing for the only available stack in a full chest.
        for (let round = 0; round < 18; round++) {
          const deposit = round >= 14;
          const amount = round >= 10 && round < 14 ? 32 : 64;
          for (const bot of bots) yield* bot.closeContainer();
          yield* Effect.sleep("200 millis");
          for (const name of NAMES) {
            yield* rcon(`clear ${name}`, { check: false });
            if (deposit) yield* rcon(`give ${name} diamond 64`);
          }
          const items = deposit
            ? Array.from(
                { length: 26 },
                (_, slot) => `{Slot:${slot + 1}b,id:"stone",count:64}`,
              ).join(",")
            : '{Slot:0b,id:"minecraft:diamond",count:64}';
          yield* rcon(`data merge block 4263 64 8 {Items:[${items}]}`);
          const containers = yield* Effect.forEach(bots, (bot) => bot.inventory.open(CHEST));
          for (const container of containers) {
            yield* eventually(
              container
                .refresh()
                .pipe(
                  Effect.tap((state) =>
                    check(
                      state.slots.filter(
                        (slot) => slot.area === InventoryArea.CONTAINER && !!slot.item,
                      ).length === (deposit ? 26 : 1),
                      "Shared contents did not arrive",
                    ),
                  ),
                ),
              "5 seconds",
              "50 millis",
            );
          }
          const actors = round < 2 ? [round] : [0, 1];
          const results = yield* Effect.forEach(
            actors,
            (index) =>
              Effect.tryPromise({
                try: async () => {
                  try {
                    const result = await service.transferItems({
                      scope: { instanceId: instance.id, botId: bots[index]!.id },
                      menuId: containers[index]!.snapshot.menuId,
                      selector,
                      count: amount,
                      from: deposit ? InventoryArea.PLAYER : InventoryArea.CONTAINER,
                      to: deposit ? InventoryArea.CONTAINER : InventoryArea.PLAYER,
                    });
                    const reported = result.container?.slots
                      .filter(
                        (slot) =>
                          [InventoryArea.MAIN, InventoryArea.HOTBAR].includes(slot.area) &&
                          slot.item?.itemId === "minecraft:diamond",
                      )
                      .reduce((sum, slot) => sum + slot.item!.count, 0);
                    return { index, success: true, reported };
                  } catch (cause) {
                    if (
                      cause instanceof ConnectError &&
                      [Code.Aborted, Code.FailedPrecondition, Code.ResourceExhausted].includes(
                        cause.code,
                      )
                    )
                      return { index, success: false };
                    throw cause;
                  }
                },
                catch: (cause) => new CheckFailed({ message: String(cause) }),
              }),
            { concurrency: 2 },
          );
          for (const bot of bots) yield* bot.closeContainer();
          yield* Effect.sleep("300 millis");
          let retained = materialCount(yield* rcon("data get block 4263 64 8 Items"));
          let completed = 0;
          for (const [index, name] of NAMES.entries()) {
            const inventory = materialCount(yield* rcon(`data get entity ${name} Inventory`));
            retained += inventory;
            const result = results.find((value) => value.index === index);
            if (result?.success) {
              completed++;
              yield* check(
                inventory === (deposit ? 0 : amount) && result.reported === inventory,
                "A successful shared transfer disagrees with vanilla inventory",
              );
            }
            yield* check(
              !(yield* bots[index]!.inventory.snapshot()).carried,
              "A shared transfer retained a cursor stack after closing",
            );
          }
          yield* check(completed > 0, "Neither shared actor completed");
          yield* check(
            retained === (deposit ? 128 : 64),
            "Shared transfers lost or duplicated items",
          );
        }
      }),
    ),
} satisfies E2ETest;
