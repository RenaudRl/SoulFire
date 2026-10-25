from collections.abc import AsyncIterator, Iterator
from typing import Any, cast

import pytest
from connectrpc.client import ConnectClient
from connectrpc.protocol import ProtocolType
from effect_py import gen, run_async, scoped, succeed
from google.protobuf.struct_pb2 import Value

from soulfire import RequiredPlugin, SoulFire
from soulfire.bot_connect import BotServiceClient
from soulfire.bot_live_connect import BotLiveServiceClient
from soulfire.bot_pb2 import (
    BOT_DESIRED_STATE_RUNNING,
    BOT_DESIRED_STATE_STOPPED,
    BOT_RUNTIME_STATE_RUNNING,
    BOT_RUNTIME_STATE_STOPPED,
    BotListEntry,
    BotListResponse,
    BotStatus,
    RestartBotsRequest,
    RestartBotsResponse,
    SetBotsDesiredStateRequest,
    SetBotsDesiredStateResponse,
)
from soulfire.client import SoulFireInstance
from soulfire.common_pb2 import ADMIN, SettingsNamespace
from soulfire.instance_connect import InstanceServiceClient
from soulfire.instance_live_connect import InstanceLiveServiceClient
from soulfire.instance_live_pb2 import InstanceEvent, WatchInstanceEventsRequest
from soulfire.instance_pb2 import InstanceConfig, InstanceInfo, InstanceInfoResponse
from soulfire.plugin_api_pb2 import PluginApiDescriptor
from soulfire.sdk_pb2 import (
    SDK_TRANSPORT_GRPC_WEB,
    SdkApiVersion,
    SdkCapability,
    SdkHandshakeResponse,
    SdkIdentity,
    SdkLimit,
)


class FakeGeneratedClient(ConnectClient):
    def __init__(self, address: str, **options: Any) -> None:
        super().__init__(address, **options)
        self.address = address
        self.options = options

    @property
    def closed(self) -> bool:
        return self._closed


class FakeSdkClient(FakeGeneratedClient):
    request = None

    async def handshake(self, request: object) -> SdkHandshakeResponse:
        self.__class__.request = request
        return handshake_response()


class FakePluginApiClient(FakeGeneratedClient):
    pass


class FakeSdkClientSync(FakeGeneratedClient):
    request = None

    async def handshake(self, request: object) -> SdkHandshakeResponse:
        self.__class__.request = request
        return handshake_response()


class FakePluginApiClientSync(FakeGeneratedClient):
    pass


class FakeBotService:
    desired_state_request: SetBotsDesiredStateRequest | None = None

    async def get_bot_list(self, *_args: object, **_kwargs: object) -> BotListResponse:
        return BotListResponse(
            bots=[
                BotListEntry(
                    profile_id=profile_id,
                    status=BotStatus(
                        profile_id=profile_id,
                        desired_state=BOT_DESIRED_STATE_STOPPED,
                        runtime_state=BOT_RUNTIME_STATE_STOPPED,
                    ),
                )
                for profile_id in ("first", "second", "third")
            ]
        )

    async def set_bots_desired_state(
        self, request: SetBotsDesiredStateRequest, **_kwargs: object
    ) -> SetBotsDesiredStateResponse:
        self.desired_state_request = request
        return SetBotsDesiredStateResponse()


class FakeInstanceService:
    async def get_instance_info(self, *_args: object, **_kwargs: object) -> InstanceInfoResponse:
        return InstanceInfoResponse(
            info=InstanceInfo(
                config=InstanceConfig(
                    settings=[
                        SettingsNamespace(
                            namespace="account",
                            entries=[
                                SettingsNamespace.SettingsEntry(
                                    key="shuffle-accounts", value=Value(bool_value=True)
                                )
                            ],
                        )
                    ]
                )
            )
        )


class FakeMixedBotService(FakeBotService):
    restart_request: RestartBotsRequest | None = None

    async def get_bot_list(self, *_args: object, **_kwargs: object) -> BotListResponse:
        return BotListResponse(
            bots=[
                BotListEntry(
                    profile_id="desired",
                    status=BotStatus(
                        profile_id="desired",
                        desired_state=BOT_DESIRED_STATE_RUNNING,
                        runtime_state=BOT_RUNTIME_STATE_RUNNING,
                    ),
                ),
                BotListEntry(
                    profile_id="stopped",
                    status=BotStatus(
                        profile_id="stopped",
                        desired_state=BOT_DESIRED_STATE_STOPPED,
                        runtime_state=BOT_RUNTIME_STATE_STOPPED,
                    ),
                ),
            ]
        )

    async def restart_bots(
        self, request: RestartBotsRequest, **_kwargs: object
    ) -> RestartBotsResponse:
        self.restart_request = request
        return RestartBotsResponse()


class FakeInstanceLiveService:
    request: WatchInstanceEventsRequest | None = None

    def watch_instance_events(
        self, request: WatchInstanceEventsRequest, **_kwargs: object
    ) -> AsyncIterator[InstanceEvent]:
        self.request = request

        async def stream() -> AsyncIterator[InstanceEvent]:
            yield InstanceEvent(bot_profile_id="bot-id")

        return stream()


class FakeInstanceLiveServiceSync:
    request: WatchInstanceEventsRequest | None = None

    async def watch_instance_events(
        self, request: WatchInstanceEventsRequest, **_kwargs: object
    ) -> Iterator[InstanceEvent]:
        self.request = request
        yield InstanceEvent(bot_profile_id="bot-id")


@pytest.mark.asyncio
async def test_connect_creates_the_public_instance_bot_hierarchy() -> None:

    @gen
    def workflow():
        yield from succeed(None)
        soulfire = yield from SoulFire.unauthenticated(
            "https://soulfire.example.com/", token="token"
        )
        bot = soulfire.instance("instance-id").bot("bot-id")
        assert bot.instance_id == "instance-id"
        assert bot.id == "bot-id"
        assert soulfire.local_server is None
        yield from soulfire.close()

    await run_async(scoped(workflow).or_die())


@pytest.mark.asyncio
async def test_generated_services_always_use_grpc_web() -> None:

    @gen
    def workflow():
        yield from succeed(None)
        soulfire = yield from SoulFire.unauthenticated("https://soulfire.example.com")
        service = soulfire.service(FakeGeneratedClient)
        assert service.address == "https://soulfire.example.com"
        assert service.options["protocol"] is ProtocolType.GRPC_WEB
        yield from soulfire.close()
        assert service.closed

    await run_async(scoped(workflow).or_die())


@pytest.mark.asyncio
async def test_connect_negotiates_before_entering_context(monkeypatch: pytest.MonkeyPatch) -> None:

    @gen
    def workflow():
        yield from succeed(None)
        monkeypatch.setattr("soulfire.client.SdkServiceClient", FakeSdkClient)
        monkeypatch.setattr("soulfire.client.PluginApiServiceClient", FakePluginApiClient)
        soulfire = yield from SoulFire.connect(
            "https://soulfire.example.com",
            token="token",
            required_capabilities=["plugin.rpc.v1"],
            required_plugins=[RequiredPlugin("example", "^1.0.0")],
        )
        assert soulfire.server.id == "server-id"
        assert soulfire.capabilities.supports("plugin.rpc.v1")
        assert soulfire.plugins.require_descriptor("example").api_major_version == 1
        assert FakeSdkClient.request is not None
        assert list(FakeSdkClient.request.required_capabilities) == ["plugin.rpc.v1"]
        assert FakeSdkClient.request.required_plugins[0].version_range == "^1.0.0"

    await run_async(scoped(workflow).or_die())


async def test_effect_connect_negotiates_before_returning(monkeypatch: pytest.MonkeyPatch) -> None:

    @gen
    def workflow():
        yield from succeed(None)
        monkeypatch.setattr("soulfire.client.SdkServiceClient", FakeSdkClientSync)
        monkeypatch.setattr("soulfire.client.PluginApiServiceClient", FakePluginApiClientSync)
        soulfire = yield from SoulFire.connect("https://soulfire.example.com", token="token")
        assert soulfire.identity.username == "developer"
        assert soulfire.limits["grpc.request_bytes"] == 1024
        assert FakeSdkClientSync.request is not None
        assert FakeSdkClientSync.request.minimum_api_version.major == 1

    await run_async(scoped(workflow).or_die())


@pytest.mark.asyncio
async def test_count_selection_honors_shuffle_accounts(monkeypatch: pytest.MonkeyPatch) -> None:

    @gen
    def workflow():
        yield from succeed(None)
        bot_service = FakeBotService()
        monkeypatch.setattr("soulfire.client.random.shuffle", lambda values: values.reverse())
        instance = SoulFireInstance(
            "instance-id",
            cast(BotServiceClient, bot_service),
            cast(BotLiveServiceClient, object()),
            cast(InstanceServiceClient, FakeInstanceService()),
        )
        yield from instance.start(count=1)
        assert bot_service.desired_state_request is not None
        assert list(bot_service.desired_state_request.bot_ids) == ["third"]
        assert bot_service.desired_state_request.desired_state == BOT_DESIRED_STATE_RUNNING

    await run_async(scoped(workflow).or_die())


@pytest.mark.asyncio
async def test_restart_without_selection_only_restarts_desired_bots() -> None:

    @gen
    def workflow():
        yield from succeed(None)
        bot_service = FakeMixedBotService()
        instance = SoulFireInstance(
            "instance-id",
            cast(BotServiceClient, bot_service),
            cast(BotLiveServiceClient, object()),
            cast(InstanceServiceClient, FakeInstanceService()),
        )
        yield from instance.restart()
        assert bot_service.restart_request is not None
        assert list(bot_service.restart_request.bot_ids) == ["desired"]

    await run_async(scoped(workflow).or_die())


@pytest.mark.asyncio
async def test_instance_events_scope_and_default_filter() -> None:

    @gen
    def workflow():
        yield from succeed(None)
        live = FakeInstanceLiveService()
        instance = SoulFireInstance(
            "instance-id",
            cast(BotServiceClient, object()),
            cast(BotLiveServiceClient, object()),
            cast(InstanceServiceClient, object()),
            instance_live=cast(InstanceLiveServiceClient, live),
        )
        event = yield from instance.events(bot_ids=["bot-id", "bot-id"]).run_head()
        assert event.bot_profile_id == "bot-id"
        assert live.request is not None
        assert live.request.instance_id == "instance-id"
        assert list(live.request.filter.bot_ids) == ["bot-id"]
        assert live.request.filter.bot_events.include_entity_events
        assert live.request.filter.bot_events.include_resource_packs
        assert live.request.filter.bot_events.include_titles

    await run_async(scoped(workflow).or_die())


async def test_effect_instance_events_scope_and_default_filter() -> None:

    @gen
    def workflow():
        yield from succeed(None)
        live = FakeInstanceLiveServiceSync()
        instance = SoulFireInstance(
            "instance-id",
            cast(Any, object()),
            cast(Any, object()),
            cast(Any, object()),
            instance_live=cast(InstanceLiveServiceClient, live),
        )
        event = yield from instance.events(bot_ids=["bot-id"]).run_head()
        assert event.bot_profile_id == "bot-id"
        assert live.request is not None
        assert live.request.instance_id == "instance-id"
        assert live.request.filter.bot_events.include_scoreboard

    await run_async(scoped(workflow).or_die())


def handshake_response() -> SdkHandshakeResponse:
    return SdkHandshakeResponse(
        server_id="server-id",
        soulfire_version="3.0.0",
        commit_hash="commit",
        branch_name="main",
        api_version=SdkApiVersion(major=1),
        native_minecraft_version="1.21.11",
        supported_minecraft_versions=["1.21.11"],
        transports=[SDK_TRANSPORT_GRPC_WEB],
        capabilities=[SdkCapability(id="plugin.rpc.v1", revision=1)],
        plugins=[
            PluginApiDescriptor(plugin_id="example", plugin_version="1.0.0", api_major_version=1)
        ],
        limits=[SdkLimit(id="grpc.request_bytes", value=1024)],
        identity=SdkIdentity(
            id="00000000-0000-0000-0000-000000000001",
            username="developer",
            email="dev@example.com",
            role=ADMIN,
            granted_global_permissions=["READ_CLIENT_DATA"],
        ),
    )
