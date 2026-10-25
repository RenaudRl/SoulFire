from typing import cast

import pytest
from connectrpc.client import ConnectClient
from effect_py import from_async, gen, run_async, scoped

from soulfire import SoulFire
from soulfire._install import LocalServerHandle
from soulfire.plugin.example.v1.example_connect import ExamplePluginServiceClient
from soulfire.plugin.example.v1.example_pb2 import EchoRequest, EchoResponse


class CloseRpcClient(ExamplePluginServiceClient):
    calls = 0

    async def close(self, request: EchoRequest) -> EchoResponse:
        self.calls += 1
        return EchoResponse(message=request.message)


async def test_connection_scope_closes_the_transport_without_calling_its_close_rpc() -> None:
    owned: list[ConnectClient] = []
    plugin: CloseRpcClient | None = None

    @gen
    def workflow():
        nonlocal plugin
        client = yield from SoulFire.unauthenticated("http://localhost:1")
        plugin = client.service(CloseRpcClient)
        owned.extend(client._clients)
        request = EchoRequest(message="Grüße 日本語")
        response = yield from from_async(lambda: plugin.close(request))
        assert response.message == request.message

    await run_async(scoped(workflow).or_die())
    assert plugin is not None
    assert plugin.calls == 1
    assert all(client._closed for client in owned)


async def test_explicit_close_releases_each_owned_transport_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = ConnectClient.close
    closed: list[ConnectClient] = []

    async def close(client: ConnectClient) -> None:
        closed.append(client)
        await original(client)

    monkeypatch.setattr(ConnectClient, "close", close)
    owned: list[ConnectClient] = []

    @gen
    def workflow():
        client = yield from SoulFire.unauthenticated("http://localhost:1")
        plugin = client.service(CloseRpcClient)
        owned.extend(client._clients)
        yield from client.close()
        yield from client.close()
        assert plugin.calls == 0
        assert all(transport._closed for transport in owned)
        owned.append(client.service(ExamplePluginServiceClient))

    await run_async(scoped(workflow).or_die())
    assert {id(client) for client in closed} == {id(client) for client in owned}
    assert len(closed) == len(owned)
    assert all(client._closed for client in owned)


async def test_failed_transport_cleanup_still_closes_other_transports_and_local_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = ConnectClient.close
    closed: list[ConnectClient] = []
    owned: list[ConnectClient] = []
    failure = RuntimeError()
    failing: ConnectClient | None = None

    async def close(client: ConnectClient) -> None:
        closed.append(client)
        await original(client)
        if client is failing:
            raise failure

    class LocalServer:
        calls = 0

        def close(self) -> None:
            self.calls += 1
            assert all(client._closed for client in owned)

    handle = LocalServer()
    monkeypatch.setattr(ConnectClient, "close", close)

    @gen
    def workflow():
        nonlocal failing
        client = yield from SoulFire.unauthenticated("http://localhost:1")
        failing = client.service(ExamplePluginServiceClient)
        owned.extend(client._clients)
        client._local_server_handle = cast(LocalServerHandle, handle)

    with pytest.raises(RuntimeError) as caught:
        await run_async(scoped(workflow).or_die())
    assert caught.value is failure
    assert {id(client) for client in closed} == {id(client) for client in owned}
    assert len(closed) == len(owned)
    assert handle.calls == 1
