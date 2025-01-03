import grpc
import pytest
from google.protobuf.empty_pb2 import Empty

from tests import helloworld_pb2


@pytest.mark.asyncio
async def test_helloworld_sayhello(asgi_greeter):
    for name in ("you", "world"):
        request = helloworld_pb2.HelloRequest(name=name)
        response = await asgi_greeter.SayHello(request)
        assert response.message != name
        assert name in response.message


@pytest.mark.asyncio
async def test_helloworld_unarytimeout(asgi_greeter):
    request = helloworld_pb2.TimeoutRequest(seconds=0.1)
    with pytest.raises(grpc.RpcError) as exc:
        await asgi_greeter.UnaryTimeout(request, timeout=0.001)
    assert exc.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED


@pytest.mark.asyncio
async def test_helloworld_streamtimeout(asgi_greeter):
    request = helloworld_pb2.TimeoutRequest(seconds=0.1)
    response = asgi_greeter.StreamTimeout(request, timeout=0.001)

    with pytest.raises(grpc.RpcError) as exc:
        async for r in response:
            pass
    assert exc.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED


@pytest.mark.asyncio
async def test_helloworld_sayhelloslowly(asgi_greeter):
    for name in ("you", "world"):
        request = helloworld_pb2.HelloRequest(name=name)
        response = asgi_greeter.SayHelloSlowly(request)
        message = "".join([r.message async for r in response])
        assert message != name
        assert name in message


@pytest.mark.asyncio
async def test_helloworld_sayhelloslowly_read(asgi_greeter):
    for name in ("you", "world"):
        request = helloworld_pb2.HelloRequest(name=name)
        message = ""
        with asgi_greeter.SayHelloSlowly(request) as stream:
            chunk = await stream.read()
            while chunk:
                message += chunk.message
                chunk = await stream.read()
        assert message != name
        assert name in message


@pytest.mark.asyncio
async def test_helloworld_abort(asgi_greeter):
    with pytest.raises(grpc.RpcError) as exc:
        await asgi_greeter.Abort(Empty())

    assert exc.value.code() == grpc.StatusCode.ABORTED
    assert exc.value.details() == "test aborting"


@pytest.mark.asyncio
async def test_helloworld_unary_metadata_ascii(asgi_greeter):
    request = helloworld_pb2.HelloRequest(name="metadata-key")
    call = asgi_greeter.HelloMetadata(request, metadata=[("metadata-key", "honk")])
    result = await call
    assert repr("honk") == result.message

    initial_metadata = await call.initial_metadata()
    trailing_metadata = await call.trailing_metadata()

    assert dict(initial_metadata)["initial-metadata-key"] == repr("honk")
    assert dict(trailing_metadata)["trailing-metadata-key"] == repr("honk")


@pytest.mark.asyncio
async def test_helloworld_unary_metadata_binary(asgi_greeter):
    request = helloworld_pb2.HelloRequest(name="metadata-key-bin")
    call = asgi_greeter.HelloMetadata(
        request, metadata=[("metadata-key-bin", b"\0\1\2\3")]
    )
    result = await call
    assert repr(b"\0\1\2\3") == result.message

    initial_metadata = await call.initial_metadata()
    trailing_metadata = await call.trailing_metadata()

    assert (
        dict(initial_metadata)["initial-metadata-key-bin"] == repr(b"\0\1\2\3").encode()
    )
    assert (
        dict(trailing_metadata)["trailing-metadata-key-bin"]
        == repr(b"\0\1\2\3").encode()
    )


@pytest.mark.asyncio
async def test_helloworld_stream_metadata_ascii(asgi_greeter):
    request = helloworld_pb2.HelloRequest(name="metadata-key")
    result = asgi_greeter.HelloStreamMetadata(
        request, metadata=[("metadata-key", "honk")]
    )

    message = "".join([m.message async for m in result])

    assert "honk" == message

    initial_metadata = await result.initial_metadata()
    trailing_metadata = await result.trailing_metadata()

    assert dict(initial_metadata)["initial-metadata-key"] == repr("honk")
    assert dict(trailing_metadata)["trailing-metadata-key"] == repr("honk")
