import asyncio

from google.protobuf.duration_pb2 import Duration
import sonora.aio
import echo_pb2_grpc, echo_pb2


async def example():
    async with sonora.aio.insecure_web_channel("http://localhost:8888") as c:
        x = echo_pb2_grpc.EchoServiceStub(c)
        d = Duration(seconds=1)

        r = await x.Echo(echo_pb2.EchoRequest(message="beep"))
        print(r)

        with x.ServerStreamingEcho(
            echo_pb2.ServerStreamingEchoRequest(
                message="honk", message_count=10, message_interval=d
            )
        ) as call:
            async for r in call:
                print(r)

        with x.ServerStreamingEchoAbort(
            echo_pb2.ServerStreamingEchoRequest(
                message="honk", message_count=10, message_interval=d
            )
        ) as call:
            async for r in call:
                print(r)


asyncio.run(example())
