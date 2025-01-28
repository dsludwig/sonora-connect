import asyncio
import time
import sys
import sonora.asgi

import hypercorn.asyncio
import echo_pb2
import echo_pb2_grpc

import grpc


class Echo(echo_pb2_grpc.EchoServiceServicer):
    async def Echo(self, request, context):
        return echo_pb2.EchoResponse(message=request.message)

    async def EchoAbort(self, request, context):
        context.set_code(grpc.StatusCode.ABORTED)
        return echo_pb2.EchoResponse(message=request.message)

    async def ServerStreamingEcho(self, request, context):
        for _ in range(request.message_count):
            yield echo_pb2.EchoResponse(message=request.message)
            await asyncio.sleep(request.message_interval.seconds)

    async def ServerStreamingEchoAbort(self, request, context):
        for _ in range(request.message_count // 2):
            yield echo_pb2.EchoResponse(message=request.message)
        context.set_code(grpc.StatusCode.ABORTED)


asgi_app = sonora.asgi.grpcASGI()
echo_pb2_grpc.add_EchoServiceServicer_to_server(Echo(), asgi_app)


def main(args):
    cfg = hypercorn.Config()
    cfg.bind = ["localhost:8888"]
    asyncio.run(hypercorn.asyncio.serve(asgi_app, cfg))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
