import time
import sys
import sonora.wsgi

import a2wsgi
import bjoern
import echo_pb2
import echo_pb2_grpc

import grpc


class Echo(echo_pb2_grpc.EchoServiceServicer):
    def Echo(self, request, context):
        return echo_pb2.EchoResponse(message=request.message)

    def EchoAbort(self, request, context):
        context.set_code(grpc.StatusCode.ABORTED)
        return echo_pb2.EchoResponse(message=request.message)

    def ServerStreamingEcho(self, request, context):
        for _ in range(request.message_count):
            yield echo_pb2.EchoResponse(message=request.message)
            time.sleep(request.message_interval.seconds)

    def ServerStreamingEchoAbort(self, request, context):
        for _ in range(request.message_count // 2):
            yield echo_pb2.EchoResponse(message=request.message)
        context.set_code(grpc.StatusCode.ABORTED)


wsgi_app = sonora.wsgi.grpcWSGI()
echo_pb2_grpc.add_EchoServiceServicer_to_server(Echo(), wsgi_app)
asgi_app = a2wsgi.wsgi.WSGIMiddleware(wsgi_app)


def main(args):
    bjoern.listen(wsgi_app, "0.0.0.0", 8888)
    bjoern.run()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
