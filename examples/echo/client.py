from google.protobuf.duration_pb2 import Duration
import sonora.client
import echo_pb2_grpc, echo_pb2

with sonora.client.insecure_web_channel("http://localhost:8888") as c:
    x = echo_pb2_grpc.EchoServiceStub(c)
    d = Duration(seconds=1)

    r = x.Echo(echo_pb2.EchoRequest(message="beep"))
    print(r)

    for r in x.ServerStreamingEcho(
        echo_pb2.ServerStreamingEchoRequest(
            message="honk", message_count=10, message_interval=d
        )
    ):
        print(r)

    for r in x.ServerStreamingEchoAbort(
        echo_pb2.ServerStreamingEchoRequest(
            message="honk", message_count=10, message_interval=d
        )
    ):
        print(r)
