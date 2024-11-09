#!/usr/bin/env python
import sys
import threading

import daphne
import daphne.server
from asgi import asgi_app
from connectrpc.conformance.v1.server_compat_pb2 import (
    ServerCompatRequest,
    ServerCompatResponse,
)


def main():
    try:
        request_size = int.from_bytes(sys.stdin.buffer.read(4), byteorder="big")
        request_buf = sys.stdin.buffer.read(request_size)
        request = ServerCompatRequest.FromString(request_buf)
    except Exception:
        sys.stderr.write("Invalid ServerCompatRequest on stdin")
        sys.exit(1)

    wait = threading.Barrier(1)
    server = daphne.server.Server(
        asgi_app,
        endpoints=["tcp:port=8080:interface=localhost"],
        ready_callable=wait.reset,
    )

    def run_server():
        server.run()

    threading.Thread(target=run_server).start()
    wait.wait()

    response = ServerCompatResponse(host="localhost", port=8080)

    response_buf = response.SerializeToString()
    response_size = len(response_buf)
    sys.stdout.buffer.write(response_size.to_bytes(length=4, byteorder="big"))
    sys.stdout.buffer.write(response_buf)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()
