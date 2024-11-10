#!/usr/bin/env python
import logging
import pathlib
import shutil
import socket
import sys
import tempfile
import threading
import time

import daphne
import daphne.server
from asgi import asgi_app
from connectrpc.conformance.v1.server_compat_pb2 import (
    ServerCompatRequest,
    ServerCompatResponse,
)


def get_open_port() -> int:
    with socket.create_server(("", 0)) as server:
        return server.getsockname()[1]


def main():
    try:
        request_size = int.from_bytes(sys.stdin.buffer.read(4), byteorder="big")
        request_buf = sys.stdin.buffer.read(request_size)
        request = ServerCompatRequest.FromString(request_buf)
    except Exception:
        sys.stderr.write("Invalid ServerCompatRequest on stdin")
        sys.exit(1)

    logging.basicConfig(
        level={
            0: logging.WARN,
            1: logging.INFO,
            2: logging.DEBUG,
            3: logging.DEBUG,  # Also turns on asyncio debug
        }[1],
        format="%(asctime)-15s %(levelname)-8s %(message)s",
    )

    wait = threading.Barrier(1)
    port = get_open_port()
    endpoint = f"tcp:port={port}:interface=localhost"
    response = ServerCompatResponse(host="localhost", port=port)

    tmpdir = None
    if request.use_tls:
        tmpdir = pathlib.Path(tempfile.mkdtemp())
        server_creds = request.server_creds
        keyfile = tmpdir / "key.pem"
        certfile = tmpdir / "cert.pem"
        keyfile.write_bytes(server_creds.key)
        certfile.write_bytes(server_creds.cert)
        response.pem_cert = server_creds.cert
        endpoint = f"ssl:port={port}:interface=localhost:privateKey={str(keyfile)}:certKey={str(certfile)}"
    server = daphne.server.Server(
        asgi_app,
        endpoints=[endpoint],
        ready_callable=wait.reset,
    )

    def run_server():
        server.run()

    threading.Thread(target=run_server).start()
    wait.wait()

    response_buf = response.SerializeToString()
    response_size = len(response_buf)
    sys.stdout.buffer.write(response_size.to_bytes(length=4, byteorder="big"))
    sys.stdout.buffer.write(response_buf)
    sys.stdout.buffer.flush()

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        # TODO: we're being killed with some other signal...
        server.stop()
        if tmpdir is not None:
            shutil.rmtree(tmpdir)


if __name__ == "__main__":
    main()
