#!/usr/bin/env python
import asyncio
import logging
import pathlib
import shutil
import socket
import sys
import tempfile
import threading
import time

import asgiref.wsgi
import hypercorn
import hypercorn.asyncio
import hypercorn.run
from asgi import asgi_app
from connectrpc.conformance.v1.server_compat_pb2 import (
    ServerCompatRequest,
    ServerCompatResponse,
)
from wsgi import wsgi_app


def get_open_port() -> int:
    with socket.create_server(("", 0)) as server:
        return server.getsockname()[1]


def write_response(response):
    response_buf = response.SerializeToString()
    response_size = len(response_buf)
    sys.stdout.buffer.write(response_size.to_bytes(length=4, byteorder="big"))
    sys.stdout.buffer.write(response_buf)
    sys.stdout.buffer.flush()


def start_server(request, app):
    port = get_open_port()
    response = ServerCompatResponse(host="localhost", port=port)

    tmpdir = None
    cfg = hypercorn.Config()
    cfg.bind = [f"localhost:{port}"]
    if request.use_tls:
        tmpdir = pathlib.Path(tempfile.mkdtemp())
        server_creds = request.server_creds
        keyfile = tmpdir / "key.pem"
        certfile = tmpdir / "cert.pem"
        keyfile.write_bytes(server_creds.key)
        certfile.write_bytes(server_creds.cert)
        response.pem_cert = server_creds.cert
        cfg.certfile = str(certfile)
        cfg.keyfile = str(keyfile)

    def notify_caller():
        time.sleep(0.1)
        write_response(response)

    threading.Thread(target=notify_caller).start()

    try:
        asyncio.run(hypercorn.asyncio.serve(app, cfg))
    except KeyboardInterrupt:
        if tmpdir is not None:
            shutil.rmtree(tmpdir)


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

    if "--wsgi" in sys.argv:
        start_server(request, asgiref.wsgi.WsgiToAsgi(wsgi_app))
    else:
        start_server(request, asgi_app)


if __name__ == "__main__":
    main()
