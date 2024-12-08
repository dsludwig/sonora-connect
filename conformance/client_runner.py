#!/usr/bin/env python3

import base64
import collections
import concurrent.futures
import functools
import json
import logging
import re
import ssl
import struct
import sys
import threading
import time
import traceback
from typing import Any, List, Tuple

import grpc
from connectrpc.conformance.v1 import (
    client_compat_pb2,
    config_pb2,
    service_pb2,
    service_pb2_grpc,
)
from google.protobuf import json_format
from grpc_status import rpc_status

import sonora.aio
import sonora.client
import sonora.protocol

logger = logging.getLogger("conformance.runner")


def read_request() -> client_compat_pb2.ClientCompatRequest | None:
    data = sys.stdin.buffer.read(4)
    if not data:
        return
    if len(data) < 4:
        raise Exception("short read (header)")
    ll = struct.unpack(">I", data)[0]
    msg = client_compat_pb2.ClientCompatRequest()
    data = sys.stdin.buffer.read(ll)
    if len(data) < ll:
        raise Exception("short read (request)")
    msg.ParseFromString(data)
    return msg


def write_response(msg: client_compat_pb2.ClientCompatResponse) -> None:
    data = msg.SerializeToString()
    ll = struct.pack(">I", len(data))
    sys.stdout.buffer.write(ll)
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()


def camelcase_to_snakecase(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def log_message(request: Any, response: Any):
    with open("messages.log", "a") as fp:
        json.dump(
            {
                "case": request.test_name,
                "request": json_format.MessageToDict(request),
                "response": json_format.MessageToDict(response),
            },
            fp=fp,
        )
        fp.write("\n")


def to_pb_headers(headers: List[Tuple[str, str]]) -> list[service_pb2.Header]:
    h_dict: dict[str, list[str]] = collections.defaultdict(list)
    for key, value in headers:
        if key.endswith("-bin") and isinstance(value, bytes):
            h_dict[key].append(base64.b64encode(value))
        else:
            h_dict[key].append(value)

    try:
        return [
            service_pb2.Header(
                name=key,
                value=values,
            )
            for key, values in h_dict.items()
        ]
    except Exception:
        logger.error("bad headers: %r", h_dict)
        raise


def handle_message(
    msg: client_compat_pb2.ClientCompatRequest,
) -> client_compat_pb2.ClientCompatResponse:
    if msg.stream_type != config_pb2.STREAM_TYPE_UNARY:
        return client_compat_pb2.ClientCompatResponse(
            test_name=msg.test_name,
            error=client_compat_pb2.ClientErrorResult(
                message="TODO STREAM TYPE NOT IMPLEMENTED"
            ),
        )

    if msg.use_get_http_method:
        return client_compat_pb2.ClientCompatResponse(
            test_name=msg.test_name,
            error=client_compat_pb2.ClientErrorResult(
                message="TODO HTTP GET NOT IMPLEMENTED"
            ),
        )

    if len(msg.request_messages) > 1:
        return client_compat_pb2.ClientCompatResponse(
            test_name=msg.test_name,
            error=client_compat_pb2.ClientErrorResult(
                message="TODO MULTIPLE MESSAGES NOT IMPLEMENTED"
            ),
        )

    logger.debug(f"** {msg.test_name} **")
    # logger.debug(log_message(msg))
    any = msg.request_messages[0]
    logger.debug(f"{any.TypeName()=}")

    req_types = {
        "connectrpc.conformance.v1.UnaryRequest": service_pb2.UnaryRequest,
        "connectrpc.conformance.v1.UnimplementedRequest": service_pb2.UnimplementedRequest,
    }

    try:
        req_type = req_types[any.TypeName()]
    except KeyError:
        return client_compat_pb2.ClientCompatResponse(
            test_name=msg.test_name,
            error=client_compat_pb2.ClientErrorResult(
                message=f"TODO unknown message type: {any.TypeName()}"
            ),
        )

    req = req_type()
    any.Unpack(req)

    http1 = msg.http_version in [
        config_pb2.HTTP_VERSION_1,
        config_pb2.HTTP_VERSION_UNSPECIFIED,
    ]
    http2 = msg.http_version in [
        config_pb2.HTTP_VERSION_2,
        config_pb2.HTTP_VERSION_UNSPECIFIED,
    ]
    ssl_context = None
    if msg.server_tls_cert:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_verify_locations(cadata=msg.server_tls_cert.decode("utf8"))
        proto = "https"
    else:
        proto = "http"

    url = f"{proto}://{msg.host}:{msg.port}"

    if msg.request_delay_ms > 0:
        time.sleep(msg.request_delay_ms / 1000.0)

    if msg.protocol == config_pb2.PROTOCOL_GRPC:
        channel = grpc.secure_channel(
            f"{msg.host}:{msg.port}",
            credentials=grpc.ssl_channel_credentials(
                root_certificates=msg.server_tls_cert
            ),
        )
    elif msg.protocol == config_pb2.PROTOCOL_GRPC_WEB:
        channel = sonora.client.insecure_web_channel(url)
    else:
        return client_compat_pb2.ClientCompatResponse(
            test_name=msg.test_name,
            error=client_compat_pb2.ClientErrorResult(
                message=f"TODO unknown message type: {any.TypeName()}"
            ),
        )

    with channel:
        try:
            client = service_pb2_grpc.ConformanceServiceStub(channel)
            resp, call = getattr(client, msg.method).with_call(
                req,
                timeout=msg.timeout_ms / 1000 if msg.timeout_ms else None,
                metadata=[
                    (h.name.lower(), value)
                    for h in msg.request_headers
                    for value in h.value
                ],
            )

            return client_compat_pb2.ClientCompatResponse(
                test_name=msg.test_name,
                response=client_compat_pb2.ClientResponseResult(
                    payloads=[resp.payload],
                    http_status_code=200,
                    response_headers=to_pb_headers(call.initial_metadata()),
                    response_trailers=to_pb_headers(call.trailing_metadata()),
                ),
            )
        except grpc.RpcError as e:
            status = rpc_status.from_call(e)
            return client_compat_pb2.ClientCompatResponse(
                test_name=msg.test_name,
                response=client_compat_pb2.ClientResponseResult(
                    error=service_pb2.Error(
                        code=getattr(
                            config_pb2,
                            f"CODE_{e.code().name.upper().replace('CANCELLED','CANCELED')}",
                        ),
                        message=e.details(),
                        details=status.details if status is not None else None,
                    ),
                    http_status_code=200,
                    response_headers=to_pb_headers(e.initial_metadata()),
                    response_trailers=to_pb_headers(e.trailing_metadata()),
                ),
            )
        except sonora.protocol.WebRpcError as e:
            # status = rpc_status.from_call(e)
            return client_compat_pb2.ClientCompatResponse(
                test_name=msg.test_name,
                response=client_compat_pb2.ClientResponseResult(
                    error=service_pb2.Error(
                        code=getattr(config_pb2, f"CODE_{e.code().name.upper()}"),
                        message=e.details(),
                        # details=e.details(),
                    ),
                    http_status_code=e.http_status_code(),
                    # response_headers=to_pb_headers(e.headers),
                    # response_trailers=to_pb_headers(e.trailers),
                ),
            )
        except Exception as e:
            return client_compat_pb2.ClientCompatResponse(
                test_name=msg.test_name,
                error=client_compat_pb2.ClientErrorResult(
                    message="\n".join(traceback.format_exception(e))
                ),
            )


def main():
    if "--debug" in sys.argv:
        logging.basicConfig(level=logging.DEBUG)

    output_lock = threading.Lock()

    def handle_done_message(req, fut):
        try:
            resp = fut.result()
        except Exception as e:
            resp = client_compat_pb2.ClientCompatResponse(
                test_name=req.test_name,
                error=client_compat_pb2.ClientErrorResult(
                    message="\n".join(traceback.format_exception(e))
                ),
            )

        with output_lock:
            log_message(req, resp)
            logger.info("Finishing request: %s", req.test_name)
            write_response(resp)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        while req := read_request():
            logger.info("Enqueuing request: %s", req.test_name)
            executor.submit(handle_message, req).add_done_callback(
                functools.partial(handle_done_message, req)
            )
    logger.info("All done")


if __name__ == "__main__":
    main()
