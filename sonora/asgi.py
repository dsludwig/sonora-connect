import asyncio
import base64
import time
from collections import namedtuple
from collections.abc import AsyncIterator
from urllib.parse import quote

import grpc
from async_timeout import timeout

from sonora import protocol

_HandlerCallDetails = namedtuple(
    "_HandlerCallDetails", ("method", "invocation_metadata")
)


class grpcASGI(grpc.Server):
    def __init__(self, application=None, enable_cors=True):
        self._application = application
        self._handlers = []
        self._enable_cors = enable_cors

    async def __call__(self, scope, receive, send):
        """
        Our actual ASGI request handler. Will execute the request
        if it matches a configured gRPC service path or fall through
        to the next application.
        """
        if not scope["type"] == "http":
            if self._application:
                return await self._application(scope, receive, send)
            else:
                return

        rpc_method = self._get_rpc_handler(scope["path"])
        request_method = scope["method"]

        if rpc_method:
            if request_method == "POST":
                context = self._create_context(scope)
                if context.code != grpc.StatusCode.OK:
                    return await self._do_grpc_error(send, context)

                try:
                    async with timeout(context.time_remaining()):
                        await self._do_grpc_request(rpc_method, context, receive, send)
                except asyncio.TimeoutError:
                    context.code = grpc.StatusCode.DEADLINE_EXCEEDED
                    context.details = "request timed out at the server"
                    await self._do_grpc_error(send, context)

            elif self._enable_cors and request_method == "OPTIONS":
                await self._do_cors_preflight(scope, receive, send)
            else:
                await send({"type": "http.response.start", "status": 400})
                await send(
                    {"type": "http.response.body", "body": b"", "more_body": False}
                )

        elif self._application:
            await self._application(scope, receive, send)

        else:
            await send({"type": "http.response.start", "status": 404})
            await send({"type": "http.response.body", "body": b"", "more_body": False})

    def _get_rpc_handler(self, path):
        handler_call_details = _HandlerCallDetails(path, None)

        rpc_handler = None
        for handler in self._handlers:
            rpc_handler = handler.service(handler_call_details)
            if rpc_handler:
                return rpc_handler

        return None

    def _create_context(self, scope):
        timeout = None
        metadata = []

        trailers_supported = (
            scope.get("extensions", {}).get("http.response.trailers") is not None
        )

        for header, value in scope["headers"]:
            if timeout is None and header == b"grpc-timeout":
                timeout = protocol.parse_timeout(value)
            else:
                if header.endswith(b"-bin"):
                    value = base64.b64decode(value)
                else:
                    value = value.decode("ascii")

                metadata.append((header.decode("ascii"), value))

        return ServicerContext(
            timeout,
            metadata,
            enable_cors=self._enable_cors,
            enable_trailers=trailers_supported,
        )

    async def _do_grpc_request(self, rpc_method, context, receive, send):
        headers = context._response_headers
        wrap_message = context._wrap_message
        unwrap_message = context._unwrap_message
        make_deserializer = context._make_deserializer

        if not rpc_method.request_streaming and not rpc_method.response_streaming:
            method = rpc_method.unary_unary
        elif not rpc_method.request_streaming and rpc_method.response_streaming:
            method = rpc_method.unary_stream
        elif rpc_method.request_streaming and not rpc_method.response_streaming:
            method = rpc_method.stream_unary
        elif rpc_method.request_streaming and rpc_method.response_streaming:
            method = rpc_method.stream_stream
        else:
            raise NotImplementedError

        deserializer = make_deserializer(rpc_method.request_deserializer)
        request_proto_iterator = (
            deserializer(bytes(message))
            async for _, _, message in unwrap_message(receive)
        )

        try:
            if rpc_method.request_streaming:
                coroutine = method(request_proto_iterator, context)
            else:
                request_proto = await anext(request_proto_iterator, None)
                if request_proto is None:
                    raise NotImplementedError()
                coroutine = method(request_proto, context)
        except NotImplementedError:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            coroutine = None

        try:
            if rpc_method.response_streaming:
                await self._do_streaming_response(
                    rpc_method, receive, send, wrap_message, context, coroutine
                )
            else:
                await self._do_unary_response(
                    rpc_method, receive, send, wrap_message, context, coroutine
                )
        except grpc.RpcError:
            await self._do_grpc_error(send, context)

    async def _do_streaming_response(
        self, rpc_method, receive, send, wrap_message, context, coroutine
    ):
        headers = context._response_headers

        if coroutine:
            message = await anext(coroutine, default=None)
        else:
            message = None

        status = 200

        if message is None:
            body = None
        else:
            body = wrap_message(False, False, rpc_method.response_serializer(message))

        if context._initial_metadata:
            headers.extend(context._initial_metadata)

        await send(
            {"type": "http.response.start", "status": status, "headers": headers}
        )
        context._started_response = True

        if body is not None:
            await send({"type": "http.response.body", "body": body, "more_body": True})

            async for message in coroutine:
                body = wrap_message(
                    False, False, rpc_method.response_serializer(message)
                )

                send_task = asyncio.create_task(
                    send(
                        {"type": "http.response.body", "body": body, "more_body": True}
                    )
                )

                recv_task = asyncio.create_task(receive())

                done, pending = await asyncio.wait(
                    {send_task, recv_task}, return_when=asyncio.FIRST_COMPLETED
                )

                if recv_task in done:
                    send_task.cancel()
                    result = recv_task.result()
                    if result["type"] == "http.disconnect":
                        break
                else:
                    recv_task.cancel()

        trailers = [("grpc-status", str(context.code.value[0]))]

        if context.details:
            trailers.append(("grpc-message", quote(context.details)))

        if context._trailing_metadata:
            trailers.extend(context._trailing_metadata)

        if context._enable_trailers:
            await send(
                {
                    "type": "http.response.trailers",
                    "headers": tuple(
                        (name.encode("ascii"), value.encode("utf-8"))
                        for name, value in trailers
                    ),
                    "more_trailers": False,
                }
            )
        else:
            trailer_message = protocol.pack_trailers(trailers)
            trailer_data = wrap_message(True, False, trailer_message)
            await send(
                {"type": "http.response.body", "body": trailer_data, "more_body": False}
            )

    async def _do_unary_response(
        self, rpc_method, receive, send, wrap_message, context, coroutine
    ):
        make_serializer = context._make_serializer
        headers = context._response_headers

        if coroutine is None:
            message = None
        else:
            message = await coroutine

        status = 200

        if context._initial_metadata:
            headers.extend(context._initial_metadata)

        serializer = make_serializer(rpc_method.response_serializer)
        if message is not None:
            message_data = wrap_message(False, False, serializer(message))
        else:
            message_data = b""

        trailers = [("grpc-status", str(context.code.value[0]))]

        if context.details:
            trailers.append(("grpc-message", quote(context.details)))

        if context._trailing_metadata:
            trailers.extend(context._trailing_metadata)

        trailer_message = protocol.pack_trailers(trailers)
        trailer_data = wrap_message(True, False, trailer_message)
        trailers_enabled = context._enable_trailers

        content_length = len(message_data) + (
            0 if trailers_enabled else len(trailer_data)
        )

        headers.append((b"content-length", str(content_length).encode()))
        if trailers_enabled:
            headers.append((b"trailers", b"grpc-status"))

        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": headers,
                "trailers": trailers_enabled,
            }
        )
        context._started_response = True

        await send(
            {
                "type": "http.response.body",
                "body": message_data,
                "more_body": not trailers_enabled,
            }
        )

        if trailers_enabled:
            await send(
                {
                    "type": "http.response.trailers",
                    "headers": tuple(
                        (name.encode("ascii"), value.encode("utf-8"))
                        for name, value in trailers
                    ),
                    "more_trailers": False,
                }
            )
        else:
            await send(
                {"type": "http.response.body", "body": trailer_data, "more_body": False}
            )

    async def _do_grpc_error(self, send, context):
        wrap_message = context._wrap_message
        status = context._status_code
        headers = context._response_headers
        trailers_enabled = context._enable_trailers
        headers.append((b"grpc-status", str(context.code.value[0]).encode()))

        if context.details:
            headers.append(
                (b"grpc-message", quote(context.details.encode("utf8")).encode("ascii"))
            )

        if context._initial_metadata and not context._started_response:
            headers.extend(context._initial_metadata)

        if context._trailing_metadata:
            headers.extend(
                (name.encode("ascii"), value.encode("utf-8"))
                for name, value in context._trailing_metadata
            )

        if context._started_response:
            # If the response has already started, we must transmit the error as a
            # trailer.
            if trailers_enabled:
                await send(
                    {
                        "type": "http.response.trailers",
                        "headers": headers,
                        "more_trailers": False,
                    }
                )
            else:
                # TODO: mixed bytes and strings all over the place
                # trailer_message = protocol.pack_trailers(headers)
                data = bytearray()
                for k, v in headers:
                    k = k.lower()
                    data.extend(k)
                    data.extend(b": ")
                    data.extend(v)
                    data.extend(b"\r\n")
                trailer_message = bytes(data)
                trailer_data = wrap_message(True, False, trailer_message)
                await send(
                    {
                        "type": "http.response.body",
                        "body": trailer_data,
                        "more_body": False,
                    }
                )
        else:
            await send(
                {"type": "http.response.start", "status": status, "headers": headers}
            )
            await send({"type": "http.response.body", "body": b"", "more_body": False})

    async def _do_cors_preflight(self, scope, receive, send):
        origin = next(
            (value for header, value in scope["headers"] if header == "host"),
            scope["server"][0],
        )

        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"Content-Type", b"text/plain"),
                    (b"Content-Length", b"0"),
                    (b"Access-Control-Allow-Methods", b"POST, OPTIONS"),
                    (b"Access-Control-Allow-Headers", b"*"),
                    (b"Access-Control-Allow-Origin", origin),
                    (b"Access-Control-Allow-Credentials", b"true"),
                    (b"Access-Control-Expose-Headers", b"*"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    def add_generic_rpc_handlers(self, handlers):
        self._handlers.extend(handlers)

    def add_insecure_port(self, port):
        raise NotImplementedError()

    def add_secure_port(self, port):
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


class ServicerContext(grpc.ServicerContext):
    def __init__(
        self, timeout=None, metadata=None, enable_cors=True, enable_trailers=False
    ):
        self.code = grpc.StatusCode.OK
        self.details = None

        self._timeout = timeout
        self._status_code = 200

        if timeout is not None:
            self._deadline = time.monotonic() + timeout
        else:
            self._deadline = None

        self._invocation_metadata = metadata or tuple()
        self._initial_metadata = None
        self._trailing_metadata = None

        response_content_type = "application/grpc-web+proto"

        self._enable_trailers = False
        self._wrap_message = protocol.wrap_message
        self._unwrap_message = protocol.unwrap_message_asgi
        self._make_serializer = lambda i: i
        self._make_deserializer = lambda i: i
        origin = None

        for header, value in metadata:
            if header == "content-type":
                if value == "application/grpc-web-text":
                    self._wrap_message = protocol.b64_wrap_message
                    self._unwrap_message = protocol.b64_unwrap_message_asgi
                elif value == "application/grpc-web+json":
                    self._make_serializer = protocol.serialize_json
                    self._make_deserializer = protocol.deserialize_json
                    response_content_type = "application/grpc-web+json"
                elif value in (
                    "application/grpc-web",
                    "application/grpc-web+proto",
                ):
                    pass
                elif value == "application/grpc+json" and enable_trailers:
                    self._make_serializer = protocol.serialize_json
                    self._make_deserializer = protocol.deserialize_json
                    self._enable_trailers = True
                    response_content_type = "application/grpc+json"
                elif (
                    value
                    in (
                        "application/grpc",
                        "application/grpc+proto",
                    )
                    and enable_trailers
                ):
                    response_content_type = "application/grpc+proto"
                    self._enable_trailers = True
                else:
                    self.code = grpc.StatusCode.UNKNOWN
                    self.details = "Unsupported content-type"
                    # If Content-Type does not begin with "application/grpc", gRPC servers
                    # SHOULD respond with HTTP status of 415 (Unsupported Media Type). This
                    # will prevent other HTTP/2 clients from interpreting a gRPC error
                    # response, which uses status 200 (OK), as successful.
                    self._status_code = 415
            elif header == "accept":
                response_content_type = value.split(",")[0].strip()
            elif header == "host":
                origin = value
            elif header == "grpc-encoding":
                if value.lower() != "identity":
                    self.code = grpc.StatusCode.UNIMPLEMENTED
                    self.details = "Unsupported encoding"

        self._started_response = False

        if not origin:
            raise ValueError("Request is missing the host header")

        self._response_headers = [
            (b"Content-Type", response_content_type.encode("ascii")),
        ]

        if enable_cors:
            self._response_headers += [
                (b"Access-Control-Allow-Origin", origin.encode("ascii")),
                (b"Access-Control-Expose-Headers", b"*"),
            ]

    def set_code(self, code):
        if isinstance(code, grpc.StatusCode):
            self.code = code

        elif isinstance(code, int):
            for status_code in grpc.StatusCode:
                if status_code.value[0] == code:
                    self.code = status_code
                    break
            else:
                raise ValueError(f"Unknown StatusCode: {code}")
        else:
            raise NotImplementedError(
                f"Unsupported status code type: {type(code)} with value {code}"
            )

    def set_details(self, details):
        self.details = details

    async def abort(self, code, details):
        if code == grpc.StatusCode.OK:
            raise ValueError()

        self.set_code(code)
        self.set_details(details)

        raise grpc.RpcError()

    async def abort_with_status(self, status: grpc.Status):
        if status.code == grpc.StatusCode.OK:
            self.set_code(grpc.StatusCode.UNKNOWN)
            raise grpc.RpcError()

        self.set_code(status.code)
        self.set_details(status.details)
        if self._trailing_metadata is None:
            self.set_trailing_metadata(status.trailing_metadata)
        else:
            self.set_trailing_metadata(
                tuple(self._trailing_metadata) + tuple(status.trailing_metadata)
            )

        raise grpc.RpcError()

    async def send_initial_metadata(self, initial_metadata):
        self._initial_metadata = [
            (key.encode("ascii"), value.encode("utf8"))
            for key, value in protocol.encode_headers(initial_metadata)
        ]

    def set_trailing_metadata(self, trailing_metadata):
        self._trailing_metadata = protocol.encode_headers(trailing_metadata)

    def invocation_metadata(self):
        return self._invocation_metadata

    def time_remaining(self):
        if self._deadline is not None:
            return max(self._deadline - time.monotonic(), 0)
        else:
            return None

    def peer(self):
        raise NotImplementedError()

    def peer_identities(self):
        raise NotImplementedError()

    def peer_identity_key(self):
        raise NotImplementedError()

    def auth_context(self):
        raise NotImplementedError()

    def add_callback(self):
        raise NotImplementedError()

    def cancel(self):
        raise NotImplementedError()

    def is_active(self):
        raise NotImplementedError()


# Copied from https://github.com/python/cpython/pull/8895


_NOT_PROVIDED = object()


async def anext(async_iterator, default=_NOT_PROVIDED):
    """anext(async_iterator[, default])
    Return the next item from the async iterator.
    If default is given and the iterator is exhausted,
    it is returned instead of raising StopAsyncIteration.
    """
    if not isinstance(async_iterator, AsyncIterator):
        raise TypeError(f"anext expected an AsyncIterator, got {type(async_iterator)}")
    anxt = async_iterator.__anext__
    try:
        return await anxt()
    except StopAsyncIteration:
        if default is _NOT_PROVIDED:
            raise
        return default
