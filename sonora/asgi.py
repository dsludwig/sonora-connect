import asyncio
import itertools
import logging
import time
from collections import namedtuple
from collections.abc import AsyncIterator

import grpc
from async_timeout import timeout

from sonora import _events, protocol
from sonora._codec import Codec, get_codec
from sonora.metadata import Metadata

_HandlerCallDetails = namedtuple(
    "_HandlerCallDetails", ("method", "invocation_metadata")
)


class grpcASGI(grpc.Server):
    def __init__(self, application=None, enable_cors=True):
        self._application = application
        self._handlers = []
        self._enable_cors = enable_cors
        self._log = logging.getLogger(__name__)

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
                trailers_supported = (
                    scope.get("extensions", {}).get("http.response.trailers")
                    is not None
                )
                try:
                    codec = get_codec(
                        context.invocation_metadata(),
                        rpc_method,
                        enable_trailers=trailers_supported,
                    )
                except protocol.InvalidContentType:
                    # If Content-Type does not begin with "application/grpc", gRPC servers
                    # SHOULD respond with HTTP status of 415 (Unsupported Media Type). This
                    # will prevent other HTTP/2 clients from interpreting a gRPC error
                    # response, which uses status 200 (OK), as successful.
                    return await send({"type": "http.response.start", "status": 415})

                try:
                    async with timeout(context.time_remaining()):
                        await self._do_grpc_request(
                            rpc_method, context, receive, send, codec
                        )
                except asyncio.TimeoutError:
                    context.code = grpc.StatusCode.DEADLINE_EXCEEDED
                    context.details = "request timed out at the server"
                    await self._do_grpc_error(send, codec, context)

            elif self._enable_cors and request_method == "OPTIONS":
                await self._do_cors_preflight(scope, receive, send)
            else:
                await send({"type": "http.response.start", "status": 405})
                await send(
                    {"type": "http.response.body", "body": b"", "more_body": False}
                )

        elif self._application:
            await self._application(scope, receive, send)

        else:
            await send({"type": "http.response.start", "status": 404})
            await send({"type": "http.response.body", "body": b"", "more_body": False})

    async def _do_events(
        self,
        headers,
        send,
        events,
    ):
        for event in events:
            if isinstance(event, _events.StartResponse):
                await send(
                    {
                        "type": "http.response.start",
                        "status": event.status_code,
                        "headers": itertools.chain(
                            headers,
                            (
                                (k.encode("ascii"), v.encode("utf-8"))
                                for (k, v) in event.headers
                            ),
                        ),
                        "trailers": event.trailers,
                    }
                )
            elif isinstance(event, _events.SendBody):
                await send(
                    {
                        "type": "http.response.body",
                        "body": event.body,
                        "more_body": event.more_body,
                    }
                )
            elif isinstance(event, _events.SendTrailers):
                await send(
                    {
                        "type": "http.response.trailers",
                        "headers": (
                            (k.encode("ascii"), v.encode("utf-8"))
                            for (k, v) in event.trailers
                        ),
                        "more_trailers": False,
                    }
                )
            else:
                raise ValueError("Unexpected codec event")

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
        metadata = Metadata()

        for header, value in scope["headers"]:
            if timeout is None and header == b"grpc-timeout":
                timeout = protocol.parse_timeout(value)
            elif timeout is None and header == b"connect-timeout-ms":
                timeout = int(value) / 1000
            else:
                if header.endswith(b"-bin"):
                    value = protocol.b64decode(value)
                else:
                    value = value.decode("ascii")

                metadata.add(header.decode("ascii"), value)

        return ServicerContext(
            timeout,
            metadata,
            enable_cors=self._enable_cors,
        )

    async def _receive_body(self, codec: Codec, receive):
        more_body = True
        while more_body:
            event = await receive()
            assert event["type"].startswith("http.")
            chunk = event["body"]
            more_body = event.get("more_body")
            for e in codec.server_receive_body(chunk, more_body):
                if isinstance(e, _events.ReceiveMessage):
                    yield e.message

    async def _do_grpc_request(self, rpc_method, context, receive, send, codec: Codec):
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

        request_proto_iterator = self._receive_body(codec, receive)

        coroutine = None
        try:
            if rpc_method.request_streaming:
                coroutine = method(request_proto_iterator, context)
            else:
                request_proto = await anext(request_proto_iterator, None)
                if request_proto is None:
                    raise NotImplementedError()
                # If more than one request is provided to a unary request,
                # that is a protocol error.
                if await anext(request_proto_iterator, None) is not None:
                    raise NotImplementedError()
                coroutine = method(request_proto, context)
        except NotImplementedError:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        except protocol.InvalidEncoding:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        except protocol.ProtocolError:
            context.set_code(grpc.StatusCode.INTERNAL)

        try:
            if rpc_method.response_streaming:
                await self._do_streaming_response(
                    rpc_method, receive, send, codec, context, coroutine
                )
            else:
                await self._do_unary_response(
                    rpc_method, receive, send, codec, context, coroutine
                )
        except grpc.RpcError:
            await self._do_grpc_error(send, codec, context)
        except protocol.InvalidEncoding:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            await self._do_grpc_error(send, codec, context)
        except protocol.ProtocolError:
            context.set_code(grpc.StatusCode.INTERNAL)
            await self._do_grpc_error(send, codec, context)
        except Exception:
            self._log.exception("Error in RPC handler")
            context.set_code(grpc.StatusCode.INTERNAL)
            await self._do_grpc_error(send, codec, context)

    async def _do_streaming_response(
        self, rpc_method, receive, send, codec: Codec, context, coroutine
    ):
        headers = context._response_headers

        if coroutine is None:
            message = None
        else:
            try:
                message = await anext(coroutine, default=None)
            except grpc.RpcError:
                message = None

        if context._initial_metadata:
            codec.set_initial_metadata(context._initial_metadata)

        if message:
            await self._do_events(headers, send, codec.send_response(message))

            async for message in coroutine:
                send_task = asyncio.create_task(
                    self._do_events(headers, send, codec.send_response(message))
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

        codec.set_code(context.code)
        if context.details:
            codec.set_details(context.details)

        if context._trailing_metadata:
            codec.set_trailing_metadata(context._trailing_metadata)

        await self._do_events(headers, send, codec.end_response())

    async def _do_unary_response(
        self, rpc_method, receive, send, codec: Codec, context, coroutine
    ):
        headers = context._response_headers

        if coroutine is None:
            message = None

        else:
            try:
                message = await coroutine
            except grpc.RpcError:
                message = None

        codec.set_code(context.code)
        if context.details:
            codec.set_details(context.details)

        if context._initial_metadata:
            codec.set_initial_metadata(context._initial_metadata)

        if context._trailing_metadata:
            codec.set_trailing_metadata(context._trailing_metadata)

        if message:
            await self._do_events(headers, send, codec.send_response(message))

        await self._do_events(headers, send, codec.end_response())

    async def _do_grpc_error(self, send, codec: Codec, context):
        headers = context._response_headers

        codec.set_code(context.code)
        codec.set_details(context.details)
        if context._initial_metadata:
            codec.set_initial_metadata(context._initial_metadata)

        if context._trailing_metadata:
            codec.set_trailing_metadata(context._trailing_metadata)

        return await self._do_events(headers, send, codec.end_response())

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
    def __init__(self, timeout=None, metadata=None, enable_cors=True):
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

        origin = None

        for header, value in metadata:
            if header == "host":
                origin = value

        if not origin:
            raise ValueError("Request is missing the host header")

        self._response_headers = []

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
            self.set_trailing_metadata(Metadata(status.trailing_metadata))
        else:
            self._trailing_metadata.extend(status.trailing_metadata)

        raise grpc.RpcError()

    async def send_initial_metadata(self, initial_metadata):
        self._initial_metadata = Metadata(initial_metadata)

    def set_trailing_metadata(self, trailing_metadata):
        self._trailing_metadata = Metadata(trailing_metadata)

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
