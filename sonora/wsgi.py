import itertools
import json
import logging
import time
from collections import namedtuple
from http import HTTPStatus
from urllib.parse import quote

import grpc

from sonora import _events, protocol
from sonora._encoder import Codec, get_codec
from sonora.metadata import Metadata

_HandlerCallDetails = namedtuple(
    "_HandlerCallDetails", ("method", "invocation_metadata")
)


class grpcWSGI(grpc.Server):
    """
    WSGI Application Object that understands gRPC-Web.

    This is called by the WSGI server that's handling our actual HTTP
    connections. That means we can't use the normal gRPC I/O loop etc.
    """

    def __init__(self, application=None, enable_cors=True):
        self._application = application
        self._handlers = []
        self._enable_cors = enable_cors
        self._log = logging.getLogger(__name__)

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

    def _get_rpc_handler(self, environ):
        path = environ["PATH_INFO"]

        handler_call_details = _HandlerCallDetails(path, None)

        rpc_handler = None
        for handler in self._handlers:
            rpc_handler = handler.service(handler_call_details)
            if rpc_handler:
                return rpc_handler

        return None

    def _create_context(self, environ):
        try:
            timeout = protocol.parse_timeout(environ["HTTP_GRPC_TIMEOUT"])
        except KeyError:
            timeout = None
        if timeout is None and environ.get("HTTP_CONNECT_TIMEOUT_MS"):
            timeout = int(environ.get("HTTP_CONNECT_TIMEOUT_MS")) / 1000

        metadata = Metadata()
        for key, value in environ.items():
            if key.startswith("HTTP_"):
                header = key[5:].lower().replace("_", "-")
                metadata.add(header, value)
        metadata.add("content-type", environ.get("CONTENT_TYPE"))

        return ServicerContext(timeout, metadata)

    def _do_grpc_request(self, rpc_method, environ, start_response):
        context = self._create_context(environ)

        try:
            codec = get_codec(
                context.invocation_metadata(), rpc_method, enable_trailers=False
            )
        except protocol.InvalidContentType:
            start_response("415 Invalid Content Type", [])
            return

        stream = environ["wsgi.input"]
        request_proto_iterator = (
            codec.serializer.deserialize_request(
                codec.encoding.decode(compressed, bytes(message))
            )
            for _, compressed, message in codec.unwrap_message_stream(stream)
        )
        resp = None

        if not rpc_method.request_streaming and not rpc_method.response_streaming:
            method = rpc_method.unary_unary
        elif not rpc_method.request_streaming and rpc_method.response_streaming:
            method = rpc_method.unary_stream
        elif rpc_method.request_streaming and not rpc_method.response_streaming:
            method = rpc_method.stream_unary
        elif rpc_method.request_streaming and rpc_method.response_streaming:
            method = rpc_method.stream_stream
        else:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        if request_proto_iterator:
            try:
                if rpc_method.request_streaming:
                    resp = method(request_proto_iterator, context)
                else:
                    request_proto = next(request_proto_iterator, None)
                    if request_proto is None:
                        raise NotImplementedError()
                    # If more than one request is provided to a unary request,
                    # that is a protocol error.
                    if next(request_proto_iterator, None) is not None:
                        raise NotImplementedError()

                    resp = method(request_proto, context)

                if rpc_method.response_streaming:
                    if context.time_remaining() is not None:
                        resp = _timeout_generator(context, resp)
            except grpc.RpcError:
                # logging.exception("Exception handling RPC method")
                pass
            except NotImplementedError:
                # logging.exception("Exception handling RPC method")
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            except protocol.InvalidEncoding:
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            except protocol.ProtocolError:
                # logging.exception("Exception handling RPC method")
                context.set_code(grpc.StatusCode.INTERNAL)
            except Exception:
                logging.exception("Exception handling RPC method")
                context.set_code(grpc.StatusCode.INTERNAL)

        headers = [
            # ("Content-Type", codec.content_type),
        ]
        if self._enable_cors:
            headers.append(
                (
                    "Access-Control-Allow-Origin",
                    environ.get("HTTP_HOST") or environ["SERVER_NAME"],
                ),
            )
            headers.append(("Access-Control-Expose-Headers", "*"))

        if rpc_method.response_streaming:
            yield from self._do_streaming_response(
                codec,
                start_response,
                context,
                headers,
                resp,
            )
        else:
            yield from self._do_unary_response(
                codec,
                start_response,
                context,
                headers,
                resp,
            )

    def _do_streaming_response(
        self,
        codec: Codec,
        start_response,
        context,
        headers,
        resp,
    ):
        first_message = None
        if resp is not None:
            try:
                first_message = next(resp)
            except (grpc.RpcError, StopIteration):
                pass

        if context._initial_metadata:
            headers.extend(context._initial_metadata)

        start_response("200 OK", headers)

        if first_message is not None:
            yield codec.wrap_message(
                False, False, codec.serializer.serialize_response(first_message)
            )

        if resp is not None:
            try:
                for message in resp:
                    yield codec.wrap_message(
                        False, False, codec.serializer.serialize_response(message)
                    )
            except (grpc.RpcError, StopIteration):
                pass

        trailers = [("grpc-status", str(context.code.value[0]))]

        if context.details:
            trailers.append(("grpc-message", quote(context.details.encode("utf8"))))

        if context._trailing_metadata:
            trailers.extend(context._trailing_metadata)

        trailer_message = protocol.pack_trailers(trailers)

        if connect:
            if context.code != grpc.StatusCode.OK:
                code = context.code.name.lower()
                if code == "cancelled":
                    code = "canceled"
                error = {"code": code}
                if context.details:
                    error["message"] = context.details
            else:
                error = None

            trailer_dict = {}
            for name, value in trailers:
                if name.lower() == "grpc-status-details-bin":
                    # TODO: it's annoying to have to round trip this
                    from google.rpc import status_pb2

                    binvalue = protocol.b64decode(value)
                    status_details = status_pb2.Status()
                    status_details.ParseFromString(binvalue)
                    error["details"] = [
                        {
                            "type": d.type_url.rpartition("/")[2],
                            "value": protocol.b64encode(d.value),
                        }
                        for d in status_details.details
                    ]
                else:
                    trailer_dict.setdefault(name, []).append(value)

            trailer = {"metadata": trailer_dict}
            if error:
                trailer["error"] = error
            trailer_message = json.dumps(trailer).encode()
            trailer_data = wrap_message(True, False, trailer_message)
            yield trailer_data
        else:
            yield wrap_message(True, False, trailer_message)

    def _do_events(
        self,
        start_response,
        headers,
        events,
    ):
        for event in events:
            if isinstance(event, _events.StartResponse):
                start_response(
                    f"{event.status_code} {event.phrase}",
                    itertools.chain(headers, event.headers),
                )
            elif isinstance(event, _events.SendBody):
                yield event.body
            else:
                raise ValueError("Unexpected codec event")

    def _do_unary_response(
        self,
        codec: Codec,
        start_response,
        context: grpc.ServicerContext,
        headers,
        resp,
    ):
        # if context.code != grpc.StatusCode.OK and connect:
        #     yield from self._do_connect_error(
        #         start_response, context, headers, connect_stream, wrap_message
        #     )
        #     return

        # events = []
        # TODO: is a codec a context? or wraps one?
        codec.set_code(context.code)
        if context.details:
            codec.set_details(context.details)

        if context._initial_metadata:
            codec.set_initial_metadata(context._initial_metadata)
            # headers.extend(context._initial_metadata)

        # trailers = [("grpc-status", str(context.code.value[0]))]

        # if context.details:
        #     trailers.append(("grpc-message", quote(context.details)))

        if context._trailing_metadata:
            codec.set_trailing_metadata(context._trailing_metadata)

        if resp:
            yield from self._do_events(
                start_response, headers, codec.send_response(resp)
            )
        #     message_data = codec.wrap_message(False, False, codec.serializer.serialize_response(resp))
        # else:
        #     message_data = b""

        yield from self._do_events(start_response, headers, codec.end_response())

        # for event in events:
        #     if isinstance(event, _events.StartResponse):
        #         start_response(
        #             f"{event.status_code} {event.phrase}",
        #             itertools.chain(headers, event.headers),
        #         )
        #     elif isinstance(event, _events.SendBody):
        #         yield event.body
        #     else:
        #         raise ValueError("Unexpected codec event")

        # trailers.extend(context._trailing_metadata)

        # trailer_message = protocol.pack_trailers(trailers)
        # trailer_data = codec.wrap_message(True, False, trailer_message)

        # if connect:
        #     if connect_stream:
        #         trailer_dict = {}
        #         for name, value in trailers:
        #             trailer_dict.setdefault(name, []).append(value)
        #         trailer_message = json.dumps({"metadata": trailer_dict}).encode()
        #         trailer_data = protocol.wrap_message_connect(
        #             True, False, trailer_message
        #         )
        #     else:
        #         headers.extend((f"trailer-{name}", value) for name, value in trailers)

        # content_length = len(message_data) + (
        #     0 if (connect and not connect_stream) else len(trailer_data)
        # )

        # headers.append(("content-length", str(content_length)))

        # ew
        # if (
        #     context.code == grpc.StatusCode.UNKNOWN
        #     and context.details == b"Unsupported content-type"
        # ):
        #     start_response("415 Unsupported Media Type", headers)
        # else:
        #     start_response("200 OK", headers)

        # yield message_data

        # if not connect or connect_stream:
        #     yield trailer_data

    def _do_connect_error(
        self, start_response, context, headers, connect_stream, wrap_message
    ):
        if connect_stream:
            status = "200 OK"
        elif context.code == grpc.StatusCode.CANCELLED:
            status = "499 Canceled"
        else:
            http_status = HTTPStatus(protocol.status_code_to_http(context.code))
            status = f"{http_status.value} {http_status.phrase}"

        code = context.code.name.lower()
        if code == "cancelled":
            code = "canceled"
        error = {"code": code}
        if context.details:
            error["message"] = context.details

        # headers =
        if context._initial_metadata:
            headers.extend(context._initial_metadata)

        if not connect_stream and not context._started_response:
            # set correct content-type for unary errors
            headers = [
                (name, value)
                for name, value in headers
                if name.lower() != "content-type"
            ]
            headers.append(("content-type", "application/json"))

        trailers = {}
        if context._trailing_metadata:
            for name, value in context._trailing_metadata:
                if name.lower() == "grpc-status-details-bin":
                    # TODO: it's annoying to have to round trip this
                    from google.rpc import status_pb2

                    binvalue = protocol.b64decode(value)
                    status_details = status_pb2.Status()
                    status_details.ParseFromString(binvalue)
                    error["details"] = [
                        {
                            "type": d.type_url.rpartition("/")[2],
                            "value": protocol.b64encode(d.value),
                        }
                        for d in status_details.details
                    ]
                elif connect_stream:
                    trailers.setdefault(name, []).append(value)
                else:
                    headers.append((name, value))

        if context._started_response:
            error_body = wrap_message(
                True, False, json.dumps({"error": error, "metadata": trailers}).encode()
            )
            yield error_body
        else:
            if connect_stream:
                error_body = wrap_message(
                    True,
                    False,
                    json.dumps({"error": error, "metadata": trailers}).encode(),
                )
            else:
                error_body = json.dumps(error).encode()

            start_response(status, headers)
            yield error_body

    def _do_cors_preflight(self, environ, start_response):
        headers = [
            ("Content-Type", "text/plain"),
            ("Content-Length", "0"),
        ]
        if self._enable_cors:
            headers += [
                ("Access-Control-Allow-Methods", "POST, OPTIONS"),
                ("Access-Control-Allow-Headers", "*"),
                (
                    "Access-Control-Allow-Origin",
                    environ.get("HTTP_HOST") or environ["SERVER_NAME"],
                ),
                ("Access-Control-Allow-Credentials", "true"),
                ("Access-Control-Expose-Headers", "*"),
            ]
        start_response("204 No Content", headers)
        return []

    def __call__(self, environ, start_response):
        """
        Our actual WSGI request handler. Will execute the request
        if it matches a configured gRPC service path or fall through
        to the next application.
        """

        rpc_method = self._get_rpc_handler(environ)
        request_method = environ["REQUEST_METHOD"]

        if rpc_method:
            if request_method == "POST":
                return self._do_grpc_request(rpc_method, environ, start_response)
            elif request_method == "OPTIONS":
                return self._do_cors_preflight(environ, start_response)
            else:
                start_response("405 Method Not Allowed", [])
                return []

        if self._application:
            return self._application(environ, start_response)
        else:
            start_response("404 Not Found", [])
            return []

    def _read_request(self, environ):
        try:
            content_length = environ.get("CONTENT_LENGTH")
            if content_length:
                content_length = int(content_length)
            else:
                content_length = None
        except ValueError:
            content_length = None

        stream = environ["wsgi.input"]

        # Transfer encoding=chunked should be handled by the WSGI server
        # (Hop-by-hop header).
        return stream.read(content_length)


class ServicerContext(grpc.ServicerContext):
    def __init__(self, timeout=None, metadata=None):
        self.code = grpc.StatusCode.OK
        self.details = None

        self._timeout = timeout

        if timeout is not None:
            self._deadline = time.monotonic() + timeout
        else:
            self._deadline = None

        self._invocation_metadata = metadata or Metadata()
        self._initial_metadata = None
        self._trailing_metadata = None
        self._started_response = False

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

    def abort(self, code, details):
        if code == grpc.StatusCode.OK:
            raise ValueError()

        self.set_code(code)
        self.set_details(details)

        raise grpc.RpcError()

    def abort_with_status(self, status):
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

    def time_remaining(self):
        if self._deadline is not None:
            return max(self._deadline - time.monotonic(), 0)
        else:
            return None

    def invocation_metadata(self):
        return self._invocation_metadata

    def send_initial_metadata(self, initial_metadata):
        self._initial_metadata = protocol.encode_headers(initial_metadata)

    def set_trailing_metadata(self, trailing_metadata):
        self._trailing_metadata = Metadata(trailing_metadata)

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


def _timeout_generator(context, gen):
    while 1:
        if context.time_remaining() > 0:
            try:
                yield next(gen)
            except StopIteration:
                return
        else:
            context.code = grpc.StatusCode.DEADLINE_EXCEEDED
            context.details = "request timed out at the server"
            raise grpc.RpcError()
