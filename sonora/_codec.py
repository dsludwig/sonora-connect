import abc
import itertools
import json
import struct
import typing
from http import HTTPStatus
from urllib.parse import quote

import grpc
from google.protobuf.message import Message
from google.rpc import status_pb2

from sonora import protocol
from sonora._encoding import Encoding, get_encoding
from sonora._events import (
    ClientEvents,
    ReceiveMessage,
    SendBody,
    SendTrailers,
    ServerEvents,
    StartRequest,
    StartResponse,
)
from sonora.metadata import Metadata

Deserializer = typing.Callable[[bytes], Message]
Serializer = typing.Callable[[Message], bytes]


class RpcMethodHandler(typing.NamedTuple):
    request_streaming: bool
    response_streaming: bool
    request_deserializer: Deserializer | None
    response_serializer: Serializer | None


class RpcOperation(typing.NamedTuple):
    request_streaming: bool
    response_streaming: bool
    request_serializer: Serializer | None
    response_deserializer: Deserializer | None


def _transform(
    serializer: Serializer | Deserializer | None, message: typing.Any
) -> typing.Any:
    if serializer is None:
        return message
    return serializer(message)


class Serializer:
    @abc.abstractmethod
    def serialize_request(self, request: Message) -> bytes: ...
    @abc.abstractmethod
    def deserialize_response(self, response: bytes) -> Message: ...
    @abc.abstractmethod
    def serialize_response(self, response: Message) -> bytes: ...
    @abc.abstractmethod
    def deserialize_request(self, request: bytes) -> Message: ...


class ProtoSerializer(Serializer):
    def __init__(
        self,
        request_serializer: Serializer | None = None,
        response_deserializer: Deserializer | None = None,
        request_deserializer: Deserializer | None = None,
        response_serializer: Serializer | None = None,
    ):
        self.request_serializer = request_serializer
        self.response_deserializer = response_deserializer
        self.request_deserializer = request_deserializer
        self.response_serializer = response_serializer

    def serialize_request(self, request: Message) -> bytes:
        return _transform(self.request_serializer, request)

    def deserialize_response(self, response: bytes) -> Message:
        return _transform(self.response_deserializer, response)

    def serialize_response(self, response: Message) -> bytes:
        return _transform(self.response_serializer, response)

    def deserialize_request(self, request: bytes) -> Message:
        return _transform(self.request_deserializer, request)


class JsonSerializer(ProtoSerializer):
    def __init__(
        self,
        request_serializer: Serializer | None = None,
        response_deserializer: Deserializer | None = None,
        request_deserializer: Deserializer | None = None,
        response_serializer: Serializer | None = None,
    ):
        super().__init__(
            protocol.serialize_json(request_serializer) if request_serializer else None,
            protocol.deserialize_json(response_deserializer)
            if response_deserializer
            else None,
            protocol.deserialize_json(request_deserializer)
            if request_deserializer
            else None,
            protocol.serialize_json(response_serializer)
            if response_serializer
            else None,
        )


class Codec:
    def __init__(self, encoding: Encoding, serializer: Serializer):
        self.encoding = encoding
        self.serializer = serializer
        self._initial_metadata = Metadata()
        self._trailing_metadata = Metadata()
        self._invocation_metadata = Metadata()
        self._timeout = None
        self._code = grpc.StatusCode.OK
        self._details = None

    @property
    @abc.abstractmethod
    def content_type(self) -> str: ...
    @property
    @abc.abstractmethod
    def requires_trailers(self) -> bool: ...

    @abc.abstractmethod
    def unpack_header_flags(self, flags: int) -> tuple[bool, bool]: ...
    @abc.abstractmethod
    def pack_header_flags(self, trailers: bool, compressed: bool) -> int: ...

    def set_code(self, code: grpc.StatusCode):
        self._code = code

    def set_details(self, details: str):
        self._details = details

    def set_initial_metadata(self, metadata: Metadata):
        self._initial_metadata = metadata

    def set_trailing_metadata(self, metadata: Metadata):
        self._trailing_metadata = metadata

    def set_invocation_metadata(self, metadata: Metadata):
        self._invocation_metadata = metadata

    def set_timeout(self, timeout: float):
        self._timeout = timeout

    @abc.abstractmethod
    def send_request(self, request: Message) -> ClientEvents: ...

    @abc.abstractmethod
    def start_request(self) -> ClientEvents: ...

    @abc.abstractmethod
    def end_request(self) -> ClientEvents: ...

    @abc.abstractmethod
    def start_response(self, response: StartResponse) -> ClientEvents: ...

    @abc.abstractmethod
    def receive_body(self, body: bytes) -> ClientEvents: ...

    @abc.abstractmethod
    def send_response(self, response: Message) -> ServerEvents: ...

    @abc.abstractmethod
    def end_response(self) -> ServerEvents: ...

    def wrap_message(self, trailers: bool, compressed: bool, message: bytes) -> bytes:
        return (
            struct.pack(
                protocol._HEADER_FORMAT,
                self.pack_header_flags(trailers, compressed),
                len(message),
            )
            + message
        )

    def unwrap_message(self, message: bytes) -> tuple[bool, bool, bytes, bytes]:
        if len(message) < protocol._HEADER_LENGTH:
            raise ValueError()
        flags, length = struct.unpack(
            protocol._HEADER_FORMAT, message[: protocol._HEADER_LENGTH]
        )
        data = message[protocol._HEADER_LENGTH : protocol._HEADER_LENGTH + length]

        if length != len(data):
            raise ValueError()

        trailers, compressed = self.unpack_header_flags(flags)
        return trailers, compressed, data, message[protocol._HEADER_LENGTH + length :]

    def unwrap_message_stream(
        self, stream
    ) -> typing.Iterable[tuple[bool, bool, bytes]]:
        data = stream.read(protocol._HEADER_LENGTH)

        while data:
            flags, length = struct.unpack(protocol._HEADER_FORMAT, data)
            trailers, compressed = self.unpack_header_flags(flags)

            body = stream.read(length)
            yield trailers, compressed, body

            if trailers:
                break

            data = stream.read(protocol._HEADER_LENGTH)

    async def unwrap_message_stream_async(
        self, stream
    ) -> typing.AsyncIterable[tuple[bool, bool, bytes]]:
        data = await stream.readexactly(protocol._HEADER_LENGTH)

        while data:
            flags, length = struct.unpack(protocol._HEADER_FORMAT, data)
            trailers, compressed = self.unpack_header_flags(flags)

            body = await stream.readexactly(length)
            yield trailers, compressed, body

            if trailers:
                break

            data = await stream.readexactly(protocol._HEADER_LENGTH)

    async def unwrap_message_asgi(
        self, receive
    ) -> typing.AsyncIterable[tuple[bool, bool, bytes]]:
        buffer = bytearray()
        waiting = False
        flags = None
        length = None

        while True:
            event = await receive()
            assert event["type"].startswith("http.")

            chunk = event["body"]

            buffer += chunk

            if len(buffer) >= protocol._HEADER_LENGTH:
                if not waiting:
                    flags, length = struct.unpack(
                        protocol._HEADER_FORMAT, buffer[: protocol._HEADER_LENGTH]
                    )

                if len(buffer) >= protocol._HEADER_LENGTH + length:
                    waiting = False
                    data = buffer[
                        protocol._HEADER_LENGTH : protocol._HEADER_LENGTH + length
                    ]
                    trailers, compressed = self.unpack_header_flags(flags)

                    yield trailers, compressed, data
                    buffer = buffer[protocol._HEADER_LENGTH + length :]
                else:
                    waiting = True

            if not event.get("more_body"):
                break


class GrpcCodec(Codec):
    def __init__(self, encoding, serializer):
        super().__init__(encoding, serializer)
        self._started = False

    @property
    def requires_trailers(self):
        return True

    def pack_header_flags(self, trailers, compressed):
        return (trailers << 7) | (compressed)

    def unpack_header_flags(self, flags):
        trailers = 1 << 7
        compressed = 1

        return bool(trailers & flags), bool(compressed & flags)

    def wrap_message(self, trailers, compressed, message):
        return (
            struct.pack(
                protocol._HEADER_FORMAT,
                self.pack_header_flags(trailers, compressed),
                len(message),
            )
            + message
        )

    def send_request(self, request):
        body = self.serializer.serialize_request(request)
        body = self.wrap_message(False, False, body)
        yield SendBody(body)

    def _start(self):
        if self._started:
            return

        self._started = True

        headers = protocol.encode_headers(
            itertools.chain(
                (("content-type", self.content_type),),
                (self._initial_metadata),
            )
        )

        yield StartResponse(200, "OK", headers, trailers=self.requires_trailers)

    def send_response(self, response):
        yield from self._start()

        body = self.wrap_message(
            False, False, self.serializer.serialize_response(response)
        )

        yield SendBody(body)

    def end_response(self):
        yield from self._start()

        trailers = [("grpc-status", str(self._code.value[0]))]

        if self._details:
            trailers.append(("grpc-message", quote(self._details)))

        if self._trailing_metadata:
            trailers.extend(self._trailing_metadata)

        yield SendTrailers(protocol.encode_headers(trailers))


class GrpcJsonCodec(GrpcCodec):
    @property
    def content_type(self):
        return "application/grpc+json"


class GrpcProtoCodec(GrpcCodec):
    @property
    def content_type(self):
        return "application/grpc+proto"


class GrpcWebCodec(GrpcCodec):
    @property
    def requires_trailers(self):
        return False

    def end_response(self):
        yield from self._start()

        trailers = [("grpc-status", str(self._code.value[0]))]

        if self._details:
            trailers.append(("grpc-message", quote(self._details)))

        if self._trailing_metadata:
            trailers.extend(self._trailing_metadata)

        body = protocol.pack_trailers(protocol.encode_headers(trailers))
        body = self.wrap_message(True, False, body)

        yield SendBody(body, more_body=False)


class GrpcWebJsonCodec(GrpcWebCodec):
    @property
    def content_type(self):
        return "application/grpc-web+json"


class GrpcWebProtoCodec(GrpcWebCodec):
    @property
    def content_type(self):
        return "application/grpc-web+proto"


class GrpcWebTextCodec(GrpcWebProtoCodec):
    def __init__(self, encoding, serializer):
        super().__init__(encoding, serializer)

    @property
    def content_type(self):
        return "application/grpc-web+proto"

    def wrap_message(self, trailers, compressed, message):
        return protocol.b64encode(super().wrap_message(trailers, compressed, message))

    def unwrap_message(self, message):
        return super().unwrap_message(protocol.b64decode(message))


class ConnectCodec(GrpcCodec):
    def __init__(self, encoding, serializer):
        super().__init__(encoding, serializer)

    @property
    def requires_trailers(self):
        return False

    def unpack_header_flags(self, flags):
        trailers = 1 << 1
        compressed = 1

        return bool(trailers & flags), bool(compressed & flags)

    def pack_header_flags(self, trailers, compressed):
        return (trailers << 1) | (compressed)

    def unpack_error(self) -> dict | None:
        if self._code == grpc.StatusCode.OK:
            return None

        code = protocol.code_to_named_status(self._code)
        error = {"code": code}
        if self._details:
            error["message"] = self._details

        for name, value in self._trailing_metadata:
            if name.lower() == "grpc-status-details-bin":
                # TODO: it's annoying to have to round trip this.
                status_details = status_pb2.Status()
                status_details.ParseFromString(value)
                error["details"] = [
                    {
                        "type": d.type_url.rpartition("/")[2],
                        "value": protocol.b64encode(d.value),
                    }
                    for d in status_details.details
                ]
        return error

    def start_response(self, response):
        initial_metadata, trailing_metadata = protocol.split_trailers(
            protocol.Metadata(response.headers)
        )
        code = protocol.http_status_to_status_code(response.status_code)
        self.set_code(code)
        self.set_initial_metadata(initial_metadata)
        self.set_trailing_metadata(trailing_metadata)

        content_type = self._initial_metadata.get("content-type")
        encoding = self._initial_metadata.get("content-encoding", "identity")
        message = response.phrase
        # TODO: other compatible subtypes should be permitted
        if (
            content_type != self.content_type
            and content_type != "application/json"
            and content_type is not None
        ):
            code = grpc.StatusCode.UNKNOWN
            message = "Unexpected content-type"
        elif encoding != self.encoding.encoding:
            code = grpc.StatusCode.INTERNAL
            message = "Unexpected encoding"
        elif content_type is None:
            pass
        else:
            return tuple()

        # Unexpected content type
        if code == grpc.StatusCode.OK:
            code = grpc.StatusCode.UNKNOWN
        raise protocol.WebRpcError(
            code,
            message,
            initial_metadata=self._initial_metadata,
            trailing_metadata=self._trailing_metadata,
        )

    def receive_body(self, body):
        if self._code != grpc.StatusCode.OK:
            try:
                data = json.loads(body)
                protocol.unpack_error_connect(
                    data, self._initial_metadata, self._trailing_metadata, self._code
                )
            except ValueError:
                pass
            raise protocol.WebRpcError(
                code=self._code,
                details="Invalid error response",
                initial_metadata=self._initial_metadata,
                trailing_metadata=self._trailing_metadata,
            )

        rest = body
        while rest:
            trailers, compressed, body, rest = self.unwrap_message(rest)
            if trailers:
                trailing_metadata = protocol.unpack_trailers_connect(
                    body, self._initial_metadata
                )
                self.set_trailing_metadata(trailing_metadata)
                return tuple()

            body = self.encoding.decode(compressed, body)
            try:
                message = self.serializer.deserialize_response(body)
            except Exception:
                raise protocol.WebRpcError(
                    code=grpc.StatusCode.INTERNAL,
                    details="Could not decode response",
                    initial_metadata=self._initial_metadata,
                    trailing_metadata=self._trailing_metadata,
                )

            yield ReceiveMessage(message=message)


class ConnectUnaryCodec(ConnectCodec):
    def __init__(self, encoding, serializer):
        super().__init__(encoding, serializer)
        self._response = None
        self._request = None

    wrap_message = staticmethod(protocol.bare_wrap_message)
    unwrap_message_stream = staticmethod(protocol.bare_unwrap_message_stream)
    unwrap_message_asgi = staticmethod(protocol.bare_unwrap_message_asgi)

    def unwrap_message(self, body):
        return False, False, body, b""

    def send_request(self, request):
        self._request = request
        return tuple()

    def start_request(self):
        if self._request:
            body = self.serializer.serialize_request(self._request)
        else:
            body = b""

        yield StartRequest(
            "POST",
            headers=protocol.encode_headers(
                itertools.chain(
                    (
                        ("content-type", self.content_type),
                        ("content-length", str(len(body))),
                    ),
                    ()
                    if self._timeout is None
                    else (("connect-timeout-ms", str(int(self._timeout * 1000))),),
                    self._invocation_metadata,
                )
            ),
        )
        yield SendBody(body, more_body=False)

    def send_response(self, response):
        self._response = response
        return tuple()

    def _end_error(self):
        # http library does not include definition for 499.
        if self._code == grpc.StatusCode.CANCELLED:
            status_code = 499
            phrase = "Canceled"
        else:
            http_status = HTTPStatus(protocol.status_code_to_http(self._code))
            status_code = http_status.value
            phrase = http_status.phrase

        error = self.unpack_error()
        body = json.dumps(error).encode()

        headers = protocol.encode_headers(
            itertools.chain(
                (
                    ("content-type", "application/json"),
                    ("content-length", str(len(body))),
                ),
                (self._initial_metadata),
                (
                    (f"trailer-{name}", value)
                    for (name, value) in self._trailing_metadata
                ),
            )
        )

        yield StartResponse(status_code, phrase, headers)
        yield SendBody(body, more_body=False)

    def end_response(self):
        if self._code != grpc.StatusCode.OK:
            yield from self._end_error()
            return

        if self._response is None:
            body = b""
        else:
            body = self.wrap_message(
                False, False, self.serializer.serialize_response(self._response)
            )

        headers = protocol.encode_headers(
            itertools.chain(
                (
                    ("content-type", self.content_type),
                    ("content-length", str(len(body))),
                ),
                (self._initial_metadata),
                (
                    (f"trailer-{name}", value)
                    for (name, value) in self._trailing_metadata
                ),
            )
        )

        yield StartResponse(200, "OK", headers)
        yield SendBody(body, more_body=False)

    def receive_body(self, body):
        if self._code == grpc.StatusCode.OK and not body:
            # an empty response is valid.
            message = self.serializer.deserialize_response(body)
            yield ReceiveMessage(message=message)
        else:
            yield from super().receive_body(body)


class ConnectUnaryJsonCodec(ConnectUnaryCodec):
    @property
    def content_type(self):
        return "application/json"


class ConnectUnaryProtoCodec(ConnectUnaryCodec):
    @property
    def content_type(self):
        return "application/proto"


class ConnectStreamCodec(ConnectCodec):
    def __init__(self, encoding, serializer):
        super().__init__(encoding, serializer)
        self._started = False

    wrap_message = staticmethod(protocol.wrap_message_connect)
    unwrap_message_stream = staticmethod(protocol.unwrap_message_stream_connect)

    def start_request(self):
        yield StartRequest(
            "POST",
            headers=protocol.encode_headers(
                itertools.chain(
                    (("content-type", self.content_type),),
                    ()
                    if self._timeout is None
                    else (("connect-timeout-ms", str(int(self._timeout * 1000))),),
                    self._invocation_metadata,
                )
            ),
        )

    def end_response(self):
        yield from self._start()

        trailer_dict = {}
        for name, value in protocol.encode_headers(self._trailing_metadata):
            trailer_dict.setdefault(name, []).append(value)

        end_of_stream = {"metadata": trailer_dict}
        error = self.unpack_error()
        if error is not None:
            end_of_stream["error"] = error

        body = json.dumps(end_of_stream).encode()
        body = self.wrap_message(True, False, body)

        yield SendBody(body, more_body=False)


class ConnectStreamJsonCodec(ConnectStreamCodec):
    @property
    def content_type(self):
        return "application/connect+json"


class ConnectStreamProtoCodec(ConnectStreamCodec):
    @property
    def content_type(self):
        return "application/connect+proto"


_CODECS = [
    ("application/grpc-web-text", "grpc-encoding", ProtoSerializer, GrpcWebTextCodec),
    ("application/grpc-web", "grpc-encoding", ProtoSerializer, GrpcWebProtoCodec),
    ("application/grpc-web+proto", "grpc-encoding", ProtoSerializer, GrpcWebProtoCodec),
    ("application/grpc-web+json", "grpc-encoding", JsonSerializer, GrpcWebJsonCodec),
    ("application/grpc", "grpc-encoding", ProtoSerializer, GrpcProtoCodec),
    ("application/grpc+proto", "grpc-encoding", ProtoSerializer, GrpcProtoCodec),
    ("application/grpc+json", "grpc-encoding", JsonSerializer, GrpcJsonCodec),
    ("application/proto", "content-encoding", ProtoSerializer, ConnectUnaryProtoCodec),
    ("application/json", "content-encoding", JsonSerializer, ConnectUnaryJsonCodec),
    (
        "application/connect+proto",
        "content-encoding",
        ProtoSerializer,
        ConnectStreamProtoCodec,
    ),
    (
        "application/connect+json",
        "content-encoding",
        JsonSerializer,
        ConnectStreamJsonCodec,
    ),
]


def get_codec(
    metadata: Metadata,
    rpc_method_handler: RpcMethodHandler,
    enable_trailers: bool,
) -> Codec:
    content_type = metadata["content-type"]
    codec = None
    for accept, encoding_name, serializer_class, codec_class in _CODECS:
        # TODO refer to accept header as well as transmitted `accept` in priority order?
        if accept == content_type:
            encoding = get_encoding(metadata.get(encoding_name))
            serializer = serializer_class(
                request_deserializer=rpc_method_handler.request_deserializer,
                response_serializer=rpc_method_handler.response_serializer,
            )
            codec = codec_class(encoding, serializer)

    if codec is None or codec.requires_trailers and not enable_trailers:
        raise protocol.InvalidContentType(f"Unsupported content-type: {content_type}")

    return codec
