import abc
import base64
import itertools
import json
import math
import struct
import typing
from http import HTTPStatus
from urllib.parse import quote

import grpc
from google.protobuf.message import Message

if typing.TYPE_CHECKING:
    from sonora import _status_pb2 as status_pb2
else:
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

Des = typing.Callable[[bytes], Message]
Ser = typing.Callable[[Message], bytes]


class RpcMethodHandler(typing.NamedTuple):
    request_streaming: bool
    response_streaming: bool
    request_deserializer: typing.Optional[Des]
    response_serializer: typing.Optional[Ser]


class RpcOperation(typing.NamedTuple):
    request_streaming: bool
    response_streaming: bool
    request_serializer: typing.Optional[Ser]
    response_deserializer: typing.Optional[Des]


def _transform(
    serializer: typing.Union[Ser, Des, None], message: typing.Any
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
        request_serializer: typing.Optional[Ser] = None,
        response_deserializer: typing.Optional[Des] = None,
        request_deserializer: typing.Optional[Des] = None,
        response_serializer: typing.Optional[Ser] = None,
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
        request_serializer: typing.Optional[Ser] = None,
        response_deserializer: typing.Optional[Des] = None,
        request_deserializer: typing.Optional[Des] = None,
        response_serializer: typing.Optional[Ser] = None,
    ):
        super().__init__(
            protocol.serialize_json(request_serializer) if request_serializer else None,
            (
                protocol.deserialize_json(response_deserializer)
                if response_deserializer
                else None
            ),
            (
                protocol.deserialize_json(request_deserializer)
                if request_deserializer
                else None
            ),
            (
                protocol.serialize_json(response_serializer)
                if response_serializer
                else None
            ),
        )


class Codec:
    def __init__(self, encoding: Encoding, serializer: Serializer):
        self.encoding = encoding
        self.serializer = serializer
        self._initial_metadata = Metadata()
        self._trailing_metadata = Metadata()
        self._invocation_metadata = Metadata()
        self._timeout: typing.Optional[float] = None
        self._code = grpc.StatusCode.OK
        self._details: typing.Optional[str] = None
        self._buffer = bytearray()

    @property
    @abc.abstractmethod
    def content_type(self) -> str: ...

    @property
    def accepted_content_types(self) -> typing.Tuple[str, ...]:
        return (self.content_type,)

    @property
    @abc.abstractmethod
    def requires_trailers(self) -> bool: ...

    @abc.abstractmethod
    def unpack_header_flags(self, flags: int) -> typing.Tuple[bool, bool]: ...

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
    def server_receive_body(self, body: bytes, more_body: bool) -> ClientEvents: ...

    @abc.abstractmethod
    def send_response(self, response: Message) -> ServerEvents: ...

    @abc.abstractmethod
    def end_response(self) -> ServerEvents: ...

    def wrap_message(self, trailers: bool, compressed: bool, message: bytes) -> bytes:
        return (
            struct.pack(
                protocol.HEADER_FORMAT,
                self.pack_header_flags(trailers, compressed),
                len(message),
            )
            + message
        )

    def unwrap_message(self, message: bytes) -> typing.Tuple[bool, bool, bytes, bytes]:
        if len(message) < protocol.HEADER_LENGTH:
            raise ValueError()
        flags, length = struct.unpack(
            protocol.HEADER_FORMAT, message[: protocol.HEADER_LENGTH]
        )
        if (protocol.HEADER_LENGTH + length) > len(message):
            raise ValueError()

        data = message[protocol.HEADER_LENGTH : protocol.HEADER_LENGTH + length]
        trailers, compressed = self.unpack_header_flags(flags)
        return (
            trailers,
            compressed,
            bytes(data),
            message[protocol.HEADER_LENGTH + length :],
        )

    def unwrap_message_stream(
        self, stream
    ) -> typing.Iterable[typing.Tuple[bool, bool, bytes]]:
        data = stream.read(protocol.HEADER_LENGTH)

        while data:
            flags, length = struct.unpack(protocol.HEADER_FORMAT, data)
            trailers, compressed = self.unpack_header_flags(flags)

            body = stream.read(length)
            yield trailers, compressed, body

            if trailers:
                break

            data = stream.read(protocol.HEADER_LENGTH)


class GrpcCodec(Codec):
    def __init__(self, encoding, serializer):
        super().__init__(encoding, serializer)
        self._started = False
        self._closed = False

    @property
    def requires_trailers(self):
        return True

    def pack_header_flags(self, trailers, compressed):
        return (trailers << 7) | (compressed)

    def unpack_header_flags(self, flags):
        trailers = 1 << 7
        compressed = 1

        return bool(trailers & flags), bool(compressed & flags)

    def start_request(self):
        yield StartRequest(
            "POST",
            headers=protocol.encode_headers(
                itertools.chain(
                    (("content-type", self.content_type),),
                    (
                        ()
                        if self._timeout is None
                        else (
                            (
                                "grpc-timeout",
                                protocol.serialize_timeout(self._timeout),
                            ),
                        )
                    ),
                    self._invocation_metadata,
                )
            ),
        )

    def send_request(self, request):
        body = self.serializer.serialize_request(request)
        body = self.wrap_message(False, False, body)
        yield SendBody(body)

    def start_response(self, response):
        initial_metadata = protocol.Metadata(response.headers)
        self.set_initial_metadata(initial_metadata)
        if response.status_code != 200:
            code = protocol.http_status_to_status_code(response.status_code)
            self.set_code(code)
            self.set_details(response.phrase)
            raise protocol.WebRpcError(
                self._code,
                self._details,
                initial_metadata=self._initial_metadata,
                trailing_metadata=self._trailing_metadata,
            )

        content_type = initial_metadata.get("content-type")
        encoding = initial_metadata.get("grpc-encoding", "identity")
        if content_type not in self.accepted_content_types and content_type is not None:
            code = grpc.StatusCode.UNKNOWN
            message = "Unexpected content-type"
            self.set_code(code)
            self.set_details(message)
        elif encoding != self.encoding.encoding:
            code = grpc.StatusCode.INTERNAL
            message = "Unexpected encoding"
            self.set_code(code)
            self.set_details(message)
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

    def server_receive_body(self, body, more_body):
        self._buffer.extend(body)
        while self._buffer:
            try:
                trailers, compressed, body, self._buffer = self.unwrap_message(
                    self._buffer
                )
            except ValueError:
                return

            if trailers:
                trailing_metadata = Metadata(protocol.unpack_trailers(body))
                self.set_trailing_metadata(trailing_metadata)
                return

            body = self.encoding.decode(compressed, body)
            message = self.serializer.deserialize_request(body)

            yield ReceiveMessage(message=message)

    def receive_body(self, body):
        self._buffer.extend(body)
        while self._buffer:
            if self._closed:
                raise protocol.WebRpcError(
                    code=grpc.StatusCode.CANCELLED,
                    details="Already received trailers, cannot receive any more",
                    initial_metadata=self._initial_metadata,
                    trailing_metadata=self._trailing_metadata,
                )

            try:
                trailers, compressed, body, self._buffer = self.unwrap_message(
                    self._buffer
                )
            except ValueError:
                return

            if trailers:
                trailing_metadata = Metadata(protocol.unpack_trailers(body))
                self.set_trailing_metadata(trailing_metadata)
                self._closed = True
                return

            try:
                body = self.encoding.decode(compressed, body)
            except protocol.ProtocolError:
                raise protocol.WebRpcError(
                    code=grpc.StatusCode.INTERNAL,
                    details="Could not decode response",
                    initial_metadata=self._initial_metadata,
                    trailing_metadata=self._trailing_metadata,
                )
            try:
                message = self.serializer.deserialize_response(body)
            except Exception:
                raise protocol.WebRpcError(
                    code=grpc.StatusCode.INTERNAL,
                    details="Could not deserialize response",
                    initial_metadata=self._initial_metadata,
                    trailing_metadata=self._trailing_metadata,
                )

            yield ReceiveMessage(message=message)

    def end_request(self):
        protocol.raise_for_status(
            self._initial_metadata,
            self._trailing_metadata,
            self.accepted_content_types,
        )
        return tuple()

    def _start(self):
        if self._closed:
            raise protocol.ProtocolError(
                "unexpected start of response after trailers have been sent"
            )

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
        self._closed = True


class GrpcJsonCodec(GrpcCodec):
    @property
    def content_type(self):
        return "application/grpc+json"


class GrpcProtoCodec(GrpcCodec):
    @property
    def content_type(self):
        return "application/grpc+proto"

    @property
    def accepted_content_types(self):
        return ("application/grpc+proto", "application/grpc")


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

    @property
    def accepted_content_types(self):
        return ("application/grpc-web+proto", "application/grpc-web")


class GrpcWebTextCodec(GrpcWebProtoCodec):
    def __init__(self, encoding, serializer):
        super().__init__(encoding, serializer)

    @property
    def content_type(self):
        return "application/grpc-web-text"

    @property
    def accepted_content_types(self):
        return ("application/grpc-web-text+proto", "application/grpc-web-text")

    def wrap_message(self, trailers, compressed, message):
        # Padding is required because we are concatenating multiple base64-encoded
        # messages. Omitting padding would produce incorrect results.
        return base64.b64encode(super().wrap_message(trailers, compressed, message))

    def unwrap_message(self, message):
        return super().unwrap_message(protocol.b64decode(message.decode("ascii")))

    def unwrap_message_stream(self, stream):
        # 5 bytes of header + potentially one of data.
        header_plus = stream.read(8).decode("ascii")
        while header_plus:
            header = base64.b64decode(header_plus)
            if len(header) < protocol.HEADER_LENGTH:
                raise ValueError()
            flags, length = struct.unpack(
                protocol.HEADER_FORMAT, header[: protocol.HEADER_LENGTH]
            )
            trailers, compressed = self.unpack_header_flags(flags)

            data = header[protocol.HEADER_LENGTH :]
            encoded_length = math.ceil((length - len(data)) / 3) * 4
            encoded_data = stream.read(encoded_length)

            data += base64.b64decode(encoded_data)
            yield trailers, compressed, data

            header_plus = stream.read(8).decode("ascii")


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

    def unpack_error(self) -> typing.Optional[dict]:
        if self._code == grpc.StatusCode.OK:
            return None

        code = protocol.code_to_named_status(self._code)
        error: typing.Dict[str, typing.Any] = {"code": code}
        if self._details:
            error["message"] = self._details

        for name, value in self._trailing_metadata:
            if name.lower() == "grpc-status-details-bin":
                # TODO: it's annoying to have to round trip this.
                if not isinstance(value, bytes):
                    value = protocol.b64decode(value)
                status_details = status_pb2.Status()
                status_details.ParseFromString(typing.cast(bytes, value))
                error["details"] = [
                    {
                        "type": d.type_url.rpartition("/")[2],
                        "value": protocol.b64encode(d.value),
                    }
                    for d in status_details.details
                ]
        return error

    def start_request(self):
        yield StartRequest(
            "POST",
            headers=protocol.encode_headers(
                itertools.chain(
                    (
                        ("content-type", self.content_type),
                        ("accept-encoding", self.encoding.encoding),
                    ),
                    (
                        ()
                        if self._timeout is None
                        else (("connect-timeout-ms", str(int(self._timeout * 1000))),)
                    ),
                    self._invocation_metadata,
                )
            ),
        )

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
            self.set_code(code)
            self.set_details(message)
        elif encoding != self.encoding.encoding:
            code = grpc.StatusCode.INTERNAL
            message = "Unexpected encoding"
            self.set_code(code)
            self.set_details(message)
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

        self._buffer.extend(body)
        while self._buffer:
            try:
                trailers, compressed, body, self._buffer = self.unwrap_message(
                    self._buffer
                )
            except ValueError:
                return

            if trailers:
                trailing_metadata = protocol.unpack_trailers_connect(
                    body, self._initial_metadata
                )
                self.set_trailing_metadata(trailing_metadata)
                return tuple()

            try:
                body = self.encoding.decode(compressed, body)
            except protocol.ProtocolError:
                raise protocol.WebRpcError(
                    code=grpc.StatusCode.INTERNAL,
                    details="Could not decode response",
                    initial_metadata=self._initial_metadata,
                    trailing_metadata=self._trailing_metadata,
                )
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

    def end_request(self):
        return tuple()


class ConnectUnaryCodec(ConnectCodec):
    def __init__(self, encoding, serializer):
        super().__init__(encoding, serializer)
        self._response = None
        self._request = None

    def unwrap_message_stream(self, stream):
        yield False, False, stream.read()

    def wrap_message(self, _trailers, _compressed, message):
        return message

    def unwrap_message(self, body):
        return False, False, bytes(body), bytearray()

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

    def server_receive_body(self, body, more_body):
        if not more_body and self._request is None:
            # an empty request is valid.
            _, compressed, body, _ = self.unwrap_message(body)
            body = self.encoding.decode(compressed, body)
            message = self.serializer.deserialize_request(body)
            yield ReceiveMessage(message=message)
        else:
            for event in super().server_receive_body(body, more_body):
                if isinstance(event, ReceiveMessage):
                    self._request = event.message
                yield event


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
        raise protocol.InvalidContentType(f"Unsupported content-type: {content_type!r}")

    return codec
