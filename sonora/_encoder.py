import abc
import struct
import typing

import grpc
from google.protobuf.message import Message

from sonora import protocol
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


class Encoding:
    @abc.abstractmethod
    def decode(self, compressed: bool, message: bytes) -> bool: ...

    @abc.abstractmethod
    def encode(self, message: bytes) -> bytes: ...


class IdentityEncoding(Encoding):
    decode = staticmethod(protocol.decode_identity)

    def encode(self, message: bytes) -> bytes:
        return message


class Base64Encoding(Encoding):
    def decode(self, _compressed, message):
        return protocol.b64decode(message)

    def encode(self, message):
        return protocol.b64encode(message)


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

    @property
    @abc.abstractmethod
    def requires_trailers(self): ...
    @abc.abstractmethod
    def unpack_header_flags(self, flags: int) -> tuple[bool, bool]: ...
    @abc.abstractmethod
    def pack_header_flags(self, trailers: bool, compressed: bool) -> int: ...

    def wrap_message(self, trailers: bool, compressed: bool, message: bytes) -> bytes:
        return (
            struct.pack(
                protocol._HEADER_FORMAT,
                self.pack_header_flags(trailers, compressed),
                len(message),
            )
            + message
        )

    def unwrap_message(self, message: bytes) -> tuple[bool, bool, bytes]:
        if len(message) < protocol._HEADER_LENGTH:
            raise ValueError()
        flags, length = struct.unpack(
            protocol._HEADER_FORMAT, message[: protocol._HEADER_LENGTH]
        )
        data = message[protocol._HEADER_LENGTH : protocol._HEADER_LENGTH + length]

        if length != len(data):
            raise ValueError()

        trailers, compressed = self.unpack_header_flags(flags)
        return trailers, compressed, data

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
    @property
    def requires_trailers(self):
        return False

    def unpack_header_flags(self, flags):
        trailers = 1 << 1
        compressed = 1

        return bool(trailers & flags), bool(compressed & flags)

    def pack_header_flags(self, trailers, compressed):
        return (trailers << 1) | (compressed)


class ConnectUnaryCodec(ConnectCodec):
    wrap_message = staticmethod(protocol.bare_wrap_message)
    unwrap_message = staticmethod(protocol.bare_unwrap_message_stream)


class ConnectUnaryJsonCodec(ConnectUnaryCodec):
    @property
    def content_type(self):
        return "application/json"


class ConnectUnaryProtoCodec(ConnectUnaryCodec):
    @property
    def content_type(self):
        return "application/proto"


class ConnectStreamCodec(ConnectCodec):
    wrap_message = staticmethod(protocol.wrap_message_connect)
    unwrap_message = staticmethod(protocol.unwrap_message_stream_connect)


class ConnectStreamJsonCodec(ConnectStreamCodec):
    @property
    def content_type(self):
        return "application/connect+json"


class ConnectStreamProtoCodec(ConnectStreamCodec):
    @property
    def content_type(self):
        return "application/connect+proto"


def get_encoding(encoding: str | None) -> Encoding:
    if encoding is None or encoding.lower() == "identity":
        return IdentityEncoding()
    raise protocol.WebRpcError(
        code=grpc.StatusCode.UNIMPLEMENTED, details=f"Unsupported encoding: {encoding}"
    )


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
