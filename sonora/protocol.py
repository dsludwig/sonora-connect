import functools
import json
import struct
from urllib.parse import unquote

import grpc
from google.protobuf import any_pb2, json_format
from google.rpc import status_pb2
from grpc_status import rpc_status

from sonora.encode import b64decode, b64encode
from sonora.metadata import Metadata

_HEADER_FORMAT = ">BI"
_HEADER_LENGTH = struct.calcsize(_HEADER_FORMAT)


def _pack_header_flags(trailers, compressed):
    return (trailers << 7) | (compressed)


def _pack_header_flags_connect(trailers, compressed):
    return (trailers << 1) | (compressed)


def _unpack_header_flags(flags):
    trailers = 1 << 7
    compressed = 1

    return bool(trailers & flags), bool(compressed & flags)


def _unpack_header_flags_connect(flags):
    trailers = 1 << 1
    compressed = 1

    return bool(trailers & flags), bool(compressed & flags)


def wrap_message(trailers, compressed, message, pack_header_flags=_pack_header_flags):
    return (
        struct.pack(
            _HEADER_FORMAT, pack_header_flags(trailers, compressed), len(message)
        )
        + message
    )


wrap_message_connect = functools.partial(
    wrap_message, pack_header_flags=_pack_header_flags_connect
)


def b64_wrap_message(trailers, compressed, message):
    return b64encode(wrap_message(trailers, compressed, message))


def unwrap_message(message, unpack_header_flags=_unpack_header_flags):
    if len(message) < _HEADER_LENGTH:
        raise ValueError()
    flags, length = struct.unpack(_HEADER_FORMAT, message[:_HEADER_LENGTH])
    data = message[_HEADER_LENGTH : _HEADER_LENGTH + length]

    if length != len(data):
        raise ValueError()

    trailers, compressed = unpack_header_flags(flags)

    return trailers, compressed, data


unwrap_message_connect = functools.partial(
    unwrap_message, unpack_header_flags=_unpack_header_flags_connect
)


def b64_unwrap_message(message):
    return unwrap_message(b64decode(message))


def unwrap_message_stream(
    stream, decoder=None, unpack_header_flags=_unpack_header_flags
):
    data = stream.read(_HEADER_LENGTH)

    while data:
        flags, length = struct.unpack(_HEADER_FORMAT, data)
        trailers, compressed = unpack_header_flags(flags)

        body = stream.read(length)
        if decoder:
            body = decoder(body)
        yield trailers, compressed, body

        if trailers:
            break

        data = stream.read(_HEADER_LENGTH)


async def unwrap_message_stream_async(stream, unpack_header_flags=_unpack_header_flags):
    data = await stream.readexactly(_HEADER_LENGTH)

    while data:
        flags, length = struct.unpack(_HEADER_FORMAT, data)
        trailers, compressed = unpack_header_flags(flags)

        yield trailers, compressed, await stream.readexactly(length)

        if trailers:
            break

        data = await stream.readexactly(_HEADER_LENGTH)


async def unwrap_message_asgi(
    receive, decoder=None, unpack_header_flags=_unpack_header_flags
):
    buffer = bytearray()
    waiting = False
    flags = None
    length = None

    while True:
        event = await receive()
        assert event["type"].startswith("http.")

        if decoder:
            chunk = decoder(event["body"])
        else:
            chunk = event["body"]

        buffer += chunk

        if len(buffer) >= _HEADER_LENGTH:
            if not waiting:
                flags, length = struct.unpack(_HEADER_FORMAT, buffer[:_HEADER_LENGTH])

            if len(buffer) >= _HEADER_LENGTH + length:
                waiting = False
                data = buffer[_HEADER_LENGTH : _HEADER_LENGTH + length]
                trailers, compressed = unpack_header_flags(flags)

                yield trailers, compressed, data
                buffer = buffer[_HEADER_LENGTH + length :]
            else:
                waiting = True

        if not event.get("more_body"):
            break


b64_unwrap_message_stream = functools.partial(unwrap_message_stream, decoder=b64decode)
b64_unwrap_message_asgi = functools.partial(unwrap_message_asgi, decoder=b64decode)

unwrap_message_asgi_connect = functools.partial(
    unwrap_message_asgi,
    unpack_header_flags=_unpack_header_flags_connect,
)

unwrap_message_stream_connect = functools.partial(
    unwrap_message_stream, unpack_header_flags=_unpack_header_flags_connect
)


async def bare_unwrap_message_asgi(receive):
    body = bytearray()
    while True:
        event = await receive()
        assert event["type"].startswith("http.")
        body.extend(event["body"])

        if not event.get("more_body"):
            break

    yield False, False, body


def bare_wrap_message(_trailers, _compressed, message):
    return message


def bare_unwrap_message_stream(stream, unpack_header_flags=_unpack_header_flags):
    yield False, False, stream.read()


class ProtocolError(Exception):
    pass


def decode_identity(compressed, message):
    if compressed:
        raise ProtocolError("Cannot decode compressed message with `identity` encoder")
    return message


def pack_trailers(trailers):
    data = bytearray()
    for k, v in trailers:
        k = k.lower()
        data.extend(k.encode("utf8"))
        data.extend(b": ")
        data.extend(v.encode("utf8"))
        data.extend(b"\r\n")
    return bytes(data)


def unpack_trailers(message, _initial_metadata=None):
    trailers = []
    for line in message.decode("ascii").splitlines():
        k, v = line.split(":", 1)
        v = v.strip()
        if k.endswith("-bin"):
            v = b64decode(v)

        trailers.append((k, v))
    return trailers


def encode_headers(metadata):
    for header, value in metadata:
        if isinstance(value, bytes):
            if not header.endswith("-bin"):
                raise ValueError("binary headers must have the '-bin' suffix")

            value = b64encode(value)

        if isinstance(header, bytes):
            header = header.decode("ascii")

        yield header, value


def split_trailers(metadata):
    initial_metadata = Metadata()
    trailing_metadata = Metadata()
    for header, value in metadata:
        if header.startswith("trailer-"):
            _, _, header = header.partition("trailer-")
            trailing_metadata.add(header, value)
        else:
            initial_metadata.add(header, value)
    return initial_metadata, trailing_metadata


def unpack_error_connect(
    error: dict | None,
    initial_metadata: Metadata,
    trailing_metadata: Metadata,
    status_code: grpc.StatusCode | None = None,
):
    if error is None:
        return
        # raise WebRpcError(
        #     code=grpc.StatusCode.INTERNAL,
        #     details="Invalid error message",
        # )
    code = error.get("code")
    if code is not None and code in _STATUS_CODE_NAME_MAP:
        status_code = named_status_to_code(error.get("code"))

    status = status_pb2.Status(
        code=status_code.value[0],
        message=error.get("message"),
    )
    if "details" in error:
        for detail in error["details"]:
            any = any_pb2.Any(
                type_url=f'type.googleapis.com/{detail["type"]}',
                value=b64decode(detail["value"]),
            )
            status.details.append(any)

    code, message, status_trailing_metadata = rpc_status.to_status(status)
    if trailing_metadata is None:
        trailing_metadata = status_trailing_metadata
    else:
        trailing_metadata.extend(status_trailing_metadata)
    raise WebRpcError(
        code=code,
        details=message,
        initial_metadata=initial_metadata,
        trailing_metadata=trailing_metadata,
    )


def unpack_trailers_connect(response, initial_metadata):
    response = json.loads(response)
    trailing_metadata = None
    if "metadata" in response:
        trailing_metadata = Metadata(response["metadata"])
    if "error" in response:
        error = response["error"]
        unpack_error_connect(error, initial_metadata, trailing_metadata)

    return trailing_metadata


def deserialize_json(request_deserializer):
    klass = request_deserializer.__self__

    def deserializer(buffer):
        message = klass()
        return json_format.Parse(buffer, message)

    return deserializer


def serialize_json(response_serializer):
    def serializer(message):
        return json_format.MessageToJson(message).encode()

    return serializer


_STATUS_CODE_MAP = {
    grpc.StatusCode.OK: 200,
    grpc.StatusCode.CANCELLED: 499,
    grpc.StatusCode.UNKNOWN: 500,
    grpc.StatusCode.INVALID_ARGUMENT: 400,
    grpc.StatusCode.DEADLINE_EXCEEDED: 504,
    grpc.StatusCode.NOT_FOUND: 404,
    grpc.StatusCode.ALREADY_EXISTS: 409,
    grpc.StatusCode.PERMISSION_DENIED: 403,
    grpc.StatusCode.RESOURCE_EXHAUSTED: 429,
    grpc.StatusCode.FAILED_PRECONDITION: 400,
    grpc.StatusCode.ABORTED: 409,
    grpc.StatusCode.OUT_OF_RANGE: 400,
    grpc.StatusCode.UNIMPLEMENTED: 501,
    grpc.StatusCode.INTERNAL: 500,
    grpc.StatusCode.UNAVAILABLE: 503,
    grpc.StatusCode.DATA_LOSS: 500,
    grpc.StatusCode.UNAUTHENTICATED: 401,
}
_STATUS_CODE_HTTP_MAP = {
    400: grpc.StatusCode.INTERNAL,
    401: grpc.StatusCode.UNAUTHENTICATED,
    403: grpc.StatusCode.PERMISSION_DENIED,
    404: grpc.StatusCode.UNIMPLEMENTED,
    429: grpc.StatusCode.UNAVAILABLE,
    502: grpc.StatusCode.UNAVAILABLE,
    503: grpc.StatusCode.UNAVAILABLE,
    504: grpc.StatusCode.UNAVAILABLE,
}
_STATUS_CODE_NAME_MAP = {
    status_code.name.lower(): status_code for status_code in grpc.StatusCode
}
# ConnectRPC uses a different name.
_STATUS_CODE_NAME_MAP["canceled"] = grpc.StatusCode.CANCELLED


def status_code_to_http(status_code):
    return _STATUS_CODE_MAP.get(status_code, 500)


def http_status_to_status_code(status: int) -> grpc.StatusCode:
    return _STATUS_CODE_HTTP_MAP.get(status, grpc.StatusCode.UNKNOWN)


def named_status_to_code(name: str) -> grpc.StatusCode:
    return _STATUS_CODE_NAME_MAP.get(name, grpc.StatusCode.UNKNOWN)


class WebRpcError(grpc.RpcError):
    _code_to_enum = {code.value[0]: code for code in grpc.StatusCode}  # type: ignore

    def __init__(
        self,
        code,
        details,
        *args,
        initial_metadata=None,
        trailing_metadata=None,
        **kwargs,
    ):
        super(WebRpcError, self).__init__(*args, **kwargs)

        self._code = code
        self._details = details
        self._initial_metadata = initial_metadata or []
        self._trailing_metadata = trailing_metadata or []

    @classmethod
    def from_metadata(cls, trailers):
        status = int(trailers["grpc-status"])
        details = trailers.get("grpc-message")

        code = cls._code_to_enum[status]

        return cls(
            code,
            details,
            trailing_metadata=trailers,
        )

    def __str__(self):
        return "WebRpcError(status_code={}, details='{}')".format(
            self._code, self._details
        )

    def http_status_code(self):
        return 200

    def initial_metadata(self):
        return self._initial_metadata

    def trailing_metadata(self):
        return self._trailing_metadata

    def code(self):
        return self._code

    def details(self):
        return self._details


def raise_for_status(headers, trailers=None, expected_content_types=[]):
    if trailers:
        # prioritize trailers over headers
        metadata = Metadata(trailers)
        metadata.extend(headers)
    else:
        metadata = Metadata(headers)

    if (
        "content-type" in metadata
        and metadata["content-type"] not in expected_content_types
    ):
        raise WebRpcError(
            grpc.StatusCode.UNKNOWN,
            "Invalid content-type",
        )

    if headers is not None and trailers is not None and "grpc-status" not in metadata:
        raise WebRpcError(grpc.StatusCode.INTERNAL, "Missing grpc-status header")

    if "grpc-status" in metadata and metadata["grpc-status"] != "0":
        metadata = metadata.copy()

        if "grpc-message" in metadata:
            metadata["grpc-message"] = unquote(metadata["grpc-message"])

        status = int(metadata["grpc-status"])
        details = metadata.get("grpc-message")
        code = WebRpcError._code_to_enum[status]

        raise WebRpcError(
            code,
            details,
            initial_metadata=Metadata(headers) if trailers else None,
            trailing_metadata=Metadata(trailers) if trailers else Metadata(headers),
        )


def raise_for_status_connect(headers, trailers=None, expected_content_types=[]):
    if trailers:
        # prioritize trailers over headers
        metadata = Metadata(trailers)
        metadata.extend(headers)
    else:
        metadata = Metadata(headers)

    if (
        "content-type" in metadata
        and metadata["content-type"] not in expected_content_types
    ):
        raise WebRpcError(
            grpc.StatusCode.UNKNOWN,
            "Invalid content-type",
        )


_timeout_units = {
    b"H": 3600.0,
    b"M": 60.0,
    b"S": 1.0,
    b"m": 1 / 1000.0,
    b"u": 1 / 1000000.0,
    b"n": 1 / 1000000000.0,
    "H": 3600.0,
    "M": 60.0,
    "S": 1.0,
    "m": 1 / 1000.0,
    "u": 1 / 1000000.0,
    "n": 1 / 1000000000.0,
}


def parse_timeout(value):
    units = value[-1:]
    coef = _timeout_units[units]
    count = int(value[:-1])
    return count * coef


def serialize_timeout(seconds):
    if seconds % 3600 == 0:
        value = seconds // 3600
        units = "H"
    elif seconds % 60 == 0:
        value = seconds // 60
        units = "M"
    elif seconds % 1 == 0:
        value = seconds
        units = "S"
    elif seconds * 1000 % 1 == 0:
        value = seconds * 1000
        units = "m"
    elif seconds * 1000000 % 1 == 0:
        value = seconds * 1000000
        units = "u"
    else:
        value = seconds * 1000000000
        units = "n"

    return f"{int(value)}{units}"
