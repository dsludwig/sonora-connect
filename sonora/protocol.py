import json
import struct
import typing
from urllib.parse import unquote

import grpc
from google.protobuf import any_pb2, json_format

if typing.TYPE_CHECKING:
    from sonora import _status_pb2 as status_pb2
else:
    from google.rpc import status_pb2
from grpc_status import rpc_status

from sonora.encode import b64decode, b64encode
from sonora.metadata import Metadata

HEADER_FORMAT = ">BI"
HEADER_LENGTH = struct.calcsize(HEADER_FORMAT)


class ProtocolError(Exception):
    pass


class InvalidContentType(ProtocolError):
    pass


class InvalidEncoding(ProtocolError):
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
        status_code = named_status_to_code(code)
    if status_code is None or status_code == grpc.StatusCode.OK:
        status_code = grpc.StatusCode.INTERNAL

    message = error.get("message")
    if message is not None and not isinstance(message, str):
        raise WebRpcError(
            code=grpc.StatusCode.INTERNAL,
            details=f"Invalid error message: {message!r}",
            initial_metadata=Metadata(initial_metadata),
            trailing_metadata=trailing_metadata,
        )

    status = status_pb2.Status(
        code=typing.cast(tuple[int, str], status_code.value)[0],
        message=message or "",
    )
    if "details" in error:
        for detail in error["details"]:
            any = any_pb2.Any(
                type_url=f'type.googleapis.com/{detail["type"]}',
                value=b64decode(detail["value"]),
            )
            status.details.append(any)

    status_message = rpc_status.to_status(status)
    code = status_message.code
    message = status_message.details
    status_trailing_metadata = status_message.trailing_metadata
    if trailing_metadata is None:
        trailing_metadata = status_trailing_metadata
    else:
        trailing_metadata.extend(status_trailing_metadata)
    raise WebRpcError(
        code=code,
        details=message,
        initial_metadata=Metadata(initial_metadata),
        trailing_metadata=trailing_metadata,
    )


def unpack_trailers_connect(response, initial_metadata):
    response = json.loads(response)
    trailing_metadata = None
    if "metadata" in response:
        trailing_metadata = Metadata(response["metadata"])
    if "error" in response:
        error = response["error"]
        unpack_error_connect(
            error,
            initial_metadata,
            trailing_metadata,
            status_code=grpc.StatusCode.UNKNOWN,
        )

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
    200: grpc.StatusCode.OK,
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
_NAME_CODE_MAP = {value: key for key, value in _STATUS_CODE_NAME_MAP.items()}


def status_code_to_http(status_code):
    return _STATUS_CODE_MAP.get(status_code, 500)


def http_status_to_status_code(status: int) -> grpc.StatusCode:
    return _STATUS_CODE_HTTP_MAP.get(status, grpc.StatusCode.UNKNOWN)


def named_status_to_code(name: str) -> grpc.StatusCode:
    return _STATUS_CODE_NAME_MAP.get(name, grpc.StatusCode.UNKNOWN)


def code_to_named_status(code: grpc.StatusCode) -> str:
    return _NAME_CODE_MAP.get(code, "unknown")


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
        details = unquote(details) if details else details

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
