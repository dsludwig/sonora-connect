import abc
import typing
import gzip
import zlib

from sonora import protocol


class Encoding:
    @property
    @abc.abstractmethod
    def encoding(self) -> str: ...

    @abc.abstractmethod
    def decode(self, compressed: bool, message: bytes) -> bytes: ...

    @abc.abstractmethod
    def encode(self, message: bytes) -> bytes: ...


class IdentityEncoding(Encoding):
    @property
    def encoding(self):
        return "identity"

    def decode(self, compressed: bool, message: bytes) -> bytes:
        if compressed:
            raise protocol.ProtocolError(
                "Cannot decode compressed message with `identity` encoder"
            )
        return message

    def encode(self, message: bytes) -> bytes:
        return message


class GZipEncoding(Encoding):
    @property
    def encoding(self):
        return "gzip"

    def decode(self, compressed: bool, message: bytes) -> bytes:
        if compressed:
            try:
                return gzip.decompress(message)
            except gzip.BadGzipFile as exc:
                raise protocol.ProtocolError(
                    "Cannot decode invalid compressed message"
                ) from exc
        return message

    def encode(self, message: bytes) -> bytes:
        return gzip.compress(message, compresslevel=7)


class DeflateEncoding(Encoding):
    @property
    def encoding(self):
        return "deflate"

    def decode(self, compressed: bool, message: bytes) -> bytes:
        if compressed:
            try:
                return zlib.decompress(message)
            except zlib.error as exc:
                raise protocol.ProtocolError(
                    "Cannot decode invalid compressed message"
                ) from exc
        return message

    def encode(self, message: bytes) -> bytes:
        return zlib.compress(message)


class InvalidEncoding(Encoding):
    @property
    def encoding(self):
        return "identity"

    def decode(self, compressed, message):
        raise protocol.InvalidEncoding("cannot decode with unsupported encoder")

    def encode(self, message):
        raise protocol.InvalidEncoding("cannot encode with unsupported encoder")


def get_encoding(encoding: typing.Optional[str]) -> Encoding:
    if encoding is None or encoding.lower() == "identity":
        return IdentityEncoding()
    elif encoding.lower() == "gzip":
        return GZipEncoding()
    elif encoding.lower() == "deflate":
        return DeflateEncoding()
    return InvalidEncoding()
