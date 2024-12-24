import abc

from sonora import protocol


class Encoding:
    @abc.abstractmethod
    def decode(self, compressed: bool, message: bytes) -> bool: ...

    @abc.abstractmethod
    def encode(self, message: bytes) -> bytes: ...


class IdentityEncoding(Encoding):
    def decode(self, compressed: bool, message: bytes) -> bytes:
        if compressed:
            raise protocol.ProtocolError(
                "Cannot decode compressed message with `identity` encoder"
            )
        return message

    def encode(self, message: bytes) -> bytes:
        return message


class InvalidEncoding(Encoding):
    def decode(self, compressed, message):
        raise protocol.InvalidEncoding("cannot decode with unsupported encoder")

    def encode(self, message):
        raise protocol.InvalidEncoding("cannot encode with unsupported encoder")


def get_encoding(encoding: str | None) -> Encoding:
    if encoding is None or encoding.lower() == "identity":
        return IdentityEncoding()
    return InvalidEncoding()
