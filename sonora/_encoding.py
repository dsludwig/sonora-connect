import abc

from sonora import protocol


class Encoding:
    @property
    @abc.abstractmethod
    def encoding(self) -> str:
        ...

    @abc.abstractmethod
    def decode(self, compressed: bool, message: bytes) -> bytes:
        ...

    @abc.abstractmethod
    def encode(self, message: bytes) -> bytes:
        ...


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


class InvalidEncoding(Encoding):
    @property
    def encoding(self):
        return "<invalid>"

    def decode(self, compressed, message):
        raise protocol.InvalidEncoding("cannot decode with unsupported encoder")

    def encode(self, message):
        raise protocol.InvalidEncoding("cannot encode with unsupported encoder")


def get_encoding(encoding: str | None) -> Encoding:
    if encoding is None or encoding.lower() == "identity":
        return IdentityEncoding()
    return InvalidEncoding()
