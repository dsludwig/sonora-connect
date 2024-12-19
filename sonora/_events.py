import typing


class StartResponse(typing.NamedTuple):
    status_code: int
    phrase: str
    headers: typing.Iterable[tuple[bytes, bytes]]


class SendBody(typing.NamedTuple):
    body: bytes


class SendTrailers(typing.NamedTuple):
    trailers: typing.Iterable[tuple[bytes, bytes]]


ServerEvents = typing.Iterable[StartResponse | SendBody | SendTrailers]
