import typing


class StartResponse(typing.NamedTuple):
    status_code: int
    phrase: str
    headers: typing.Iterable[tuple[str, str]]
    trailers: bool = False


class SendBody(typing.NamedTuple):
    body: bytes
    more_body: bool = True


class SendTrailers(typing.NamedTuple):
    trailers: typing.Iterable[tuple[str, str]]


ServerEvents = typing.Iterable[StartResponse | SendBody | SendTrailers]
