import typing


class StartResponse(typing.NamedTuple):
    status_code: int
    phrase: str
    headers: typing.Iterable[typing.Tuple[str, str]]
    trailers: bool = False


class SendBody(typing.NamedTuple):
    body: bytes
    more_body: bool = True


class SendTrailers(typing.NamedTuple):
    trailers: typing.Iterable[typing.Tuple[str, str]]


class StartRequest(typing.NamedTuple):
    method: str
    headers: typing.Iterable[typing.Tuple[str, str]]


class ReceiveInitialMetadata(typing.NamedTuple):
    headers: typing.Iterable[typing.Tuple[str, typing.Union[str, bytes]]]


class ReceiveTrailingMetadata(typing.NamedTuple):
    headers: typing.Iterable[typing.Tuple[str, typing.Union[str, bytes]]]


class ReceiveMessage(typing.NamedTuple):
    message: typing.Any


ClientEvents = typing.Iterable[
    typing.Union[
        StartRequest,
        SendBody,
        ReceiveInitialMetadata,
        ReceiveMessage,
        ReceiveTrailingMetadata,
    ]
]
ServerEvents = typing.Iterable[typing.Union[StartResponse, SendBody, SendTrailers]]
