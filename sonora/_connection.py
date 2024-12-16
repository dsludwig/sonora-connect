import typing


class ProtocolError(Exception):
    pass


class Proto:
    def __init__(self) -> None:
        self._data_to_send = bytearray()

    def data_to_send(self, amount=None):
        if amount is None:
            data = bytes(self._data_to_send)
            self._data_to_send = bytearray()
            return data
        else:
            data = bytes(self._data_to_send[:amount])
            self._data_to_send = self._data_to_send[amount:]
            return data

    def begin_request(self, method: str, headers: typing.Iterable[tuple[str, str]]):
        pass

    def send_initial_metadata(self):
        pass

    def set_trailing_metadata(self):
        pass


class GrpcProto(Proto):
    pass


class GprcWebProto(Proto):
    pass


class ConnectProto(Proto):
    pass
