class Event:
    """Base class for protocol events"""

    pass


class RequestReceived(Event):
    def __init__(self) -> None:
        self.headers = None


class ResponseReceived(Event):
    def __init__(self) -> None:
        self.headers = None


# class
