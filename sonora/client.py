import functools
import inspect
import io
from urllib.parse import urljoin

import grpc
import urllib3
import urllib3.exceptions
from urllib3._collections import HTTPHeaderDict

from sonora import protocol


def insecure_web_channel(url, pool_manager_kws=None):
    return WebChannel(url, pool_manager_kws=pool_manager_kws)


class WebChannel:
    def __init__(self, url, pool_manager_kws=None):
        if not url.startswith("http") and "://" not in url:
            url = f"http://{url}"

        if pool_manager_kws is None:
            pool_manager_kws = {}

        self._url = url
        self._session = urllib3.PoolManager(**pool_manager_kws)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.clear()

    def unary_unary(self, path, request_serializer, response_deserializer):
        return UnaryUnaryMulticallable(
            self._session, self._url, path, request_serializer, response_deserializer
        )

    def unary_stream(self, path, request_serializer, response_deserializer):
        return UnaryStreamMulticallable(
            self._session, self._url, path, request_serializer, response_deserializer
        )

    def stream_unary(self, path, request_serializer, response_deserializer):
        return NotImplementedMulticallable()

    def stream_stream(self, path, request_serializer, response_deserializer):
        return NotImplementedMulticallable()


class Multicallable:
    def __init__(self, session, url, path, request_serializer, request_deserializer):
        self._session = session

        self._url = url
        self._path = path
        self._rpc_url = urljoin(url, path[1:])

        self._metadata = [
            ("x-user-agent", "grpc-web-python/0.1"),
            ("content-type", "application/grpc-web+proto"),
        ]

        self._serializer = request_serializer
        self._deserializer = request_deserializer

    def future(self, request):
        raise NotImplementedError()


class NotImplementedMulticallable(Multicallable):
    def __init__(self):
        pass

    def __call__(self, request, timeout=None):
        def nope(*args, **kwargs):
            raise NotImplementedError()

        return nope


class UnaryUnaryMulticallable(Multicallable):
    def __call__(self, request, timeout=None, metadata=None):
        result, _call = self.with_call(request, timeout, metadata)
        return result

    def with_call(self, request, timeout=None, metadata=None):
        call_metadata = self._metadata.copy()
        if metadata is not None:
            call_metadata.extend(protocol.encode_headers(metadata))

        call = UnaryUnaryCall(
            request,
            timeout,
            call_metadata,
            self._rpc_url,
            self._session,
            self._serializer,
            self._deserializer,
        )

        return call(), call


class UnaryStreamMulticallable(Multicallable):
    def __call__(self, request, timeout=None, metadata=None):
        call_metadata = self._metadata.copy()
        if metadata is not None:
            call_metadata.extend(protocol.encode_headers(metadata))

        return UnaryStreamCall(
            request,
            timeout,
            call_metadata,
            self._rpc_url,
            self._session,
            self._serializer,
            self._deserializer,
        )


class Call:
    def __init__(
        self, request, timeout, metadata, url, session, serializer, deserializer
    ):
        self._request = request
        self._timeout = timeout
        self._metadata = metadata
        self._url = url
        self._session = session
        self._serializer = serializer
        self._deserializer = deserializer
        self._response = None
        self._trailers = None

        if timeout is not None:
            self._metadata.append(("grpc-timeout", protocol.serialize_timeout(timeout)))

    def initial_metadata(self):
        return self._response.headers.items()

    def trailing_metadata(self):
        return self._trailers

    @classmethod
    def _raise_timeout(cls, exc):
        def decorator(func):
            if inspect.isasyncgenfunction(func):

                async def wrapper(self, *args, **kwargs):
                    try:
                        async for result in func(self, *args, **kwargs):
                            yield result
                    except exc:
                        raise protocol.WebRpcError(
                            grpc.StatusCode.DEADLINE_EXCEEDED,
                            "request timed out at the client",
                        )

            elif inspect.iscoroutinefunction(func):

                async def wrapper(self, *args, **kwargs):
                    try:
                        return await func(self, *args, **kwargs)
                    except exc:
                        raise protocol.WebRpcError(
                            grpc.StatusCode.DEADLINE_EXCEEDED,
                            "request timed out at the client",
                        )

            elif inspect.isgeneratorfunction(func):

                def wrapper(self, *args, **kwargs):
                    try:
                        result = yield from func(self, *args, **kwargs)
                        return result
                    except exc:
                        raise protocol.WebRpcError(
                            grpc.StatusCode.DEADLINE_EXCEEDED,
                            "request timed out at the client",
                        )

            else:

                def wrapper(self, *args, **kwargs):
                    try:
                        return func(self, *args, **kwargs)
                    except exc:
                        raise protocol.WebRpcError(
                            grpc.StatusCode.DEADLINE_EXCEEDED,
                            "request timed out at the client",
                        )

            return functools.wraps(func)(wrapper)

        return decorator


class UnaryUnaryCall(Call):
    @Call._raise_timeout(urllib3.exceptions.TimeoutError)
    def __call__(self):
        self._response = self._session.request(
            "POST",
            self._url,
            body=protocol.wrap_message(False, False, self._serializer(self._request)),
            headers=HTTPHeaderDict(self._metadata),
            timeout=self._timeout,
        )

        if self._response.status != 200 and "grpc-status" not in self._response.headers:
            raise protocol.WebRpcError(
                protocol.http_status_to_status_code(self._response.status),
                self._response.reason,
                initial_metadata=[
                    (key, value) for key, value in self._response.headers.items()
                ],
            )

        buffer = io.BytesIO(self._response.data)

        messages = protocol.unwrap_message_stream(buffer)
        result = None

        try:
            trailers, compressed, message = next(messages)
        except StopIteration:
            protocol.raise_for_status(self._response.headers)
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
            )

        if trailers:
            self._trailers = protocol.unpack_trailers(message)
        elif compressed:
            raise protocol.WebRpcError(
                grpc.StatusCode.INTERNAL, "Unexpected compression"
            )
        else:
            try:
                result = self._deserializer(message)
            except Exception:
                raise protocol.WebRpcError(
                    grpc.StatusCode.UNIMPLEMENTED, "Could not decode response"
                )

        try:
            trailers, _, message = next(messages)
        except StopIteration:
            pass
        else:
            if trailers:
                self._trailers = protocol.unpack_trailers(message)
            else:
                raise protocol.WebRpcError(
                    grpc.StatusCode.UNIMPLEMENTED,
                    "UnaryUnary should only return a single message",
                )

        protocol.raise_for_status(self._response.headers, self._trailers)

        if result is None:
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
            )

        return result


class UnaryStreamCall(Call):
    @Call._raise_timeout(urllib3.exceptions.TimeoutError)
    def __iter__(self):
        self._response = self._session.request(
            "POST",
            self._url,
            body=protocol.wrap_message(False, False, self._serializer(self._request)),
            headers=dict(self._metadata),
            timeout=self._timeout,
            preload_content=False,
        )
        self._response.auto_close = False

        stream = io.BufferedReader(self._response, buffer_size=16384)

        for trailers, _, message in protocol.unwrap_message_stream(stream):
            if trailers:
                self._trailers = protocol.unpack_trailers(message)
                break
            else:
                yield self._deserializer(message)

        self._response.release_conn()

        protocol.raise_for_status(self._response.headers, self._trailers)

    def __del__(self):
        if self._response and self._response.connection:
            self._response.close()
