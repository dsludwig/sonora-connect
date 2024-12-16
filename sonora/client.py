import functools
import inspect
import io
import json
from urllib.parse import urljoin

import grpc
import urllib3
import urllib3.exceptions
from urllib3._collections import HTTPHeaderDict

from sonora import protocol


def insecure_web_channel(url, pool_manager_kws=None):
    return WebChannel(url, pool_manager_kws=pool_manager_kws)


def insecure_connect_channel(url, pool_manager_kws=None):
    return WebChannel(url, pool_manager_kws=pool_manager_kws, connect=True)


class WebChannel:
    def __init__(self, url, pool_manager_kws=None, connect=False):
        if not url.startswith("http") and "://" not in url:
            url = f"http://{url}"

        if pool_manager_kws is None:
            pool_manager_kws = {}

        self._connect = connect
        self._url = url
        self._session = urllib3.PoolManager(**pool_manager_kws)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.clear()

    def unary_unary(self, path, request_serializer, response_deserializer):
        return UnaryUnaryMulticallable(
            self._session,
            self._url,
            path,
            request_serializer,
            response_deserializer,
            self._connect,
        )

    def unary_stream(self, path, request_serializer, response_deserializer):
        return UnaryStreamMulticallable(
            self._session,
            self._url,
            path,
            request_serializer,
            response_deserializer,
            self._connect,
        )

    def stream_unary(self, path, request_serializer, response_deserializer):
        return StreamUnaryMulticallable(
            self._session,
            self._url,
            path,
            request_serializer,
            response_deserializer,
            self._connect,
        )

    def stream_stream(self, path, request_serializer, response_deserializer):
        return StreamStreamMulticallable(
            self._session,
            self._url,
            path,
            request_serializer,
            response_deserializer,
            self._connect,
        )


class Multicallable:
    def __init__(
        self, session, url, path, request_serializer, request_deserializer, connect
    ):
        self._session = session

        self._url = url
        self._path = path
        self._rpc_url = urljoin(url, path[1:])

        self._connect = connect
        if connect:
            self._metadata = [
                ("x-user-agent", "grpc-web-python/0.1"),
                ("content-type", "application/connect+proto"),
            ]
            self._expected_content_types = ["application/connect+proto"]
        else:
            self._metadata = [
                ("x-user-agent", "grpc-web-python/0.1"),
                ("content-type", "application/grpc-web+proto"),
            ]
            self._expected_content_types = [
                "application/grpc-web+proto",
                "application/grpc-web",
            ]

        self._serializer = request_serializer
        self._deserializer = request_deserializer
        if self._connect:
            self._wrap_message = protocol.wrap_message_connect
            self._unwrap_message_stream = protocol.unwrap_message_stream_connect
            self._unpack_trailers = protocol.unpack_trailers_connect
            self._raise_for_status = protocol.raise_for_status_connect
        else:
            self._wrap_message = protocol.wrap_message
            self._unwrap_message_stream = protocol.unwrap_message_stream
            self._unpack_trailers = protocol.unpack_trailers
            self._raise_for_status = protocol.raise_for_status

    def future(self, request):
        raise NotImplementedError()


class UnaryUnaryMulticallable(Multicallable):
    def __init__(
        self, session, url, path, request_serializer, request_deserializer, connect
    ):
        super().__init__(
            session, url, path, request_serializer, request_deserializer, connect
        )
        if self._connect:
            self._wrap_message = protocol.bare_wrap_message
            self._unwrap_message_stream = protocol.bare_unwrap_message_stream
            self._metadata = [
                ("x-user-agent", "grpc-web-python/0.1"),
                ("content-type", "application/proto"),
            ]
            self._expected_content_types = ["application/proto"]

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
            self._wrap_message,
            self._unwrap_message_stream,
            self._unpack_trailers,
            self._raise_for_status,
            self._connect,
            self._expected_content_types,
        )

        return call(), call


class UnaryStreamMulticallable(Multicallable):
    # def __init__(
    #     self, session, url, path, request_serializer, request_deserializer, connect
    # ):
    #     super().__init__(
    #         session, url, path, request_serializer, request_deserializer, connect
    #     )
    #     if self._connect:
    #         self._wrap_message = protocol.bare_wrap_message
    #         self._unwrap_message_stream = protocol.bare_unwrap_message_stream
    #         self._metadata = [
    #             ("x-user-agent", "grpc-web-python/0.1"),
    #             ("content-type", "application/connect+proto"),
    #         ]

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
            self._wrap_message,
            self._unwrap_message_stream,
            self._unpack_trailers,
            self._raise_for_status,
            self._connect,
            self._expected_content_types,
        )


class StreamUnaryMulticallable(Multicallable):
    # def __init__(
    #     self, session, url, path, request_serializer, request_deserializer, connect
    # ):
    #     super().__init__(
    #         session, url, path, request_serializer, request_deserializer, connect
    #     )
    #     if self._connect:
    #         self._unwrap_message_stream = protocol.bare_unwrap_message_stream
    #         self._metadata = [
    #             ("x-user-agent", "grpc-web-python/0.1"),
    #             ("content-type", "application/connect+proto"),
    #         ]

    def __call__(self, request_iterator, timeout=None, metadata=None):
        resp, _call = self.with_call(request_iterator, timeout, metadata)
        return resp

    def with_call(self, request_iterator, timeout=None, metadata=None):
        call_metadata = self._metadata.copy()
        if metadata is not None:
            call_metadata.extend(protocol.encode_headers(metadata))

        call = StreamUnaryCall(
            request_iterator,
            timeout,
            call_metadata,
            self._rpc_url,
            self._session,
            self._serializer,
            self._deserializer,
            self._wrap_message,
            self._unwrap_message_stream,
            self._unpack_trailers,
            self._raise_for_status,
            self._connect,
            self._expected_content_types,
        )
        return call(), call


class StreamStreamMulticallable(Multicallable):
    # def __init__(self, session, url, path, request_serializer, request_deserializer, connect):
    #     super().__init__(session, url, path, request_serializer, request_deserializer, connect)
    #     if self._connect:
    #         self._metadata = [
    #             ("x-user-agent", "grpc-web-python/0.1"),
    #             ("content-type", "application/connect+proto"),
    #         ]

    def __call__(self, request_iterator, timeout=None, metadata=None):
        call_metadata = self._metadata.copy()
        if metadata is not None:
            call_metadata.extend(protocol.encode_headers(metadata))

        return StreamStreamCall(
            request_iterator,
            timeout,
            call_metadata,
            self._rpc_url,
            self._session,
            self._serializer,
            self._deserializer,
            self._wrap_message,
            self._unwrap_message_stream,
            self._unpack_trailers,
            self._raise_for_status,
            self._connect,
            self._expected_content_types,
        )


class Call:
    def __init__(
        self,
        request,
        timeout,
        metadata,
        url,
        session,
        serializer,
        deserializer,
        wrap_message,
        unwrap_message_stream,
        unpack_trailers,
        raise_for_status,
        connect,
        expected_content_types,
    ):
        self._request = request
        self._timeout = timeout
        self._metadata = metadata
        self._url = url
        self._session = session
        self._serializer = serializer
        self._deserializer = deserializer
        self._wrap_message = wrap_message
        self._unwrap_message_stream = unwrap_message_stream
        self._unpack_trailers = unpack_trailers
        self._raise_for_status = raise_for_status
        self._expected_content_types = expected_content_types
        self._response = None
        self._trailers = None
        self._connect = connect

        if timeout is not None:
            if self._connect:
                self._metadata.append(("connect-timeout-ms", str(int(timeout * 1000))))
            else:
                self._metadata.append(
                    ("grpc-timeout", protocol.serialize_timeout(timeout))
                )

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
            body=self._wrap_message(False, False, self._serializer(self._request)),
            headers=HTTPHeaderDict(self._metadata),
            timeout=self._timeout,
        )

        if self._response.status != 200 and "grpc-status" not in self._response.headers:
            if self._connect:
                initial_metadata, trailing_metadata = protocol.split_trailers(
                    protocol.Metadata(self._response.headers.items())
                )
                try:
                    data = json.loads(self._response.data)
                    protocol.unpack_error_connect(
                        data,
                        initial_metadata,
                        trailing_metadata,
                        status_code=protocol.http_status_to_status_code(
                            self._response.status
                        ),
                    )
                except ValueError:
                    pass
            raise protocol.WebRpcError(
                protocol.http_status_to_status_code(self._response.status),
                self._response.reason,
                initial_metadata=[
                    (key, value) for key, value in self._response.headers.items()
                ],
            )

        buffer = io.BytesIO(self._response.data)

        messages = self._unwrap_message_stream(buffer)
        result = None

        try:
            trailers, compressed, message = next(messages)
        except StopIteration:
            protocol.raise_for_status(
                self._response.headers,
                expected_content_types=self._expected_content_types,
            )
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
            )

        if trailers:
            self._trailers = self._unpack_trailers(message, self.initial_metadata())
        elif compressed:
            raise protocol.WebRpcError(
                grpc.StatusCode.INTERNAL, "Unexpected compression"
            )
        else:
            try:
                result = self._deserializer(message)
            except Exception:
                raise protocol.WebRpcError(
                    grpc.StatusCode.INTERNAL, "Could not decode response"
                )

        try:
            trailers, _, message = next(messages)
        except StopIteration:
            pass
        else:
            if trailers:
                self._trailers = self._unpack_trailers(message, self.initial_metadata())
            else:
                raise protocol.WebRpcError(
                    grpc.StatusCode.UNIMPLEMENTED,
                    "UnaryUnary should only return a single message",
                )

        self._raise_for_status(
            self._response.headers, self._trailers, self._expected_content_types
        )

        if result is None:
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
            )

        if self._connect:
            _, self._trailers = protocol.split_trailers(
                protocol.Metadata(self._response.headers.items())
            )

        return result


class UnaryStreamCall(Call):
    @Call._raise_timeout(urllib3.exceptions.TimeoutError)
    def __iter__(self):
        self._response = self._session.request(
            "POST",
            self._url,
            body=self._wrap_message(False, False, self._serializer(self._request)),
            headers=HTTPHeaderDict(self._metadata),
            timeout=self._timeout,
            preload_content=False,
        )
        self._response.auto_close = False

        stream = io.BufferedReader(self._response, buffer_size=16384)

        for trailers, compressed, message in self._unwrap_message_stream(stream):
            if compressed:
                raise protocol.WebRpcError(
                    grpc.StatusCode.INTERNAL, "Unexpected compression"
                )
            if trailers:
                self._trailers = self._unpack_trailers(message, self.initial_metadata())
                break
            else:
                try:
                    yield self._deserializer(message)
                except Exception:
                    raise protocol.WebRpcError(
                        grpc.StatusCode.INTERNAL, "Could not decode response"
                    )

        self._response.release_conn()

        self._raise_for_status(
            self._response.headers, self._trailers, self._expected_content_types
        )

    def __del__(self):
        if self._response and self._response.connection:
            self._response.close()


class StreamUnaryCall(Call):
    @Call._raise_timeout(urllib3.exceptions.TimeoutError)
    def __call__(self):
        self._response = self._session.request(
            "POST",
            self._url,
            body=(
                self._wrap_message(False, False, self._serializer(req))
                for req in self._request
            ),
            headers=HTTPHeaderDict(self._metadata),
            timeout=self._timeout,
            chunked=True,
        )

        if self._response.status != 200 and "grpc-status" not in self._response.headers:
            if self._connect:
                initial_metadata, trailing_metadata = protocol.split_trailers(
                    self._response.headers.items()
                )
                data = self._response.json()
                protocol.unpack_error_connect(data, initial_metadata, trailing_metadata)
            raise protocol.WebRpcError(
                protocol.http_status_to_status_code(self._response.status),
                self._response.reason,
                initial_metadata=[
                    (key, value) for key, value in self._response.headers.items()
                ],
            )

        buffer = io.BytesIO(self._response.data)

        messages = self._unwrap_message_stream(buffer)
        result = None

        try:
            trailers, compressed, message = next(messages)
        except StopIteration:
            self._raise_for_status(
                self._response.headers,
                expected_content_types=self._expected_content_types,
            )
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
            )

        if trailers:
            self._trailers = self._unpack_trailers(message, self.initial_metadata())
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
                self._trailers = self._unpack_trailers(message, self.initial_metadata())
            else:
                raise protocol.WebRpcError(
                    grpc.StatusCode.UNIMPLEMENTED,
                    "StreamUnary should only return a single message",
                )

        self._raise_for_status(
            self._response.headers, self._trailers, self._expected_content_types
        )

        if result is None:
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
            )

        return result

    def __del__(self):
        if self._response and self._response.connection:
            self._response.close()


class StreamStreamCall(Call):
    @Call._raise_timeout(urllib3.exceptions.TimeoutError)
    def __iter__(self):
        self._response = self._session.request(
            "POST",
            self._url,
            body=(
                self._wrap_message(False, False, self._serializer(req))
                for req in self._request
            ),
            headers=HTTPHeaderDict(self._metadata),
            timeout=self._timeout,
            preload_content=False,
            chunked=True,
        )
        self._response.auto_close = False

        stream = io.BufferedReader(self._response, buffer_size=16384)

        for trailers, _, message in self._unwrap_message_stream(stream):
            if trailers:
                self._trailers = self._unpack_trailers(message, self.initial_metadata())
                break
            else:
                yield self._deserializer(message)

        self._response.release_conn()

        self._raise_for_status(
            self._response.headers, self._trailers, self._expected_content_types
        )

    def __del__(self):
        if self._response and self._response.connection:
            self._response.close()
