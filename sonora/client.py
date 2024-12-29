import functools
import inspect
import io
import itertools
from urllib.parse import urljoin

import grpc
import urllib3
import urllib3.exceptions
from urllib3._collections import HTTPHeaderDict

from sonora import _codec, _encoding, _events, protocol
from sonora.metadata import Metadata


def insecure_web_channel(url, pool_manager_kws=None):
    return WebChannel(url, pool_manager_kws=pool_manager_kws)


def insecure_connect_channel(url, pool_manager_kws=None):
    return WebChannel(url, pool_manager_kws=pool_manager_kws, connect=True)


class WebChannel:
    def __init__(self, url, pool_manager_kws=None, connect=False, json=False):
        if not url.startswith("http") and "://" not in url:
            url = f"http://{url}"

        if pool_manager_kws is None:
            pool_manager_kws = {}

        self._connect = connect
        self._json = json
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
            self._json,
        )

    def unary_stream(self, path, request_serializer, response_deserializer):
        return UnaryStreamMulticallable(
            self._session,
            self._url,
            path,
            request_serializer,
            response_deserializer,
            self._connect,
            self._json,
        )

    def stream_unary(self, path, request_serializer, response_deserializer):
        return StreamUnaryMulticallable(
            self._session,
            self._url,
            path,
            request_serializer,
            response_deserializer,
            self._connect,
            self._json,
        )

    def stream_stream(self, path, request_serializer, response_deserializer):
        return StreamStreamMulticallable(
            self._session,
            self._url,
            path,
            request_serializer,
            response_deserializer,
            self._connect,
            self._json,
        )


class Multicallable:
    def __init__(
        self,
        session,
        url,
        path,
        request_serializer,
        response_deserializer,
        connect,
        json,
    ):
        self._session = session

        self._url = url
        self._path = path
        self._rpc_url = urljoin(url, path[1:])

        self._connect = connect
        self._json = json
        self._codec = self._make_codec(request_serializer, response_deserializer)
        self._metadata = Metadata(
            [
                # ("content-type", self._codec.content_type),
                ("x-user-agent", "grpc-web-python/0.1"),
            ]
        )

        # if connect:
        #     self._metadata = [
        #         ("x-user-agent", "grpc-web-python/0.1"),
        #         ("content-type", "application/connect+proto"),
        #     ]
        #     self._expected_content_types = ["application/connect+proto"]
        # else:
        #     self._metadata = [
        #         ("x-user-agent", "grpc-web-python/0.1"),
        #         ("content-type", "application/grpc-web+proto"),
        #     ]
        #     self._expected_content_types = [
        #         "application/grpc-web+proto",
        #         "application/grpc-web",
        #     ]

        self._serializer = request_serializer
        self._deserializer = response_deserializer
        # if self._connect:
        #     self._wrap_message = protocol.wrap_message_connect
        #     self._unwrap_message_stream = protocol.unwrap_message_stream_connect
        #     self._unpack_trailers = protocol.unpack_trailers_connect
        #     self._raise_for_status = protocol.raise_for_status_connect
        # else:
        #     self._wrap_message = protocol.wrap_message
        #     self._unwrap_message_stream = protocol.unwrap_message_stream
        #     self._unpack_trailers = protocol.unpack_trailers
        #     self._raise_for_status = protocol.raise_for_status

    def _make_codec(self, request_serializer, response_deserializer):
        if self._connect:
            codec_class = (
                _codec.ConnectUnaryJsonCodec
                if self._json
                else _codec.ConnectUnaryProtoCodec
            )
        else:
            codec_class = (
                _codec.GrpcWebJsonCodec if self._json else _codec.GrpcWebProtoCodec
            )
        serializer_class = (
            _codec.JsonSerializer if self._json else _codec.ProtoSerializer
        )
        encoding = _encoding.IdentityEncoding()
        serializer = serializer_class(
            request_serializer=request_serializer,
            response_deserializer=response_deserializer,
        )
        return codec_class(encoding, serializer)

    def future(self, request):
        raise NotImplementedError()


class UnaryUnaryMulticallable(Multicallable):
    # def __init__(
    #     self, session, url, path, request_serializer, request_deserializer, connect, json
    # ):
    #     super().__init__(
    #         session, url, path, request_serializer, request_deserializer, connect
    #     )
    # if self._connect:
    #     self._wrap_message = protocol.bare_wrap_message
    #     self._unwrap_message_stream = protocol.bare_unwrap_message_stream
    #     self._metadata = [
    #         ("x-user-agent", "grpc-web-python/0.1"),
    #         ("content-type", "application/proto"),
    #     ]
    #     self._expected_content_types = ["application/proto"]

    def __call__(self, request, timeout=None, metadata=None):
        result, _call = self.with_call(request, timeout, metadata)
        return result

    def with_call(self, request, timeout=None, metadata=None):
        call_metadata = self._metadata.copy()
        if metadata is not None:
            call_metadata.extend((metadata))

        call = UnaryUnaryCall(
            request,
            timeout,
            call_metadata,
            self._rpc_url,
            self._session,
            self._codec,
            # self._deserializer,
            # self._wrap_message,
            # self._unwrap_message_stream,
            # self._unpack_trailers,
            # self._raise_for_status,
            # self._connect,
            # self._expected_content_types,
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
        codec: _codec.Codec,
        # serializer,
        # deserializer,
        # wrap_message,
        # unwrap_message_stream,
        # unpack_trailers,
        # raise_for_status,
        # connect,
        # expected_content_types,
    ):
        self._request = request
        self._timeout = timeout
        self._metadata = metadata
        self._url = url
        self._session = session
        self._codec = codec
        # self._serializer = serializer
        # self._deserializer = deserializer
        # self._wrap_message = wrap_message
        # self._unwrap_message_stream = unwrap_message_stream
        # self._unpack_trailers = unpack_trailers
        # self._raise_for_status = raise_for_status
        # self._expected_content_types = expected_content_types
        self._response = None
        self._trailers = None
        self._message = None
        # self._body = body_generator()
        # self._body.send(None)
        # self._connect = connect

        # if timeout is not None:
        #     if self._connect:
        #         self._metadata.append(("connect-timeout-ms", str(int(timeout * 1000))))
        #     else:
        #         self._metadata.append(
        #             ("grpc-timeout", protocol.serialize_timeout(timeout))
        #         )

    def _do_events(self, events: _events.ClientEvents):
        for event in events:
            if isinstance(event, _events.StartRequest):
                self._response = self._session.request(
                    event.method,
                    self._url,
                    body=self._body,
                    headers=HTTPHeaderDict(event.headers),
                    timeout=self._timeout,
                )
            elif isinstance(event, _events.SendBody):
                pass
            elif isinstance(event, _events.ReceiveMessage):
                self._message = event.message
            else:
                raise ValueError("Unexpected codec event")

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


def body_generator(events: _events.ClientEvents):
    for event in events:
        if isinstance(event, _events.SendBody):
            yield event.body
            if not event.more_body:
                break


class UnaryUnaryCall(Call):
    @Call._raise_timeout(urllib3.exceptions.TimeoutError)
    def __call__(self):
        def do_call():
            self._codec.set_invocation_metadata(self._metadata)
            self._codec.set_timeout(self._timeout)

            yield from self._codec.send_request(self._request)
            yield from self._codec.start_request()
            yield from self._codec.start_response(
                _events.StartResponse(
                    status_code=self._response.status,
                    phrase=self._response.reason,
                    headers=HTTPHeaderDict(self._response.headers),
                )
            )
            yield from self._codec.receive_body(self._response.data)

            # TODO: event?
            self._trailers = self._codec._trailing_metadata

        body_events, events = itertools.tee(do_call())
        self._body = body_generator(body_events)
        self._do_events(events)
        return self._message


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
