import functools
import inspect
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
                _codec.ConnectStreamJsonCodec
                if self._json
                else _codec.ConnectStreamProtoCodec
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

    def _make_codec(self, request_serializer, response_deserializer):
        if self._connect:
            codec_class = (
                _codec.ConnectUnaryJsonCodec
                if self._json
                else _codec.ConnectUnaryProtoCodec
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
        else:
            return super()._make_codec(request_serializer, response_deserializer)

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
            self._codec,
            # self._serializer,
            # self._deserializer,
            # self._wrap_message,
            # self._unwrap_message_stream,
            # self._unpack_trailers,
            # self._raise_for_status,
            # self._connect,
            # self._expected_content_types,
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
            self._codec,
            # self._serializer,
            # self._deserializer,
            # self._wrap_message,
            # self._unwrap_message_stream,
            # self._unpack_trailers,
            # self._raise_for_status,
            # self._connect,
            # self._expected_content_types,
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
            self._codec,
            # self._serializer,
            # self._deserializer,
            # self._wrap_message,
            # self._unwrap_message_stream,
            # self._unpack_trailers,
            # self._raise_for_status,
            # self._connect,
            # self._expected_content_types,
        )


class Call:
    response_streaming: bool
    request_streaming: bool

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

    def _do_event(self, event):
        if isinstance(event, _events.StartRequest):
            self._response = self._session.request(
                event.method,
                self._url,
                body=self._body,
                headers=HTTPHeaderDict(event.headers),
                timeout=self._timeout,
                preload_content=not self.response_streaming,
                chunked=self.request_streaming or self.response_streaming,
            )
        elif isinstance(event, _events.SendBody):
            pass
        elif isinstance(event, _events.ReceiveMessage):
            self._message = event.message
        else:
            raise ValueError("Unexpected codec event")

    def _do_events(self, events: _events.ClientEvents):
        for event in events:
            self._do_event(event)
            # import sys

            # print(event, file=sys.stderr)
            # if isinstance(event, _events.StartRequest):
            #     self._response = self._session.request(
            #         event.method,
            #         self._url,
            #         body=self._body,
            #         headers=HTTPHeaderDict(event.headers),
            #         timeout=self._timeout,
            #     )
            # elif isinstance(event, _events.SendBody):
            #     pass
            # elif isinstance(event, _events.ReceiveMessage):
            #     self._message = event.message
            # else:
            #     raise ValueError("Unexpected codec event")

            # yield event

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
    request_streaming = False
    response_streaming = False

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
    request_streaming = False
    response_streaming = True

    @Call._raise_timeout(urllib3.exceptions.TimeoutError)
    def __iter__(self):
        self._body = body_generator(self._codec.send_request(self._request))

        def do_call():
            self._codec.set_invocation_metadata(self._metadata)
            self._codec.set_timeout(self._timeout)

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

        try:
            for event in do_call():
                if isinstance(event, _events.ReceiveMessage):
                    yield event.message
                else:
                    self._do_event(event)
        except protocol.ProtocolError:
            raise protocol.WebRpcError(
                grpc.StatusCode.INTERNAL, "Unexpected compression"
            )
        finally:
            self._response.release_conn()

    def __del__(self):
        if self._response and self._response.connection:
            self._response.close()


class StreamUnaryCall(Call):
    request_streaming = True
    response_streaming = False

    def _do_event(self, event):
        if isinstance(event, _events.ReceiveMessage):
            if self._message is None:
                self._message = event.message
            else:
                raise protocol.WebRpcError(
                    grpc.StatusCode.UNIMPLEMENTED,
                    "Received multiple responses for a stream-unary call",
                )
        else:
            super()._do_event(event)

    @Call._raise_timeout(urllib3.exceptions.TimeoutError)
    def __call__(self):
        self._body = body_generator(
            e for request in self._request for e in self._codec.send_request(request)
        )

        def do_call():
            self._codec.set_invocation_metadata(self._metadata)
            self._codec.set_timeout(self._timeout)

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

        self._do_events(do_call())
        if self._message is None:
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for stream-unary call",
                initial_metadata=self._codec._initial_metadata,
                trailing_metadata=self._codec._trailing_metadata,
            )
        return self._message

    def __del__(self):
        if self._response and self._response.connection:
            self._response.close()


class StreamStreamCall(Call):
    request_streaming = True
    response_streaming = True

    @Call._raise_timeout(urllib3.exceptions.TimeoutError)
    def __iter__(self):
        self._body = body_generator(
            itertools.chain.from_iterable(
                self._codec.send_request(request) for request in self._request
            )
        )

        def do_call():
            self._codec.set_invocation_metadata(self._metadata)
            self._codec.set_timeout(self._timeout)

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

        try:
            for event in do_call():
                if isinstance(event, _events.ReceiveMessage):
                    yield event.message
                else:
                    self._do_event(event)
        except protocol.ProtocolError:
            raise protocol.WebRpcError(
                grpc.StatusCode.INTERNAL, "Unexpected compression"
            )
        finally:
            self._response.release_conn()

    def __del__(self):
        if self._response and self._response.connection:
            self._response.close()
