import asyncio
import io
import json

import aiohttp
import grpc.experimental.aio

import sonora.client
from sonora import protocol


def insecure_web_channel(url, session_kws=None):
    return WebChannel(url, session_kws)


def insecure_connect_channel(url, session_kws=None):
    return WebChannel(url, session_kws, connect=True)


class WebChannel:
    def __init__(self, url, session_kws=None, connect=False):
        if not url.startswith("http") and "://" not in url:
            url = f"http://{url}"

        self._url = url
        if session_kws is None:
            session_kws = {}

        self._session = aiohttp.ClientSession(**session_kws)
        self._connect = connect

    async def __aenter__(self):
        return self

    async def __aexit__(self, exception_type, exception_value, traceback):
        await self._session.close()

    def __await__(self):
        yield self

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


class UnaryUnaryMulticallable(sonora.client.Multicallable):
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
        call_metadata = self._metadata.copy()
        if metadata is not None:
            call_metadata.extend(protocol.encode_headers(metadata))

        return UnaryUnaryCall(
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


class UnaryStreamMulticallable(sonora.client.Multicallable):
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


class StreamUnaryMulticallable(sonora.client.Multicallable):
    def __call__(self, request, timeout=None, metadata=None):
        call_metadata = self._metadata.copy()
        if metadata is not None:
            call_metadata.extend(protocol.encode_headers(metadata))

        return StreamUnaryCall(
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


class StreamStreamMulticallable(sonora.client.Multicallable):
    def __call__(self, request, timeout=None, metadata=None):
        call_metadata = self._metadata.copy()
        if metadata is not None:
            call_metadata.extend(protocol.encode_headers(metadata))

        return StreamStreamCall(
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


class Call(sonora.client.Call):
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self._response and not self._response.closed:
            self._response.close()

    def __del__(self):
        if self._response and not self._response.closed:
            self._response.close()

    async def _get_response(self):
        if self._response is None:
            timeout = aiohttp.ClientTimeout(total=self._timeout)

            self._response = await self._session.post(
                self._url,
                data=self._wrap_message(False, False, self._serializer(self._request)),
                headers=self._metadata,
                timeout=timeout,
            )

            # XXX
            # protocol.raise_for_status(self._response.headers)

        return self._response

    async def initial_metadata(self):
        response = await self._get_response()
        return response.headers.items()

    async def trailing_metadata(self):
        return self._trailers


class UnaryUnaryCall(Call):
    @Call._raise_timeout(asyncio.TimeoutError)
    def __await__(self):
        response = yield from self._get_response().__await__()

        data = yield from response.read().__await__()

        response.release()

        if self._response.status != 200 and "grpc-status" not in self._response.headers:
            if self._connect:
                initial_metadata, trailing_metadata = protocol.split_trailers(
                    protocol.Metadata(self._response.headers.items())
                )
                try:
                    data = json.loads(data)
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

        if not data:
            self._raise_for_status(
                response.headers, expected_content_types=self._expected_content_types
            )

        buffer = io.BytesIO(data)

        messages = self._unwrap_message_stream(buffer)
        result = None

        try:
            trailers, compressed, message = next(messages)
        except StopIteration:
            self._raise_for_status(
                self._response.headers, {}, self._expected_content_types
            )
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
            )

        if trailers:
            self._trailers = self._unpack_trailers(message, response.headers)
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
                self._trailers = self._unpack_trailers(message, response.headers)
            else:
                raise protocol.WebRpcError(
                    grpc.StatusCode.UNIMPLEMENTED,
                    "StreamUnary should only return a single message",
                )

        self._raise_for_status(
            response.headers, self._trailers, self._expected_content_types
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
        super().__init__(
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
        )
        if connect:
            self._unwrap_message_stream_async = (
                protocol.unwrap_message_stream_async_connect
            )
        else:
            self._unwrap_message_stream_async = protocol.unwrap_message_stream_async

    @Call._raise_timeout(asyncio.TimeoutError)
    async def read(self):
        response = await self._get_response()

        async for trailers, compressed, message in self._unwrap_message_stream_async(
            response.content
        ):
            if trailers:
                self._trailers = self._unpack_trailers(message, response.headers)
                break
            elif compressed:
                raise protocol.WebRpcError(
                    grpc.StatusCode.INTERNAL, "Unexpected compression"
                )
            else:
                return self._deserializer(message)

        response.release()

        self._raise_for_status(
            response.headers, self._trailers, self._expected_content_types
        )

        return grpc.experimental.aio.EOF

    @Call._raise_timeout(asyncio.TimeoutError)
    async def __aiter__(self):
        response = await self._get_response()

        async for trailers, compressed, message in self._unwrap_message_stream_async(
            response.content
        ):
            if trailers:
                self._trailers = self._unpack_trailers(message, response.headers)
                break
            elif compressed:
                raise protocol.WebRpcError(
                    grpc.StatusCode.INTERNAL, "Unexpected compression"
                )
            else:
                try:
                    yield self._deserializer(message)
                except Exception:
                    raise protocol.WebRpcError(
                        grpc.StatusCode.INTERNAL, "Could not decode response"
                    )

        response.release()

        self._raise_for_status(
            response.headers, self._trailers, self._expected_content_types
        )


class StreamingRequestCall(Call):
    async def _request_stream(self):
        for req in self._request:
            yield protocol.wrap_message(False, False, self._serializer(req))

    async def _get_response(self):
        if self._response is None:
            timeout = aiohttp.ClientTimeout(total=self._timeout)

            self._response = await self._session.post(
                self._url,
                data=self._request_stream(),
                headers=self._metadata,
                timeout=timeout,
                chunked=True,
            )

            # XXX
            # protocol.raise_for_status(self._response.headers)

        return self._response


class StreamUnaryCall(StreamingRequestCall):
    @Call._raise_timeout(asyncio.TimeoutError)
    def __await__(self):
        response = yield from self._get_response().__await__()

        data = yield from response.read().__await__()

        response.release()

        if self._response.status != 200 and "grpc-status" not in self._response.headers:
            raise protocol.WebRpcError(
                protocol.http_status_to_status_code(self._response.status),
                self._response.reason,
                initial_metadata=[
                    (key, value) for key, value in self._response.headers.items()
                ],
            )

        if not data:
            self._raise_for_status(
                response.headers, expected_content_types=self._expected_content_types
            )

        buffer = io.BytesIO(data)

        messages = self._unwrap_message_stream(buffer)
        result = None

        try:
            trailers, compressed, message = next(messages)
        except StopIteration:
            self._raise_for_status(
                self._response.headers, {}, self._expected_content_types
            )
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
            )

        if trailers:
            self._trailers = self._unpack_trailers(message, response.headers)
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
                self._trailers = self._unpack_trailers(message, response.headers)
            else:
                raise protocol.WebRpcError(
                    grpc.StatusCode.UNIMPLEMENTED,
                    "UnaryUnary should only return a single message",
                )

        self._raise_for_status(
            response.headers, self._trailers, self._expected_content_types
        )
        if result is None:
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
            )

        return result


class StreamStreamCall(StreamingRequestCall):
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
        super().__init__(
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
        )
        if connect:
            self._unwrap_message_stream_async = (
                protocol.unwrap_message_stream_async_connect
            )
        else:
            self._unwrap_message_stream_async = protocol.unwrap_message_stream_async

    @Call._raise_timeout(asyncio.TimeoutError)
    async def read(self):
        response = await self._get_response()

        async for trailers, compressed, message in self._unwrap_message_stream_async(
            response.content
        ):
            if trailers:
                self._trailers = self._unpack_trailers(message, response.headers)
                break
            elif compressed:
                raise protocol.WebRpcError(
                    grpc.StatusCode.INTERNAL, "Unexpected compression"
                )
            else:
                try:
                    return self._deserializer(message)
                except Exception:
                    raise protocol.WebRpcError(
                        grpc.StatusCode.INTERNAL, "Could not decode response"
                    )

        response.release()

        self._raise_for_status(
            response.headers, self._trailers, self._expected_content_types
        )

        return grpc.experimental.aio.EOF

    @Call._raise_timeout(asyncio.TimeoutError)
    async def __aiter__(self):
        response = await self._get_response()

        async for trailers, compressed, message in self._unwrap_message_stream_async(
            response.content
        ):
            if trailers:
                self._trailers = self._unpack_trailers(message, response.headers)
                break
            elif compressed:
                raise protocol.WebRpcError(
                    grpc.StatusCode.INTERNAL, "Unexpected compression"
                )
            else:
                try:
                    yield self._deserializer(message)
                except Exception:
                    raise protocol.WebRpcError(
                        grpc.StatusCode.INTERNAL, "Could not decode response"
                    )

        response.release()

        self._raise_for_status(
            response.headers, self._trailers, self._expected_content_types
        )
