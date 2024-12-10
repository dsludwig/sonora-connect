import asyncio
import io

import aiohttp
import grpc.experimental.aio

import sonora.client
from sonora import protocol


def insecure_web_channel(url, session_kws=None):
    return WebChannel(url, session_kws)


class WebChannel:
    def __init__(self, url, session_kws=None):
        if not url.startswith("http") and "://" not in url:
            url = f"http://{url}"

        self._url = url
        if session_kws is None:
            session_kws = {}

        self._session = aiohttp.ClientSession(**session_kws)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exception_type, exception_value, traceback):
        await self._session.close()

    def __await__(self):
        yield self

    def unary_unary(self, path, request_serializer, response_deserializer):
        return UnaryUnaryMulticallable(
            self._session, self._url, path, request_serializer, response_deserializer
        )

    def unary_stream(self, path, request_serializer, response_deserializer):
        return UnaryStreamMulticallable(
            self._session, self._url, path, request_serializer, response_deserializer
        )

    def stream_unary(self, path, request_serializer, response_deserializer):
        return StreamUnaryMulticallable(
            self._session, self._url, path, request_serializer, response_deserializer
        )

    def stream_stream(self, path, request_serializer, response_deserializer):
        return StreamStreamMulticallable(
            self._session, self._url, path, request_serializer, response_deserializer
        )


class UnaryUnaryMulticallable(sonora.client.Multicallable):
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
                data=protocol.wrap_message(
                    False, False, self._serializer(self._request)
                ),
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
            raise protocol.WebRpcError(
                protocol.http_status_to_status_code(self._response.status),
                self._response.reason,
                initial_metadata=[
                    (key, value) for key, value in self._response.headers.items()
                ],
            )

        if not data:
            protocol.raise_for_status(response.headers)

        buffer = io.BytesIO(data)

        messages = protocol.unwrap_message_stream(buffer)
        result = None

        try:
            trailers, compressed, message = next(messages)
        except StopIteration:
            protocol.raise_for_status(self._response.headers, {})
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

        protocol.raise_for_status(response.headers, self._trailers)
        if result is None:
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
            )

        return result


class UnaryStreamCall(Call):
    @Call._raise_timeout(asyncio.TimeoutError)
    async def read(self):
        response = await self._get_response()

        async for trailers, _, message in protocol.unwrap_message_stream_async(
            response.content
        ):
            if trailers:
                self._trailers = protocol.unpack_trailers(message)
                break
            else:
                return self._deserializer(message)

        response.release()

        protocol.raise_for_status(response.headers, self._trailers)

        return grpc.experimental.aio.EOF

    @Call._raise_timeout(asyncio.TimeoutError)
    async def __aiter__(self):
        response = await self._get_response()

        async for trailers, _, message in protocol.unwrap_message_stream_async(
            response.content
        ):
            if trailers:
                self._trailers = protocol.unpack_trailers(message)
                break
            else:
                yield self._deserializer(message)

        response.release()

        protocol.raise_for_status(response.headers, self._trailers)


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
            protocol.raise_for_status(response.headers)

        buffer = io.BytesIO(data)

        messages = protocol.unwrap_message_stream(buffer)
        result = None

        try:
            trailers, compressed, message = next(messages)
        except StopIteration:
            protocol.raise_for_status(self._response.headers, {})
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

        protocol.raise_for_status(response.headers, self._trailers)
        if result is None:
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
            )

        return result


class StreamStreamCall(StreamingRequestCall):
    @Call._raise_timeout(asyncio.TimeoutError)
    async def read(self):
        response = await self._get_response()

        async for trailers, _, message in protocol.unwrap_message_stream_async(
            response.content
        ):
            if trailers:
                self._trailers = protocol.unpack_trailers(message)
                break
            else:
                return self._deserializer(message)

        response.release()

        protocol.raise_for_status(response.headers, self._trailers)

        return grpc.experimental.aio.EOF

    @Call._raise_timeout(asyncio.TimeoutError)
    async def __aiter__(self):
        response = await self._get_response()

        async for trailers, _, message in protocol.unwrap_message_stream_async(
            response.content
        ):
            if trailers:
                self._trailers = protocol.unpack_trailers(message)
                break
            else:
                yield self._deserializer(message)

        response.release()

        protocol.raise_for_status(response.headers, self._trailers)
