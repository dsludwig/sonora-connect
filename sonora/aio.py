import asyncio
import itertools
import typing

import aiohttp
import grpc.experimental.aio

import sonora.client
from sonora import _codec, _encoding, _events, protocol
from sonora.metadata import Metadata


def insecure_web_channel(
    url,
    session_kws=None,
    json=False,
    compression: typing.Optional[grpc.Compression] = None,
):
    return WebChannel(url, session_kws, json=json, compression=compression)


def insecure_connect_channel(
    url,
    session_kws=None,
    json=False,
    compression: typing.Optional[grpc.Compression] = None,
):
    return WebChannel(
        url, session_kws, connect=True, json=json, compression=compression
    )


async def body_generator(events: _events.ClientEvents):
    for event in events:
        if isinstance(event, _events.SendBody):
            yield event.body
            if not event.more_body:
                break


class WebChannel:
    def __init__(
        self,
        url,
        session_kws=None,
        connect=False,
        json=False,
        compression: typing.Optional[grpc.Compression] = None,
    ):
        if not url.startswith("http") and "://" not in url:
            url = f"http://{url}"

        self._url = url
        if session_kws is None:
            session_kws = {}
        session_kws["auto_decompress"] = True

        self._session = aiohttp.ClientSession(**session_kws)
        self._connect = connect
        self._json = json
        self._compression = compression

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
            self._json,
            self._compression,
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
            self._compression,
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
            self._compression,
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
            self._compression,
        )


class UnaryUnaryMulticallable(sonora.client.Multicallable):
    @property
    def _codec(self):
        if self._connect:
            codec_class = (
                _codec.ConnectUnaryJsonCodec
                if self._json
                else _codec.ConnectUnaryProtoCodec
            )
            serializer_class = (
                _codec.JsonSerializer if self._json else _codec.ProtoSerializer
            )
            if (
                self._compression is None
                or self._compression == grpc.Compression.NoCompression
            ):
                encoding = _encoding.IdentityEncoding()
            elif self._compression == grpc.Compression.Deflate:
                encoding = _encoding.DeflateEncoding()
            elif self._compression == grpc.Compression.Gzip:
                encoding = _encoding.GZipEncoding()
            else:
                raise ValueError(f"Unsupported compression: {self._compression!r}")
            serializer = serializer_class(
                request_serializer=self._serializer,
                response_deserializer=self._deserializer,
            )
            return codec_class(encoding, serializer, _codec.CodecRole.CLIENT)
        else:
            return super()._codec

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
            self._codec,
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
            self._codec,
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
            self._codec,
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
            self._codec,
        )


class Call(sonora.client.Call):
    _response: typing.Optional[aiohttp.ClientResponse]

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self._response:
            self._response.close()

    def __del__(self):
        if self._response and not self._response.closed:
            self._response.close()

    async def _do_event(self, event):
        if isinstance(event, _events.StartRequest):
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            self._response = await self._session.request(
                event.method,
                self._url,
                data=self._body,
                headers=event.headers,
                timeout=timeout,
                compress=False,
                chunked=self.request_streaming or self.response_streaming,
            )
        elif isinstance(event, _events.SendBody):
            pass
        elif isinstance(event, _events.ReceiveMessage):
            pass
        else:
            raise ValueError("Unexpected codec event")

    async def _do_call(self):
        self._codec.set_invocation_metadata(self._metadata)
        self._codec.set_timeout(self._timeout)

        for e in self._codec.start_request():
            yield e

        for e in self._codec.start_response(
            _events.StartResponse(
                status_code=self._response.status,
                phrase=self._response.reason,
                headers=self._response.headers,
            )
        ):
            yield e
        if self.response_streaming:
            async for chunk in self._response.content.iter_any():
                for e in self._codec.receive_body(chunk):
                    yield e
        else:
            for e in self._codec.receive_body(await self._response.content.read()):
                yield e

        for e in self._codec.end_request():
            yield e
        # TODO: event?
        self._trailers = self._codec._trailing_metadata

    async def _get_response(self):
        if self._response is None:
            timeout = aiohttp.ClientTimeout(total=self._timeout)

            self._response = await self._session.post(
                self._url,
                data=self._body,
                headers=self._metadata,
                timeout=timeout,
            )

            # XXX
            # protocol.raise_for_status(self._response.headers)

        return self._response

    async def initial_metadata(self):
        # response = await self._get_response()
        return Metadata(self._response.headers.items())

    async def trailing_metadata(self):
        return self._trailers


class UnaryResponseCall(Call):
    async def _get_response(self):
        message = None
        async for e in self._do_call():
            if isinstance(e, _events.ReceiveMessage):
                if message is None:
                    message = e.message
                else:
                    raise protocol.WebRpcError(
                        grpc.StatusCode.UNIMPLEMENTED,
                        "Received multiple responses for a unary call",
                    )
            else:
                await self._do_event(e)

        if message is None:
            raise protocol.WebRpcError(
                grpc.StatusCode.UNIMPLEMENTED,
                "Missing response for unary call",
                initial_metadata=self._codec._initial_metadata,
                trailing_metadata=self._codec._trailing_metadata,
            )
        return message


class UnaryUnaryCall(UnaryResponseCall):
    request_streaming = False
    response_streaming = False

    @Call._raise_timeout(asyncio.TimeoutError)
    def __await__(self):
        self._body = body_generator(self._codec.send_request(self._request))

        try:
            message = yield from self._get_response().__await__()
            return message
        finally:
            if self._response:
                self._response.release()


class UnaryStreamCall(Call):
    request_streaming = False
    response_streaming = True

    def __init__(self, request, timeout, metadata, url, session, codec):
        super().__init__(request, timeout, metadata, url, session, codec)
        self._aiter = None

    @Call._raise_timeout(asyncio.TimeoutError)
    async def read(self):
        if self._aiter is None:
            self._aiter = self.__aiter__()

        try:
            return await self._aiter.__anext__()
        except StopAsyncIteration:
            return grpc.experimental.aio.EOF

    @Call._raise_timeout(asyncio.TimeoutError)
    async def __aiter__(self):
        self._body = body_generator(self._codec.send_request(self._request))

        async for e in self._do_call():
            if isinstance(e, _events.ReceiveMessage):
                yield e.message
            else:
                await self._do_event(e)


class StreamUnaryCall(UnaryResponseCall):
    request_streaming = True
    response_streaming = False

    @Call._raise_timeout(asyncio.TimeoutError)
    def __await__(self):
        self._body = body_generator(
            itertools.chain.from_iterable(
                self._codec.send_request(request) for request in self._request
            )
        )

        try:
            message = yield from self._get_response().__await__()
            return message
        finally:
            if self._response:
                self._response.release()


class StreamStreamCall(Call):
    request_streaming = True
    response_streaming = True

    def __init__(self, request, timeout, metadata, url, session, codec):
        super().__init__(request, timeout, metadata, url, session, codec)
        self._aiter = None

    @Call._raise_timeout(asyncio.TimeoutError)
    async def read(self):
        if self._aiter is None:
            self._aiter = self.__aiter__()

        try:
            return await self._aiter.__anext__()
        except StopAsyncIteration:
            return grpc.experimental.aio.EOF

    @Call._raise_timeout(asyncio.TimeoutError)
    async def __aiter__(self):
        self._body = body_generator(
            itertools.chain.from_iterable(
                self._codec.send_request(request) for request in self._request
            )
        )

        async for e in self._do_call():
            if isinstance(e, _events.ReceiveMessage):
                yield e.message
            else:
                await self._do_event(e)
