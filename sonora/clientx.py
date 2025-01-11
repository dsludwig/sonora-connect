import itertools
import typing

import grpc.experimental.aio
import httpx

import sonora.client
from sonora import _codec, _encoding, _events, protocol


def insecure_web_channel(
    url,
    client_kws=None,
    json=False,
    compression: typing.Optional[grpc.Compression] = None,
):
    return WebChannel(url, client_kws, json=json, compression=compression)


def insecure_connect_channel(
    url,
    client_kws=None,
    json=False,
    compression: typing.Optional[grpc.Compression] = None,
):
    return WebChannel(url, client_kws, connect=True, json=json, compression=compression)


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
        client_kws=None,
        connect=False,
        json=False,
        compression: typing.Optional[grpc.Compression] = None,
    ):
        if not url.startswith("http") and "://" not in url:
            url = f"http://{url}"

        self._url = url
        if client_kws is None:
            client_kws = {}

        self._session = httpx.AsyncClient(**client_kws)
        self._connect = connect
        self._json = json
        self._compression = compression

    async def __aenter__(self):
        return self

    async def __aexit__(self, exception_type, exception_value, traceback):
        await self._session.aclose()

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
    _response: typing.Optional[httpx.Response]

    async def __aenter__(self):
        return self

    async def __aexit__(self, exception_type, exception_value, traceback):
        if self._response:
            await self._response.aclose()

    async def _do_event(self, event):
        if isinstance(event, _events.StartRequest):
            timeout = httpx.Timeout(self._timeout)
            req = self._session.build_request(
                event.method,
                self._url,
                data=self._body,
                headers=event.headers,
                timeout=timeout,
            )
            self._response = await self._session.send(
                req,
                stream=True,
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
                status_code=self._response.status_code,
                phrase=self._response.reason_phrase,
                headers=self._response.headers,
            )
        ):
            yield e
        if self.response_streaming:
            async for chunk in self._response.aiter_raw():
                for e in self._codec.receive_body(chunk):
                    yield e
        else:
            body = bytearray()
            async for chunk in self._response.aiter_raw():
                body.extend(chunk)

            for e in self._codec.receive_body(bytes(body)):
                yield e

        for e in self._codec.end_request():
            yield e
        # TODO: event?
        self._trailers = self._codec._trailing_metadata

    async def _get_response(self):
        if self._response is None:
            timeout = httpx.Timeout(self._timeout)

            self._response = await self._session.post(
                self._url,
                data=self._body,
                headers=self._metadata,
                timeout=timeout,
            )

        return self._response

    async def initial_metadata(self):
        # response = await self._get_response()
        return self._response.headers.items()

    async def trailing_metadata(self):
        return self._trailers


class UnaryResponseCall(Call):
    async def _get_response(self):
        message = None
        try:
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
        finally:
            if self._response:
                await self._response.aclose()

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

    @Call._raise_timeout(httpx.TimeoutException)
    def __await__(self):
        self._body = body_generator(self._codec.send_request(self._request))

        message = yield from self._get_response().__await__()
        return message


class UnaryStreamCall(Call):
    request_streaming = False
    response_streaming = True

    @Call._raise_timeout(httpx.TimeoutException)
    async def read(self):
        if self._aiter is None:
            self._aiter = self.__aiter__()

        try:
            return await self._aiter.__anext__()
        except StopAsyncIteration:
            return grpc.experimental.aio.EOF

    @Call._raise_timeout(httpx.TimeoutException)
    async def __aiter__(self):
        self._body = body_generator(self._codec.send_request(self._request))

        try:
            async for e in self._do_call():
                if isinstance(e, _events.ReceiveMessage):
                    yield e.message
                else:
                    await self._do_event(e)
        finally:
            if self._response:
                await self._response.aclose()


class StreamUnaryCall(UnaryResponseCall):
    request_streaming = True
    response_streaming = False

    @Call._raise_timeout(httpx.TimeoutException)
    def __await__(self):
        self._body = body_generator(
            itertools.chain.from_iterable(
                self._codec.send_request(request) for request in self._request
            )
        )

        message = yield from self._get_response().__await__()
        return message


class StreamStreamCall(Call):
    request_streaming = True
    response_streaming = True

    def __init__(self, request, timeout, metadata, url, session, codec):
        super().__init__(request, timeout, metadata, url, session, codec)
        self._aiter = None

    @Call._raise_timeout(httpx.TimeoutException)
    async def read(self):
        if self._aiter is None:
            self._aiter = self.__aiter__()

        try:
            return await self._aiter.__anext__()
        except StopAsyncIteration:
            return grpc.experimental.aio.EOF

    @Call._raise_timeout(httpx.TimeoutException)
    async def __aiter__(self):
        self._body = body_generator(
            itertools.chain.from_iterable(
                self._codec.send_request(request) for request in self._request
            )
        )

        try:
            async for e in self._do_call():
                if isinstance(e, _events.ReceiveMessage):
                    yield e.message
                else:
                    await self._do_event(e)
        finally:
            if self._response:
                await self._response.aclose()
