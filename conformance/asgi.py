import asyncio
import typing

import grpc
from connectrpc.conformance.v1 import service_pb2, service_pb2_grpc
from google.protobuf.any_pb2 import Any
from google.rpc import status_pb2
from grpc_status import rpc_status
from util import create_rich_error, headers_from_metadata, metadata_from_headers

from sonora.asgi import grpcASGI

# Copied from https://github.com/python/cpython/pull/8895
_NOT_PROVIDED = object()


async def anext(async_iterator, default=_NOT_PROVIDED):
    """anext(async_iterator[, default])
    Return the next item from the async iterator.
    If default is given and the iterator is exhausted,
    it is returned instead of raising StopAsyncIteration.
    """
    if not isinstance(async_iterator, typing.AsyncIterator):
        raise TypeError(f"anext expected an AsyncIterator, got {type(async_iterator)}")
    anxt = async_iterator.__anext__
    try:
        return await anxt()
    except StopAsyncIteration:
        if default is _NOT_PROVIDED:
            raise
        return default


# Setup your frameworks default ASGI app.
# Install the Sonora grpcASGI middleware so we can handle requests to gRPC's paths.

asgi_app = grpcASGI()


# Attach your gRPC server implementation.
class ConformanceServiceServicer(service_pb2_grpc.ConformanceServiceServicer):
    async def Unary(
        self, request: service_pb2.UnaryRequest, context: grpc.ServicerContext
    ) -> service_pb2.UnaryResponse:
        response_definition = request.response_definition
        await context.send_initial_metadata(
            metadata_from_headers(response_definition.response_headers)
        )
        context.set_trailing_metadata(
            metadata_from_headers(response_definition.response_trailers)
        )
        time_remaining = context.time_remaining()
        request_any = Any()
        request_any.Pack(request)
        request_info = service_pb2.ConformancePayload.RequestInfo(
            request_headers=headers_from_metadata(context.invocation_metadata()),
            requests=[request_any],
            timeout_ms=None if time_remaining is None else int(time_remaining * 1000),
        )

        if response_definition.HasField("error"):
            status = create_rich_error(response_definition.error, request_info)
            await context.abort_with_status(status)

        if response_definition.response_delay_ms:
            await asyncio.sleep(response_definition.response_delay_ms / 1000)

        return service_pb2.UnaryResponse(
            payload=service_pb2.ConformancePayload(
                data=response_definition.response_data,
                request_info=request_info,
            )
        )

    async def ClientStream(
        self,
        requests: typing.AsyncIterable[service_pb2.ClientStreamRequest],
        context: grpc.ServicerContext,
    ):
        response_definition = None
        request_data = []

        async for request in requests:
            if response_definition is None:
                response_definition = request.response_definition
            request_any = Any()
            request_any.Pack(request)
            request_data.append(request_any)

        await context.send_initial_metadata(
            metadata_from_headers(response_definition.response_headers)
        )
        context.set_trailing_metadata(
            metadata_from_headers(response_definition.response_trailers)
        )

        time_remaining = context.time_remaining()
        request_info = service_pb2.ConformancePayload.RequestInfo(
            request_headers=headers_from_metadata(context.invocation_metadata()),
            requests=request_data,
            timeout_ms=None if time_remaining is None else int(time_remaining * 1000),
        )

        if response_definition.HasField("error"):
            status = create_rich_error(response_definition.error, request_info)
            await context.abort_with_status(status)

        if response_definition.response_delay_ms:
            await asyncio.sleep(response_definition.response_delay_ms / 1000)

        return service_pb2.ClientStreamResponse(
            payload=service_pb2.ConformancePayload(
                data=response_definition.response_data,
                request_info=request_info,
            )
        )

    async def ServerStream(
        self,
        request: service_pb2.ServerStreamRequest,
        context: grpc.ServicerContext,
    ) -> typing.AsyncGenerator[service_pb2.ServerStreamResponse, None]:
        response_definition = request.response_definition

        await context.send_initial_metadata(
            metadata_from_headers(response_definition.response_headers)
        )
        context.set_trailing_metadata(
            metadata_from_headers(response_definition.response_trailers)
        )

        request_any = Any()
        request_any.Pack(request)
        time_remaining = context.time_remaining()
        request_info = service_pb2.ConformancePayload.RequestInfo(
            request_headers=headers_from_metadata(context.invocation_metadata()),
            requests=[request_any],
            timeout_ms=None if time_remaining is None else int(time_remaining * 1000),
        )

        first_response = True
        for response_data in response_definition.response_data:
            if first_response:
                payload = service_pb2.ConformancePayload(
                    data=response_data,
                    request_info=request_info,
                )
                first_response = False
            else:
                payload = service_pb2.ConformancePayload(
                    data=response_data,
                )

            if response_definition.response_delay_ms:
                await asyncio.sleep(response_definition.response_delay_ms / 1000)

            yield service_pb2.ServerStreamResponse(
                payload=payload,
            )

        if response_definition.HasField("error"):
            if first_response:
                status = create_rich_error(response_definition.error, request_info)
            else:
                error = response_definition.error
                status = rpc_status.to_status(
                    status_pb2.Status(
                        code=error.code,
                        message=error.message,
                        details=error.details,
                    )
                )

            await context.abort_with_status(status)

    async def BidiStream(
        self,
        requests: typing.AsyncIterable[service_pb2.BidiStreamRequest],
        context: grpc.ServicerContext,
    ) -> typing.AsyncGenerator[service_pb2.BidiStreamResponse, None]:
        first_request = True
        full_duplex = False
        request_data = []
        response_definition = None
        response_index = 0
        async for request in requests:
            request_any = Any()
            request_any.Pack(request)
            request_data.append(request_any)

            if first_request:
                response_definition = request.response_definition
                full_duplex = request.full_duplex

                await context.send_initial_metadata(
                    metadata_from_headers(response_definition.response_headers)
                )
                context.set_trailing_metadata(
                    metadata_from_headers(response_definition.response_trailers)
                )
                first_request = False

            if full_duplex:
                if response_index >= len(response_definition.response_data):
                    break

                if response_index == 0:
                    time_remaining = context.time_remaining()
                    request_info = service_pb2.ConformancePayload.RequestInfo(
                        request_headers=headers_from_metadata(
                            context.invocation_metadata()
                        ),
                        requests=request_data,
                        timeout_ms=None
                        if time_remaining is None
                        else int(time_remaining * 1000),
                    )
                else:
                    request_info = service_pb2.ConformancePayload.RequestInfo(
                        requests=[request_any],
                    )

                if response_definition.response_delay_ms:
                    await asyncio.sleep(response_definition.response_delay_ms / 1000)

                response = service_pb2.BidiStreamResponse(
                    payload=service_pb2.ConformancePayload(
                        request_info=request_info,
                        data=response_definition.response_data[response_index],
                    )
                )

                response_index += 1
                yield response

        while response_index < len(response_definition.response_data):
            if response_index == 0:
                time_remaining = context.time_remaining()
                request_info = service_pb2.ConformancePayload.RequestInfo(
                    request_headers=headers_from_metadata(
                        context.invocation_metadata()
                    ),
                    requests=request_data,
                    timeout_ms=None
                    if time_remaining is None
                    else int(time_remaining * 1000),
                )
            else:
                request_info = None

            response = service_pb2.BidiStreamResponse(
                payload=service_pb2.ConformancePayload(
                    request_info=request_info,
                    data=response_definition.response_data[response_index],
                )
            )

            if response_definition.response_delay_ms:
                await asyncio.sleep(response_definition.response_delay_ms / 1000)

            response_index += 1
            yield response

        if response_definition.HasField("error"):
            if response_index == 0:
                time_remaining = context.time_remaining()
                request_info = service_pb2.ConformancePayload.RequestInfo(
                    request_headers=headers_from_metadata(
                        context.invocation_metadata()
                    ),
                    requests=request_data,
                    timeout_ms=None
                    if time_remaining is None
                    else int(time_remaining * 1000),
                )
                status = create_rich_error(response_definition.error, request_info)
            else:
                error = response_definition.error
                status = rpc_status.to_status(
                    status_pb2.Status(
                        code=error.code,
                        message=error.message,
                        details=error.details,
                    )
                )

            await context.abort_with_status(status)


service_pb2_grpc.add_ConformanceServiceServicer_to_server(
    ConformanceServiceServicer(), asgi_app
)
