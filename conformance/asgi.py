import collections
import time
import typing

import grpc
from connectrpc.conformance.v1 import service_pb2, service_pb2_grpc
from google.protobuf.any_pb2 import Any

# from google.rpc import code_pb2
from google.rpc import status_pb2
from grpc_status import rpc_status

from sonora.asgi import grpcASGI

# Setup your frameworks default ASGI app.
# Install the Sonora grpcASGI middleware so we can handle requests to gRPC's paths.

asgi_app = grpcASGI()

grpc_Metadata = tuple[tuple[str, str | bytes], ...]


def metadata_from_headers(
    headers: typing.Iterable[service_pb2.Header],
) -> grpc_Metadata:
    header_pairs = []
    for header in headers:
        header_pairs.extend((header.name, value) for value in header.value)
    return grpc_Metadata(header_pairs)
    # return grpc_Metadata((header.name, header.value) for header in headers)


def headers_from_metadata(
    metadata: grpc_Metadata,
) -> typing.Iterable[service_pb2.Header]:
    headers = collections.defaultdict(list)
    for header, value in metadata:
        headers[header].append(value)

    return (
        service_pb2.Header(name=name, value=value) for name, value in headers.items()
    )


def create_rich_error(
    error: service_pb2.Error, request_info: service_pb2.ConformancePayload.RequestInfo
) -> grpc.Status:
    details = list(error.details)
    detail = Any()
    detail.Pack(request_info)
    details.append(details)

    status = status_pb2.Status(
        code=error.code,
        message=error.message,
        details=details,
    )
    return rpc_status.to_status(status)


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
            time.sleep(response_definition.response_delay_ms / 1000)

        return service_pb2.UnaryResponse(
            payload=service_pb2.ConformancePayload(
                data=response_definition.response_data,
                request_info=request_info,
            )
        )


service_pb2_grpc.add_ConformanceServiceServicer_to_server(
    ConformanceServiceServicer(), asgi_app
)
