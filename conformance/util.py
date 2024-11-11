import collections
import typing

import grpc
from connectrpc.conformance.v1 import service_pb2
from google.protobuf.any_pb2 import Any
from google.rpc import status_pb2
from grpc_status import rpc_status

grpc_Metadata = tuple[tuple[str, str | bytes], ...]


def metadata_from_headers(
    headers: typing.Iterable[service_pb2.Header],
) -> grpc_Metadata:
    header_pairs = []
    for header in headers:
        header_pairs.extend((header.name, value) for value in header.value)
    return grpc_Metadata(header_pairs)


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
    detail = Any()
    detail.Pack(request_info)
    details = error.details
    details.append(detail)

    status = status_pb2.Status(
        code=error.code,
        message=error.message,
        details=details,
    )
    return rpc_status.to_status(status)
