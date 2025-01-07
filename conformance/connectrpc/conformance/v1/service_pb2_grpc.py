# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from connectrpc.conformance.v1 import service_pb2 as connectrpc_dot_conformance_dot_v1_dot_service__pb2


class ConformanceServiceStub(object):
    """The service implemented by conformance test servers. This is implemented by
    the reference servers, used to test clients, and is expected to be implemented
    by test servers, since this is the service used by reference clients.

    Test servers must implement the service as described.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Unary = channel.unary_unary(
                '/connectrpc.conformance.v1.ConformanceService/Unary',
                request_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnaryRequest.SerializeToString,
                response_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnaryResponse.FromString,
                )
        self.ServerStream = channel.unary_stream(
                '/connectrpc.conformance.v1.ConformanceService/ServerStream',
                request_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.ServerStreamRequest.SerializeToString,
                response_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.ServerStreamResponse.FromString,
                )
        self.ClientStream = channel.stream_unary(
                '/connectrpc.conformance.v1.ConformanceService/ClientStream',
                request_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.ClientStreamRequest.SerializeToString,
                response_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.ClientStreamResponse.FromString,
                )
        self.BidiStream = channel.stream_stream(
                '/connectrpc.conformance.v1.ConformanceService/BidiStream',
                request_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.BidiStreamRequest.SerializeToString,
                response_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.BidiStreamResponse.FromString,
                )
        self.Unimplemented = channel.unary_unary(
                '/connectrpc.conformance.v1.ConformanceService/Unimplemented',
                request_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnimplementedRequest.SerializeToString,
                response_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnimplementedResponse.FromString,
                )
        self.IdempotentUnary = channel.unary_unary(
                '/connectrpc.conformance.v1.ConformanceService/IdempotentUnary',
                request_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.IdempotentUnaryRequest.SerializeToString,
                response_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.IdempotentUnaryResponse.FromString,
                )


class ConformanceServiceServicer(object):
    """The service implemented by conformance test servers. This is implemented by
    the reference servers, used to test clients, and is expected to be implemented
    by test servers, since this is the service used by reference clients.

    Test servers must implement the service as described.
    """

    def Unary(self, request, context):
        """A unary operation. The request indicates the response headers and trailers
        and also indicates either a response message or an error to send back.

        Response message data is specified as bytes. The service should echo back
        request properties in the ConformancePayload and then include the message
        data in the data field.

        If the response_delay_ms duration is specified, the server should wait the
        given duration after reading the request before sending the corresponding
        response.

        Servers should allow the response definition to be unset in the request and
        if it is, set no response headers or trailers and return no response data.
        The returned payload should only contain the request info.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ServerStream(self, request, context):
        """A server-streaming operation. The request indicates the response headers,
        response messages, trailers, and an optional error to send back. The
        response data should be sent in the order indicated, and the server should
        wait between sending response messages as indicated.

        Response message data is specified as bytes. The service should echo back
        request properties in the first ConformancePayload, and then include the
        message data in the data field. Subsequent messages after the first one
        should contain only the data field.

        Servers should immediately send response headers on the stream before sleeping
        for any specified response delay and/or sending the first message so that
        clients can be unblocked reading response headers.

        If a response definition is not specified OR is specified, but response data
        is empty, the server should skip sending anything on the stream. When there
        are no responses to send, servers should throw an error if one is provided
        and return without error if one is not. Stream headers and trailers should
        still be set on the stream if provided regardless of whether a response is
        sent or an error is thrown.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ClientStream(self, request_iterator, context):
        """A client-streaming operation. The first request indicates the response
        headers and trailers and also indicates either a response message or an
        error to send back.

        Response message data is specified as bytes. The service should echo back
        request properties, including all request messages in the order they were
        received, in the ConformancePayload and then include the message data in
        the data field.

        If the input stream is empty, the server's response will include no data,
        only the request properties (headers, timeout).

        Servers should only read the response definition from the first message in
        the stream and should ignore any definition set in subsequent messages.

        Servers should allow the response definition to be unset in the request and
        if it is, set no response headers or trailers and return no response data.
        The returned payload should only contain the request info.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def BidiStream(self, request_iterator, context):
        """A bidirectional-streaming operation. The first request indicates the response
        headers, response messages, trailers, and an optional error to send back.
        The response data should be sent in the order indicated, and the server
        should wait between sending response messages as indicated.

        Response message data is specified as bytes and should be included in the
        data field of the ConformancePayload in each response.

        Servers should send responses indicated according to the rules of half duplex
        vs. full duplex streams. Once all responses are sent, the server should either
        return an error if specified or close the stream without error.

        Servers should immediately send response headers on the stream before sleeping
        for any specified response delay and/or sending the first message so that
        clients can be unblocked reading response headers.

        If a response definition is not specified OR is specified, but response data
        is empty, the server should skip sending anything on the stream. Stream
        headers and trailers should always be set on the stream if provided
        regardless of whether a response is sent or an error is thrown.

        If the full_duplex field is true:
        - the handler should read one request and then send back one response, and
        then alternate, reading another request and then sending back another response, etc.

        - if the server receives a request and has no responses to send, it
        should throw the error specified in the request.

        - the service should echo back all request properties in the first response
        including the last received request. Subsequent responses should only
        echo back the last received request.

        - if the response_delay_ms duration is specified, the server should wait the given
        duration after reading the request before sending the corresponding
        response.

        If the full_duplex field is false:
        - the handler should read all requests until the client is done sending.
        Once all requests are read, the server should then send back any responses
        specified in the response definition.

        - the server should echo back all request properties, including all request
        messages in the order they were received, in the first response. Subsequent
        responses should only include the message data in the data field.

        - if the response_delay_ms duration is specified, the server should wait that
        long in between sending each response message.

        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Unimplemented(self, request, context):
        """A unary endpoint that the server should not implement and should instead
        return an unimplemented error when invoked.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def IdempotentUnary(self, request, context):
        """A unary endpoint denoted as having no side effects (i.e. idempotent).
        Implementations should use an HTTP GET when invoking this endpoint and
        leverage query parameters to send data.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ConformanceServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Unary': grpc.unary_unary_rpc_method_handler(
                    servicer.Unary,
                    request_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnaryRequest.FromString,
                    response_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnaryResponse.SerializeToString,
            ),
            'ServerStream': grpc.unary_stream_rpc_method_handler(
                    servicer.ServerStream,
                    request_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.ServerStreamRequest.FromString,
                    response_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.ServerStreamResponse.SerializeToString,
            ),
            'ClientStream': grpc.stream_unary_rpc_method_handler(
                    servicer.ClientStream,
                    request_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.ClientStreamRequest.FromString,
                    response_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.ClientStreamResponse.SerializeToString,
            ),
            'BidiStream': grpc.stream_stream_rpc_method_handler(
                    servicer.BidiStream,
                    request_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.BidiStreamRequest.FromString,
                    response_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.BidiStreamResponse.SerializeToString,
            ),
            'Unimplemented': grpc.unary_unary_rpc_method_handler(
                    servicer.Unimplemented,
                    request_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnimplementedRequest.FromString,
                    response_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnimplementedResponse.SerializeToString,
            ),
            'IdempotentUnary': grpc.unary_unary_rpc_method_handler(
                    servicer.IdempotentUnary,
                    request_deserializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.IdempotentUnaryRequest.FromString,
                    response_serializer=connectrpc_dot_conformance_dot_v1_dot_service__pb2.IdempotentUnaryResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'connectrpc.conformance.v1.ConformanceService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class ConformanceService(object):
    """The service implemented by conformance test servers. This is implemented by
    the reference servers, used to test clients, and is expected to be implemented
    by test servers, since this is the service used by reference clients.

    Test servers must implement the service as described.
    """

    @staticmethod
    def Unary(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/connectrpc.conformance.v1.ConformanceService/Unary',
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnaryRequest.SerializeToString,
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnaryResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ServerStream(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/connectrpc.conformance.v1.ConformanceService/ServerStream',
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.ServerStreamRequest.SerializeToString,
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.ServerStreamResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ClientStream(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/connectrpc.conformance.v1.ConformanceService/ClientStream',
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.ClientStreamRequest.SerializeToString,
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.ClientStreamResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def BidiStream(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/connectrpc.conformance.v1.ConformanceService/BidiStream',
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.BidiStreamRequest.SerializeToString,
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.BidiStreamResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Unimplemented(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/connectrpc.conformance.v1.ConformanceService/Unimplemented',
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnimplementedRequest.SerializeToString,
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.UnimplementedResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def IdempotentUnary(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/connectrpc.conformance.v1.ConformanceService/IdempotentUnary',
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.IdempotentUnaryRequest.SerializeToString,
            connectrpc_dot_conformance_dot_v1_dot_service__pb2.IdempotentUnaryResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
