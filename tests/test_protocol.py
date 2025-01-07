import io

import pytest
from hypothesis import given
from hypothesis import strategies as st

from sonora import _codec, _encoding, protocol


class TestSerializer(_codec.Serializer):
    def serialize_request(self, request):
        return request

    def deserialize_response(self, response):
        return response

    def serialize_response(self, response):
        return response

    def deserialize_request(self, request):
        return request


@pytest.mark.parametrize(
    "codec_class",
    [
        _codec.ConnectStreamCodec,
        _codec.ConnectUnaryCodec,
        _codec.GrpcCodec,
    ],
)
def test_wrapping(codec_class):
    encoding = _encoding.IdentityEncoding()
    serializer = TestSerializer()
    codec = codec_class(encoding, serializer)
    data = b"foobar"
    wrapped = codec.wrap_message(False, False, data)
    assert codec.unwrap_message(wrapped) == (False, False, data, b"")


@pytest.mark.parametrize(
    "codec_class",
    [
        _codec.ConnectStreamCodec,
        # _codec.ConnectUnaryCodec,
        _codec.GrpcCodec,
    ],
)
def test_unwrapping_stream(codec_class):
    encoding = _encoding.IdentityEncoding()
    serializer = TestSerializer()
    codec = codec_class(encoding, serializer)
    buffer = io.BytesIO()

    messages = [
        b"Tyger Tyger, burning bright,",
        b"In the forests of the night;",
        b"What immortal hand or eye,",
        b"Could frame thy fearful symmetry?",
    ]
    for message in messages:
        buffer.write(codec.wrap_message(False, False, message))

    buffer.seek(0)

    resp_messages = []
    for _, _, resp in codec.unwrap_message_stream(buffer):
        resp_messages.append(resp)

    assert resp_messages == messages


@given(st.floats(allow_nan=False, allow_infinity=False))
def test_timeout_serdes(timeout):
    ser = protocol.serialize_timeout(timeout).encode("ascii")
    des = protocol.parse_timeout(ser)
    assert abs(timeout - des) < 1e-6, (timeout, ser)
