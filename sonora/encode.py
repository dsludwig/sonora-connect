import base64


def b64encode(value):
    # https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
    # Implementations MUST accept padded and un-padded values and should emit un-padded values.
    return base64.b64encode(value).decode("ascii").rstrip("=")


def b64decode(value):
    # https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
    # Implementations MUST accept padded and un-padded values and should emit un-padded values.
    _, padlength = divmod(len(value), 8)
    padvalue = value + "=" * padlength
    binvalue = base64.b64decode(padvalue)
    return binvalue
