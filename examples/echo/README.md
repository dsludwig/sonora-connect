# Echo

A nice simple example showing various method types and streaming modes.

Copied almost entirely from the
[official grpc-web repo](https://github.com/grpc/grpc-web/blob/92aa9f8fc8e7af4aadede52ea075dd5790a63b62/net/grpc/gateway/examples/echo/echo.proto).

## How to run

Start a server:

```
python wsgi.py
```

OR

```
python asgi.py
```

Then run one of the clients:

```
python client.py
```

OR

```
python aclient.py
```
