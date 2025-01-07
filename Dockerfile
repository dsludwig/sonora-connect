FROM python:3.8.1-buster AS base

WORKDIR /usr/src/app

RUN apt update && \
    apt install -y build-essential libev-dev

ENV POETRY_VERSION=1.8.4
RUN curl -sSL https://install.python-poetry.org | python -
ENV PATH="${PATH}:/root/.local/bin"

COPY . .

RUN poetry install

RUN poetry run python -m grpc.tools.protoc \
    --proto_path="$(pwd)/" \
    --python_out=. \
    --grpc_python_out=. \
    "$(pwd)"/test_server/*.proto

FROM base AS wsgi

CMD poetry run python test_server/wsgi.py

FROM base AS asgi

CMD poetry run daphne -b 0.0.0.0 -p 8080 test_server.asgi:application
