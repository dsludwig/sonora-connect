# Copyright 2020 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import typing
from collections import abc

from .protocol import b64decode

MetadataKey = str
MetadataValue = typing.Union[str, bytes]
MetadataList = typing.Iterable[tuple[MetadataKey, MetadataValue]]
MetadataDict = dict[
    MetadataKey, typing.Union[MetadataValue, typing.Iterable[MetadataValue]]
]


class Metadata(abc.Mapping[MetadataKey, MetadataValue]):
    """Deserialized GPRC headers. `-bin` headers are stored in bytes format, not
    base64-encoded."""

    _state: dict[MetadataKey, list[MetadataValue]]

    def __init__(
        self, headers: "Metadata | MetadataDict | MetadataList | None" = None
    ) -> None:
        self._state = {}
        if headers is not None:
            self.extend(headers)

    def _normalize(self, key: MetadataKey) -> MetadataKey:
        return key.lower()

    def add(self, key: MetadataKey, value: MetadataValue):
        key = self._normalize(key)
        if key.endswith("-bin") and isinstance(value, str):
            value = b64decode(value)

        self._state.setdefault(key, [])
        self._state[key].append(value)

    def __setitem__(self, key: MetadataKey, value: MetadataValue):
        key = self._normalize(key)
        if key.endswith("-bin") and isinstance(value, str):
            value = b64decode(value)

        if key in self._state:
            self._state[key][0] = value
        else:
            self._state[key] = [value]

    def extend(self, other: "Metadata | MetadataDict | MetadataList"):
        if isinstance(other, type(self)):
            for key, value in other:
                self.add(key, value)
        elif isinstance(other, abc.Mapping):
            for key, values in other.items():
                if isinstance(values, (str, bytes)):
                    self.add(key, values)
                else:
                    for value in values:
                        self.add(key, value)
        else:
            for key, value in other:
                self.add(key, value)

    def get_all(self, key: MetadataKey) -> tuple[MetadataValue, ...]:
        return tuple(self._state.get(self._normalize(key), []))

    getlist = get_all

    def get(self, key, default=None):
        values = self._state.get(self._normalize(key))
        return values[0] if values else default

    def __getitem__(self, key: MetadataKey) -> MetadataValue:
        values = self._state[self._normalize(key)]
        return values[0]

    # This method is compatible with the expected interface of metadata for grpc.
    def __iter__(self) -> typing.Iterator[tuple[MetadataKey, MetadataValue]]:  # type: ignore[override]
        for key, values in self._state.items():
            for value in values:
                yield (key, value)

    def __len__(self) -> int:
        return sum(map(len, self._state.values()))

    def copy(self):
        return self.__class__(self)

    def __repr__(self) -> str:
        return f"Metadata({self._state!r})"


__all__ = ["Metadata"]
