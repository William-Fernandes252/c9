from dataclasses import dataclass
from typing import Final, Self

from buckets.api import Client as Bucket
from buckets.api import Region
from lib.middleware import Connection, ContentType, Status

DISTANCE_MATRIX: Final[dict[Region, dict[Region, int]]] = {
    "us-east": {
        "us-west": 2500,
        "eu": 4000,
        "asia": 7000,
        "latin-america": 1500,
        "africa": 6000,
        "australia": 9000,
    },
    "us-west": {
        "us-east": 2500,
        "eu": 3500,
        "asia": 6500,
        "latin-america": 2000,
        "africa": 5500,
        "australia": 8500,
    },
    "eu": {
        "us-east": 4000,
        "us-west": 3500,
        "asia": 6000,
        "latin-america": 5000,
        "africa": 2000,
        "australia": 8000,
    },
    "asia": {
        "us-east": 7000,
        "us-west": 6500,
        "eu": 6000,
        "latin-america": 11000,
        "africa": 5000,
        "australia": 3000,
    },
    "latin-america": {
        "us-east": 1500,
        "us-west": 2000,
        "eu": 5000,
        "asia": 11000,
        "africa": 4000,
        "australia": 12000,
    },
    "africa": {
        "us-east": 6000,
        "us-west": 5500,
        "eu": 2000,
        "asia": 5000,
        "latin-america": 4000,
        "australia": 7000,
    },
    "australia": {
        "us-east": 9000,
        "us-west": 8500,
        "eu": 8000,
        "asia": 3000,
        "latin-america": 12000,
        "africa": 7000,
    },
}


@dataclass(frozen=True, slots=True)
class Context:
    region: Region
    buckets: list[Bucket]
    connection: Connection


@dataclass(frozen=True, slots=True)
class Response:
    status: Status
    content_type: ContentType
    context: Context
    data: bytes | None = None

    def send(self) -> Self:
        """Send the response."""
        self.context.connection.send_chunk(self.status.value)
        self.context.connection.send_chunk(self.content_type.value)
        if self.data:
            self.context.connection.send(self.data)
        return self

    def __str__(self) -> str:
        return f"{self.status.name} {self.content_type.name}"
