import time
from dataclasses import dataclass
from enum import Enum
from logging import Logger
from pathlib import Path
from socket import AF_INET, SOCK_STREAM, socket
from typing import Literal, Protocol, Self, TypedDict

from c9.lib.middleware import ENCODING, Connection


class NotFoundError(Exception):
    def __init__(self):
        super().__init__("File not found")


class StatusCode(Enum):
    OK = "OK"
    NOT_FOUND = "NOT_FOUND"
    ERROR = "ERROR"
    INSUFFICIENT_SPACE = "INSUFFICIENT_SPACE"


Region = Literal[
    "us-east", "us-west", "eu", "asia", "latin-america", "africa", "australia"
]


class Spec(TypedDict):
    files_count: int
    available_space: int
    used_space: int


@dataclass
class File:
    name: str
    content: bytes

    def save(self, path: Path):
        with open(path / self.name, "wb") as file:
            file.write(self.content)

    @property
    def size(self):
        return len(self.content)


class Client:
    id: str

    def __init__(self, host: str, port: int, base_path: Path, id: str, region: Region):
        self._base_path = base_path
        self.host = host
        self.port = port
        self.region = region
        self.id = id
        self.files: list[str] = []
        self._spec: Spec | None = None

    def get(self, filename: str):
        with Connection(socket()) as connection:
            connection.connect(self.host, self.port)
            connection.send(f"GET {filename}")
            print("Sent GET command")
            status = connection.receive().decode(ENCODING).strip()
            print(f"Received status: {status}")
            if status == StatusCode.OK.value:
                name, _ = connection.receive().decode(ENCODING).strip().split(" ")
                print(f"Received file: {name}")
                content = connection.receive()
                self.files.append(name)
                return File(name, content)
            elif status == StatusCode.NOT_FOUND.value:
                raise NotFoundError()
            else:
                error = connection.receive().decode(ENCODING).strip()
                raise RuntimeError(error)

    def put(self, file: File):
        with Connection(socket()) as connection:
            connection.connect(self.host, self.port)
            connection.send(f"PUT {file.name} {file.size}")
            time.sleep(1)
            connection.send(file.content)

            status = connection.receive().decode(ENCODING).strip()
            if status != StatusCode.OK.value:
                error = connection.receive().decode(ENCODING).strip()
                raise RuntimeError(error)

            self.files.append(file.name)

    @property
    def spec(self) -> Spec | None:
        return self._spec

    @spec.setter
    def spec(self, value: Spec):
        self._spec = value

    def __str__(self):
        return f"Client({self.host}:{self.port}, {self._base_path})"


class Observer(Protocol):
    def registered(self, client: Client): ...

    def unregistered(self, id: str): ...

    def updated(self, id: str, spec: Spec): ...


class Subject:
    def __init__(self, host: str, port: int, logger: Logger):
        self._host = host
        self._port = port
        self._observers: list[Observer] = []
        self._logger = logger

    def subscribe(self, observer: Observer):
        self._observers.append(observer)

    def unsubscribe(self, observer: Observer):
        self._observers.remove(observer)

    def listen(self) -> Self:
        with Connection(socket(AF_INET, SOCK_STREAM)) as connection:
            connection.listen(self._host, self._port)
            self._logger.info(f"Subject started on {self._host}:{self._port}")

            while True:
                with connection.accept() as bucket_server:
                    command, *args = (
                        bucket_server.receive().decode(ENCODING).strip().split(" ")
                    )
                    match command:
                        case "register":
                            (
                                id,
                                host,
                                port,
                                region,
                                path,
                                files_count,
                                available_space,
                                used_space,
                            ) = args
                            for observer in self._observers:
                                client = Client(host, int(port), Path(path), id, region)
                                client.spec = {
                                    "files_count": int(files_count),
                                    "available_space": int(available_space),
                                    "used_space": int(used_space),
                                }
                                observer.registered(client)
                        case "unregister":
                            id = args[0]
                            for observer in self._observers:
                                observer.unregistered(id)
                        case "update":
                            id, files_count, available_space, used_space = args
                            for observer in self._observers:
                                observer.updated(
                                    id,
                                    {
                                        "files_count": int(files_count),
                                        "available_space": int(available_space),
                                        "used_space": int(used_space),
                                    },
                                )

                self._logger.info(f"Handled command: {command}")
