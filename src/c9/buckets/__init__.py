import logging
import random
import sys
import threading
from logging import Logger
from pathlib import Path
from socket import socket
from typing import ClassVar, get_args
from uuid import uuid4

from c9.buckets.api import Region, Spec, StatusCode
from c9.buckets.utils import get_directory_size
from c9.lib.middleware import ENCODING, Connection


class Bucket:
    """Bucket server.

    Attributes:
        base_path (Path): The base path of the bucket.
        host (str): The host of the bucket server.
        port (int): The port of the bucket server.
        id (str): The unique identifier of the bucket.
        path (Path): The path of the bucket.
        available_space (int): The available space of the bucket in bytes.
    """

    DEFAULT_SPACE: ClassVar[int] = 250_000_000

    def __init__(
        self,
        base_path: Path,
        host: str,
        port: int,
        region: Region,
        logger: Logger,
        subjects: list[tuple[str, int]] | None = None,
    ):
        self._host = host
        self._port = port
        self._region = region
        self._id = str(uuid4())

        self._logger = logger
        self._logger.name = f"bucket-{self._id}-{self._region}"

        self._path = base_path / self._id
        self._path.mkdir(parents=True, exist_ok=True)

        self._subjects = subjects

        self._spec : Spec= {
            "files_count": 0,
            "available_space": self.DEFAULT_SPACE,
            "used_space": 0,
        }

    def spec_message(self) -> str:
        """Get the bucket specification.

        Returns:
            str: The bucket specification in the format "`id` `files_count` `available_space` `used_space`".
        """
        return f"{self._spec["files_count"]} {self._spec['available_space']} {self._spec['used_space']}"

    def register(self):
        """Register the bucket in the subjects."""
        for host, port in self._subjects:
            with Connection(socket()) as subject_connection:
                subject_connection.connect(host, port)
                subject_connection.send(
                    f"register {self._id} {self._host} {self._port} {self._region} {self._path.resolve().as_posix()} {self.spec_message()}"
                )
                self._logger.info(f"Registered in {host}:{port}")

    def try_update(self):
        """Try to update the bucket in the subject."""
        for host, port in self._subjects:
            with Connection(socket()) as subject_connection:
                subject_connection.connect(host, port)
                subject_connection.send(f"update {self._id} {self.spec_message()}")
                self._logger.info(f"Updated in {host}:{port}")

    def unregister(self):
        """Unregister the bucket from the subject."""
        for host, port in self._subjects:
            with Connection(socket()) as subject_connection:
                subject_connection.connect(host, port)
                subject_connection.send(f"unregister {self._id}")
                self._logger.info(f"Unregistered from {host}:{port}")

    def run(self):
        """Run the bucket server."""
        self._logger.info(f"Server running on {self._host}:{self._port}")
        with Connection(socket()) as client_connection:
            client_connection.listen(self._host, self._port)

            while True:
                with client_connection.accept() as client:
                    command, *args = (
                        client.receive().decode(ENCODING).strip().split(" ")
                    )
                    status: StatusCode = getattr(self, f"handle_{command.lower()}")(client, args)
                    self._logger.info(f"{command} ({", ".join(args)}) - {status.value}")

                self.try_update()

    def handle_get(self, client: Connection, args: list[str]):
        """Handle the GET command.

        Args:
            client (Connection): The client connection.
            args (list[str]): A list with the name of the file to get.
        """
        filename = args[0]

        path = self._path / filename
        if path.exists():
            with path.open("rb") as file:
                content = file.read()
                status = StatusCode.OK
                client.send(status.value)
                client.send(f"{filename} {len(content)}")
                client.send(content)
        else:
            status = StatusCode.NOT_FOUND
            client.send(status.value)

        return status

    def handle_put(self, client: Connection, args: list[str]):
        """Handle the PUT command.

        Args:
            client (Connection): The client connection.
            args (list[str]): A list with the name of the file to put.
        """
        filename = args[0]

        content = client.receive()
        if len(content) > self._spec["available_space"]:
            status = StatusCode.INSUFFICIENT_SPACE
            client.send(status.value)
        else:
            path = self._path / filename
            with path.open("wb") as file:
                file.write(content)

            self._spec["available_space"] -= len(content)
            self._spec["used_space"] = get_directory_size(self._path)
            self._spec["files_count"] += 1

            status = StatusCode.OK
            client.send(status.value)

        return status

    def handle_delete(self, client: Connection, args: list[str]):
        """Handle the DELETE command.

        Args:
            client (Connection): The client connection.
            args (list[str]): A list with the name of the file to delete.
        """
        filename = args[0]

        path = self._path / filename
        if path.exists():
            self._spec["available_space"] += path.stat().st_size
            path.unlink()
            status = StatusCode.OK
            client.send(StatusCode.OK.value)
        else:
            status = StatusCode.NOT_FOUND
            client.send(StatusCode.NOT_FOUND.value)

        return status

    def handle_list(self, client: Connection, *_):
        """Handle the LIST command.

        Args:
            client (Connection): The client connection.
        """
        files = [file.name for file in self._path.iterdir() if file.is_file()]
        status = StatusCode.OK
        client.send(status.value)
        client.send(" ".join(files))
        return status

    def handle_spec(self, client: Connection, *_):
        """The the SPEC command.

        Args:
            client (Connection): The client connection.
        """
        status = StatusCode.OK
        client.send(status.value)
        client.send(self.spec_message())
        return status

    def __call__(self):
        try:
            self.register()
        except Exception as error:
            self._logger.error(f"Error registering: {error}")
            return
        self.run()

    @property
    def path(self) -> Path:
        """The path of the bucket."""
        return self._path

    @property
    def id(self) -> str:
        """The unique identifier of the bucket."""
        return self._id

    @property
    def host(self) -> str:
        """The host of the bucket server."""
        return self._host

    @property
    def port(self) -> int:
        """The port of the bucket server."""
        return self._port


def main():
    """Run the bucket server."""
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="[%(asctime)s] %(levelname)s (%(name)s): %(message)s",
    )

    start_port = 3001
    threads: list[threading.Thread] = []
    for i in range(3):
        thread = threading.Thread(
            target=Bucket(
                Path(".data"),
                "localhost",
                start_port + i,
                random.choice(get_args(Region)),
                logging.getLogger(f"bucket-{i}"),
                [("localhost", 3000)],
            )
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
