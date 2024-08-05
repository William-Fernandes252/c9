import io
import socket
from enum import Enum
from typing import Final

CHUNK_SIZE: Final[int] = 1024
ENCODING: Final[str] = "utf-8"


class Connection:
    """Middleware for communication between c9 management server, clients and buckets."""

    def __init__(self, socket: socket.socket):
        self.socket = socket

    def send(self, data: bytes | str):
        """Send data to the server.

        Args:
            data (bytes): The data to send.

        Raises:
            RuntimeError: If the socket connection is broken.
        """
        content = data if isinstance(data, bytes) else data.encode(ENCODING)
        self.socket.sendall(content)

    def send_file(self, file: io.BufferedReader):
        """Send a file to the server."""
        self.socket.sendfile(file)

    def receive(self) -> bytes:
        """Receive data from the server.

        Returns:
            bytes: The data received.
        """
        return self.socket.recv(CHUNK_SIZE)

    def receive_file(self, file: io.BufferedWriter):
        """Receive a file from the server."""
        data = self.receive()
        file.write(data)

    def receive_chunk(self) -> str:
        """Receive a chunk of data from the server."""
        return self.receive().decode(ENCODING).strip()

    def close(self):
        """Close the socket connection."""
        self.socket.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def send_chunk(self, content: str) -> bytes:
        """Justify content with spaces to fill one chunk."""
        if len(content) > CHUNK_SIZE:
            raise ValueError("Content lenght must be less than the chunk size.")
        return self.send(content.ljust(CHUNK_SIZE).encode(ENCODING))

    def listen(self, host: str, port: int):
        """Listen a port."""
        self.socket.bind((host, port))
        self.socket.listen(1)

    def accept(self) -> "Connection":
        socket, _ = self.socket.accept()
        return Connection(socket)

    def connect(self, host: str, port: int):
        """Connect to a server."""
        self.socket.connect((host, port))


class Status(Enum):
    OK = "OK"
    ERROR = "ERROR"
    NOT_FOUND = "NOT_FOUND"


class ContentType(Enum):
    NO_CONTENT = "no-content"
    TEXT_PLAIN = "text/plain"
    BYTES = "application/bytes"
