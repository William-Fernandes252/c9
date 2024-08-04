from abc import ABC, abstractmethod
from typing import Dict, Generic, NamedTuple, TypeVar

from lib.middleware import Connection, ContentType, Status


class Result(NamedTuple):
    """Result of a c9 command."""

    success: bool
    message: str = ""

    def __str__(self):
        return self.message


Args = TypeVar("Args", bound=NamedTuple)


class Command(ABC, Generic[Args]):
    """Abstract base class for c9 commands."""

    name: str

    def __init__(
        self,
        client: Connection,
        arguments: Args | None = None,
        options: Dict | None = None,
    ) -> None:
        self.client = client
        self.arguments = arguments
        self.options = options

    def _prepare_header(self) -> str:
        return (
            self.name
            + " "
            + " ".join(
                [str(arg) for arg in (self.arguments if self.arguments else [])]  # type: ignore
                + [
                    f"{key}={value}"
                    for key, value in (self.options.items() if self.options else {})
                ]
            )
        )

    def _receive_response(self):
        status: Status = self.client.receive_chunk()
        content_type: ContentType = self.client.receive_chunk()
        data = None
        if content_type != ContentType.NO_CONTENT:
            data = self.client.receive()
        return status, content_type, data

    @abstractmethod
    def _execute(self) -> Result: ...

    def execute(self):
        """Execute the command."""
        header = self._prepare_header()
        self.client.send_chunk(header)
        return self._execute()


class UploadCommandArgs(NamedTuple):
    """Arguments for the upload command."""

    path: str
    filename: str


class UploadCommand(Command[UploadCommandArgs]):
    """Upload a file to the c9 cloud."""

    name = "upload"

    def _execute(self):
        try:
            with open(self.arguments.path, "rb") as file:
                self.client.send_file(file)

            status, *_ = self._receive_response()
            if status == Status.OK.value:
                return Result(success=True, message="File uploaded successfully.")
            else:
                return Result(success=False, message="File upload failed.")
        except Exception as e:
            return Result(success=False, message=str(e))


class DownloadCommandArgs(NamedTuple):
    """Arguments for the download command."""

    filename: str


class DownloadCommand(Command[DownloadCommandArgs]):
    """Download a file from the c9 cloud."""

    name = "download"

    def _execute(self):
        try:
            status, _, data = self._receive_response()
            if status == Status.OK.value:
                with open(self.arguments.filename, "wb") as file:
                    file.write(data)
                return Result(success=True, message="File downloaded successfully.")
            else:
                return Result(success=False, message="File download failed.")
        except Exception as e:
            return Result(success=False, message=str(e))
