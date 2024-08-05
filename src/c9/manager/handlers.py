from abc import ABC, ABCMeta, abstractmethod
from typing import ClassVar

from c9.buckets.api import Client as Bucket
from c9.buckets.api import File, NotFoundError
from c9.lib.middleware import ENCODING, ContentType, Status
from c9.manager.server import DISTANCE_MATRIX, Context, Response


class Handler(ABC):
    """Abstract base class for c9 handlers."""

    name: str

    def __init__(self, context: Context) -> None:
        self.context = context

    @abstractmethod
    def select_bucket(self, buckets: list[Bucket], **kwargs) -> Bucket | None:
        """Select a bucket for the request.

        It receives a list of buckets instead of using the instance
        attribute for flexibility.
        The parameters should be passed as keyword arguments.

        Args:
            buckets (list[Bucket]): The list of buckets.
            **kwargs: The request parameters.

        Returns:
            Bucket | None: The selected bucket or None if no bucket is available.

        Raises:
            ValueError: If a required parameter is missing.
        """

    @staticmethod
    def parse_parameters(parameters: list[str]) -> tuple[list[str], dict[str, str]]:
        """Parse a command parameters."""
        args = []
        kwargs = {}
        for parameter in parameters:
            if "=" in parameter:
                key, value = parameter.split("=")
                kwargs[key] = value
            else:
                args.append(parameter)
        return args, kwargs

    @abstractmethod
    def _handle(self, *args, **kwargs) -> tuple[Response | None, Exception | None]: ...

    def handle(self, *args, **kwargs):
        """Handle the request."""
        return self._handle(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self.handle(*args, **kwargs)


class HandlerRegistry(ABCMeta):
    """Metaclass to register handlers."""

    handlers: ClassVar[dict[str, type]] = {}

    def __new__(cls, name, bases, attrs):
        handler_class = super().__new__(cls, name, bases, attrs)
        HandlerRegistry.handlers[getattr(handler_class, "name")] = handler_class
        return handler_class

    @classmethod
    def get(cls, command: str) -> type[Handler] | None:
        """Get the handler for the given command if it is registered."""
        return cls.handlers.get(command, None)


class UploadHandler(Handler, metaclass=HandlerRegistry):
    """Handler for the upload command."""

    @staticmethod
    def get_bucket_available_space(bucket: Bucket) -> int:
        """Get the available space of the bucket, or 0 if it is not available.

        Args:
            bucket (Bucket): The bucket.

        Returns:
            int: The available space.
        """
        if not bucket.spec:
            return 0
        return bucket.spec["available_space"]

    def select_bucket(self, buckets: list[Bucket], **kwargs) -> Bucket | None:
        """Select a bucket for the upload."""
        if "size" not in kwargs:
            raise ValueError("Size is required")

        available_buckets = [
            bucket
            for bucket in buckets
            if self.get_bucket_available_space(bucket) >= kwargs["size"]
        ]
        return (
            sorted(
                available_buckets,
                key=lambda bucket: DISTANCE_MATRIX[self.context.region][bucket.region],
            )[0]
            if available_buckets
            else None
        )

    name = "upload"

    def _handle(self, _: str, filename: str):
        try:
            content = self.context.connection.receive()

            main_bucket = self.select_bucket(self.context.buckets, size=len(content))
            if not main_bucket:
                raise RuntimeError("No bucket available")

            backup_bucket = self.select_bucket(
                [
                    bucket
                    for bucket in self.context.buckets
                    if bucket.id != main_bucket.id
                ],
                size=len(content),
                filename=filename,
            )

            for bucket in [main_bucket, backup_bucket]:
                if isinstance(bucket, Bucket):
                    bucket.put(File(filename, content))

            return Response(Status.OK, ContentType.NO_CONTENT, self.context), None
        except Exception as error:
            return Response(Status.ERROR, ContentType.NO_CONTENT, self.context), error


class DownloadHandler(Handler, metaclass=HandlerRegistry):
    """Handler for the download command."""

    name = "download"

    def select_bucket(self, buckets: list[Bucket], **kwargs) -> Bucket | None:
        """Select a bucket for the download."""
        if "filename" not in kwargs:
            raise ValueError("Filename is required")

        for bucket in buckets:
            if kwargs["filename"] in bucket.files:
                return bucket
        return None

    def _handle(self, filename: str):
        try:
            bucket = self.select_bucket(self.context.buckets, filename=filename)
            if not bucket:
                raise NotFoundError()

            file = bucket.get(filename)
            return (
                Response(Status.OK, ContentType.BYTES, self.context, file.content),
                None,
            )
        except NotFoundError:
            return (
                Response(Status.NOT_FOUND, ContentType.NO_CONTENT, self.context),
                None,
            )
        except Exception as error:
            return (
                Response(
                    Status.ERROR,
                    ContentType.TEXT_PLAIN,
                    self.context,
                    "Unknown error".encode(ENCODING),
                ),
                error,
            )
