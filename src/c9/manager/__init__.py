import logging
import sys
from logging import Logger
from socket import AF_INET, SOCK_STREAM, socket
from threading import Thread
from typing import ClassVar

from c9.buckets.api import Client as Bucket
from c9.buckets.api import Region
from c9.buckets.api import Spec as BucketSpec
from c9.buckets.api import Subject as BucketSubject
from c9.lib.middleware import ENCODING, Connection
from c9.manager.handlers import Handler, HandlerRegistry
from c9.manager.server import Context


class Manager:

    REGION: ClassVar[Region] = "latin-america"

    def __init__(
        self, host: str, port: int, subject: BucketSubject, logger: Logger, **options
    ):
        self._host = host
        self._port = port
        self._options = options
        self._buckets: dict[str, Bucket] = {}
        self._logger = logger

        self._subject = subject
        self._subject.subscribe(self)

    def run(self):
        with Connection(socket(AF_INET, SOCK_STREAM)) as connection:
            connection.listen(self._host, self._port)
            self._logger.info(f"Manager started on {self._host}:{self._port}")

            while True:
                server = connection.accept()  # Must be closed by the handler
                try:
                    command, *parameters = (
                        server.receive().decode(ENCODING).strip().split(" ")
                    )

                    handler_class = HandlerRegistry.get(command)
                    if not handler_class:
                        raise RuntimeError("Invalid command")

                    args, kwargs = handler_class.parse_parameters(parameters)
                    Thread(
                        target=self.thread_wrapper(
                            handler_class(
                                Context(
                                    self.REGION, list(self._buckets.values()), server
                                )
                            ),
                            server,
                        ),
                        args=args,
                        kwargs=kwargs,
                    ).start()
                except Exception as e:
                    self._logger.error(f"{e}")

    def thread_wrapper(self, handler: Handler, server: Connection):
        def wrapper(*args, **kwargs):
            response, error = handler(*args, **kwargs)
            if error:
                self._logger.error(f"{handler.name} - {error}")

            try:
                response.send()
                self._logger.info(f"{handler.name} - {response}")
            except Exception as e:
                self._logger.error(f"{handler.name} - {e}")
            finally:
                server.close()

        return wrapper

    def registered(self, bucket: Bucket):
        self._logger.info(f"Bucket registered: {bucket}")
        self._buckets[bucket.id] = bucket

    def unregistered(self, id: str):
        self._buckets.pop(id, None)
        self._logger.info(f"Bucket unregistered: {id}")

    def updated(self, id: str, spec: BucketSpec):
        self._logger.info(f"Bucket updated: {id} - {spec}")
        self._buckets[id].spec = spec


def main():
    """Run the manager."""
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="[%(asctime)s] %(levelname)s (%(name)s): %(message)s",
    )

    subject = BucketSubject("localhost", 3000, logging.getLogger("subject"))

    thread = Thread(target=lambda: subject.listen())
    thread.start()

    manager = Manager("localhost", 8000, subject, logging.getLogger("manager"))
    manager.run()
