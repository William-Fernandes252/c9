import socket

import click

from c9.client import commands
from c9.lib.middleware import Connection


@click.group()
def c9():
    pass


@c9.command()
@click.argument("path", type=click.Path(exists=True, readable=True))
@click.argument("filename", type=str)
def upload(path: str, filename: str):
    """Upload a file to the c9 cloud."""
    with Connection(socket.socket()) as client:
        client.socket.connect(("localhost", 8000))
        command = commands.UploadCommand(
            client, arguments=commands.UploadCommandArgs(path, filename)
        )
        result = command.execute()

    click.echo(result, color=True)

    return


@c9.command()
@click.argument("filename", type=click.Path(exists=False, writable=True))
def download(filename: str):
    """Download a file from the c9 cloud."""
    with Connection(socket.socket()) as client:
        client.socket.connect(("localhost", 8000))
        command = commands.DownloadCommand(
            client, arguments=commands.DownloadCommandArgs(filename)
        )
        result = command.execute()

    click.echo(result, color=True)

    return
