# c9

A toy distributed file management system.

## About

This project was a requirement for the Distributed Systems discipline in my Computer Science graduation. It uses sockets to simulate a distributed file management application with localization transparency and automatic backups.

## Requirements

- Python `3.12.*`
- pip `24.*`

## Installing

To install and try out this project,

- optionally, create and activate a virtual environment:

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

- install the project:

    ```bash
    pip install .
    ```

- Now run

    ```bash
    manager
    ```

    and

    ```bash
    buckets
    ```

    in different terminals and *in that order*. This will start the manager and the file servers.

Note that a directory called `.data` was added to the working directory. This is where the buckets will keep the managed files.

## Testing

To see the available commands, run

```bash
c9 --help
```
