from pathlib import Path


def get_directory_size(directory_path: Path) -> int:
    """Get the total size of a directory from it path.

    Args:
        directory_path (Path): The path to the directory.

    Returns:
        int: The total size of the directory in bytes.

    Raises:
        ValueError: If the path is not a directory.
    """
    if not directory_path.is_dir():
        raise ValueError("The path is not a directory.")

    total_size = 0
    for path in directory_path.rglob("*"):
        if path.is_file():
            total_size += path.stat().st_size
    return total_size
