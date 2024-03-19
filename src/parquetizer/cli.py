"""CLI for Parquetizer."""

import logging
import os

import dotenv
import questionary as q
from tqdm.contrib.concurrent import thread_map

from parquetizer._converter import csv2parquet
from parquetizer._source_handler import MinIO, SrcHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_file(source: SrcHandler, file: str) -> None:
    """Process the file by reading, converting to Parquet, writing, and removing the original file.

    Args:
        source (SrcHandler): The source handler.
        file (str): The name of the file.
    """  # noqa: E501
    try:
        # Read
        source_buffer = source.read(file)

        # Convert to Parquet
        parquet_buffer = csv2parquet(source_buffer, file.split("/")[-1])

        # Write
        source.write(file.replace(".csv", ".parquet"), parquet_buffer)

        # Remove original file
        source.remove(file)

    except Exception:
        logger.exception("Error processing %s", file)


def _get_env_or_ask(
    env_var_name: str,
    message: str,
    is_password: bool = False,  # noqa: FBT001, FBT002
) -> str | None:
    value = os.getenv(env_var_name)
    if not value:
        prompt_function = q.password if is_password else q.text
        value = prompt_function(message).ask()
    return value


def main() -> None:  # noqa: C901
    """Main function for the CLI."""
    minio_config = None
    q.print(
        "Welcome to Parquetizer - Your CLI tool for converting various data formats to Parquet!",  # noqa: E501
    )
    while True:
        if q.select("Continue or Exit?", choices=["Continue", "Exit"]).ask() == "Exit":
            break

        source = q.select("Select the file source:", choices=["MinIO"]).ask()

        if source == "MinIO":
            if minio_config is not None:  # noqa: SIM102
                if not q.confirm(
                    "Do you want to use the same MinIO configuration?",
                ).ask():
                    minio_config = None

            if minio_config is None:
                minio_config = {
                    "endpoint": _get_env_or_ask("MINIO_URL", "Enter MinIO URL:"),
                    "access_key": _get_env_or_ask(
                        "MINIO_ACCESS_KEY",
                        "Enter MinIO Access Key:",
                    ),
                    "secret_key": _get_env_or_ask(
                        "MINIO_SECRET_KEY",
                        "Enter MinIO Secret Key:",
                        is_password=True,
                    ),
                    "secure": q.confirm("Use secure connection?").ask(),
                    "cert_check": q.confirm("Check server certificate?").ask(),
                }

            full_path = q.text("Enter the full path of the directory:").ask()
            if not full_path:
                q.print("Please enter a valid path.")
                continue

            source_handler = MinIO(full_path=full_path, **minio_config)

        extension = q.select("Select the file format:", choices=[".csv"]).ask()
        files = source_handler.list_filtered_objects(extension)
        logger.debug("Found", extra={"files": files})
        q.print(f"Found {len(files)} files with the extension {extension}.")

        if not q.confirm("Do you want to continue?").ask():
            continue

        n_workers = q.text("Enter the number of workers:").ask()
        default_workers = max(32, os.cpu_count() + 4)  # type: ignore[operator]
        max_workers = int(n_workers) if n_workers else default_workers
        q.print(f"Using number of workers: {max_workers}")

        if not q.confirm(
            f"Confirm to convert {len(files)} {extension} files to Parquet?",
        ).ask():
            continue

        thread_map(
            process_file,
            [(source_handler, file) for file in files],
            max_workers=max_workers,
            colour="green",
        )

        extension = q.select("Select the file format:", choices=[".csv"]).ask()
        files = source_handler.list_filtered_objects(extension)
        logger.debug("Found", extra={"files": files})
        q.print(f"Found {len(files)} files with the extension {extension}.")

        if not q.confirm("Do you want to continue?").ask():
            continue

        n_workers = q.text("Enter the number of workers:").ask()

        if not n_workers:
            q.print(f"Using default number of workers: {max(32, os.cpu_count() + 4)}")  # type: ignore[operator]

        if not q.confirm(
            f"Confirm to convert {len(files)} {extension} files to Parquet?",
        ).ask():
            continue

        thread_map(
            lambda file: process_file(source_handler, file),  # noqa: B023
            files,
            max_workers=int(n_workers) if n_workers else None,
            colour="green",
        )


if __name__ == "__main__":
    dotenv.load_dotenv()
    main()
