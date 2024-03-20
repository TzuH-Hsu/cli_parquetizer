"""CLI for Parquetizer."""

import logging
import os
from functools import partial

import dotenv
import questionary as q
import urllib3
from tqdm.contrib.concurrent import thread_map

from parquetizer._converter import csv2parquet, lvm2parquet
from parquetizer._source_handler import Local, MinIO, SrcHandler

urllib3.disable_warnings()
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def process_file(
    handler: SrcHandler,
    file: str,
    remove: bool = False,  # noqa: FBT001, FBT002
) -> None:
    """Process the file by reading, converting to Parquet, writing, and removing the original file.

    Args:
        handler (SrcHandler): The source handler.
        file (str): The name of the file.
        remove (bool, optional): Whether to remove the original file. Defaults to False.
    """  # noqa: E501
    try:
        source_buffer = handler.read(file)
        # Determine which converter function to use based on file extension
        if file.lower().endswith(".csv"):
            parquet_buffer = csv2parquet(source_buffer, file.split("/")[-1])
            handler.write(
                file.replace(".csv", ".parquet"),
                parquet_buffer,
            )
        elif file.lower().endswith(".lvm"):
            parquet_buffer, json_buffer = lvm2parquet(
                source_buffer,
                file.split("/")[-1],
            )
            handler.write(
                file.replace(".lvm", ".parquet"),
                parquet_buffer,
            )
            handler.write(
                file.replace(".lvm", ".json"),
                json_buffer,
            )
        else:
            logger.error("Unsupported file type for %s", file)
            return

        if remove:
            handler.remove(file)

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


def _configure_minio(minio_config: dict | None) -> dict:
    if minio_config is None or not q.confirm("Use the same MinIO configuration?").ask():
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
    return minio_config


def main() -> None:
    """Main function for the CLI."""
    source_handler: SrcHandler
    q.print(
        "Welcome to Parquetizer - Your CLI tool for converting various data formats to Parquet!",  # noqa: E501
    )
    minio_config = None
    while True:
        action = q.select("Continue or Exit?", choices=["Continue", "Exit"]).ask()
        if action == "Exit":
            break

        source_type = q.select(
            "Select the file source:",
            choices=["Local", "MinIO"],
        ).ask()
        full_path = q.text("Enter the full path of the directory:").ask()

        if not full_path:
            q.print("Please enter a valid path.")
            continue

        if source_type == "Local":
            source_handler = Local(full_path=full_path)
        elif source_type == "MinIO":
            minio_config = _configure_minio(minio_config)
            source_handler = MinIO(full_path=full_path, **minio_config)

        extension = q.select("Select the file format:", choices=[".csv", ".lvm"]).ask()
        files = source_handler.list_filtered_objects(extension)
        logger.debug("Found", extra={"files": files})
        q.print(f"Found {len(files)} files with the extension {extension}.")

        remove = q.confirm("Do you want to remove the original files?").ask()

        if not q.confirm("Do you want to continue?").ask():
            continue

        try:
            n_workers = int(
                q.text("Enter the number of workers (default: 10):").ask() or 10,
            )
            if n_workers < 1:
                raise ValueError  # noqa: TRY301
        except (ValueError, AssertionError):
            logger.exception("Invalid number of workers.")
            n_workers = 10
        max_workers = min(
            n_workers,
            10,
        )  # urllib3 default maximum of 10 ConnectionPool instances.
        q.print(f"Using number of workers: {max_workers}")

        if not q.confirm(
            f"Confirm to convert {len(files)} {extension} files to Parquet?",
        ).ask():
            continue

        thread_map(
            partial(process_file, source_handler, remove=remove),
            files,
            max_workers=max_workers,
            colour="green",
        )


if __name__ == "__main__":
    dotenv.load_dotenv()
    main()
