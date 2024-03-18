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


def main() -> None:
    """Main function for the CLI."""
    while True:
        q.print(
            "Welcome to Parquetizer \
            - Your CLI tool for converting various data formats to Parquet!",
        )
        if q.select("Continue or Exit?", choices=["Continue", "Exit"]).ask() == "Exit":
            break
        source = q.select("Select the file source:", choices=["MinIO"]).ask()

        if source == "MinIO":
            if os.getenv("MINIO_URL"):
                minio_url = os.getenv("MINIO_URL")
                q.print(f"Using minio url from env variable: {minio_url}")
            else:
                minio_url = q.text("Enter MinIO URL:").ask()
            if os.getenv("MINIO_ACCESS_KEY"):
                minio_access_key = os.getenv("MINIO_ACCESS_KEY")
                q.print(
                    f"Using minio access key from env variable: {minio_access_key}",
                )
            else:
                minio_access_key = q.text("Enter MinIO Access Key:").ask()
            if os.getenv("MINIO_SECRET_KEY"):
                minio_secret_key = os.getenv("MINIO_SECRET_KEY")
                q.print("Using minio secret key from env variable: ********")
            else:
                minio_secret_key = q.password("Enter MinIO Secret Key:").ask()
            minio_secure = q.confirm("Use secure connection?").ask()
            full_path = q.text("Enter the full path of the directory:").ask()

            source_handler = MinIO(
                full_path=full_path,
                endpoint=minio_url,  # type: ignore[arg-type]
                access_key=minio_access_key,
                secret_key=minio_secret_key,
                secure=minio_secure,
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
