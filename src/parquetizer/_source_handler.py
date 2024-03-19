# ruff: noqa: PLR0913, FBT001, FBT002
import logging
import os
from abc import ABC, abstractmethod
from io import BytesIO
from pathlib import Path

from minio import Minio
from minio.error import S3Error
from tqdm.auto import tqdm

from parquetizer._utils import Progress

logger = logging.getLogger(__name__)


class SrcHandler(ABC):
    @abstractmethod
    def list_filtered_objects(self, extension: str) -> list[str]:
        pass

    @abstractmethod
    def read(self, object_name: str) -> BytesIO:
        pass

    @abstractmethod
    def write(self, object_name: str, buffer: BytesIO) -> None:
        pass

    @abstractmethod
    def remove(self, object_name: str) -> None:
        pass


class Local(SrcHandler):
    def __init__(self, full_path: str) -> None:
        self.path = Path(full_path)

    def list_filtered_objects(self, extension: str) -> list[str]:
        """Lists all the objects in the path with the specified extension.

        Args:
            path (str): The path of the directory.
            extension (str): The extension of the objects.

        Returns:
            list[str]: The list of object names.
        """
        return [file for file in os.listdir(self.path) if file.endswith(extension)]

    def read(self, file: str) -> BytesIO:
        file_path = self.path / file
        logger.debug("Reading %s", file)
        buffer = BytesIO()

        file_size = file_path.stat().st_size
        with file_path.open("rb") as f:
            for chunk in tqdm(
                iter(lambda: f.read(4096), b""),
                total=file_size,
                unit="B",
                unit_scale=True,
                desc=f"Reading {file}",
                colour="blue",
                leave=False,
            ):
                buffer.write(chunk)
        return buffer

    def write(self, file: str, buffer: BytesIO) -> None:
        file_path = self.path / file
        logger.debug("Writing %s", file)
        buffer_size = buffer.getbuffer().nbytes

        with file_path.open("wb") as f:
            for chunk in tqdm(
                iter(lambda: buffer.read(4096), b""),
                total=buffer_size,
                unit="B",
                unit_scale=True,
                desc=f"Writing {file}",
                colour="magenta",
                leave=False,
            ):
                f.write(chunk)

    def remove(self, file: str) -> None:
        file_path = self.path / file
        logger.debug("Removing %s", file)
        with tqdm(
            total=1,
            desc=f"Deleting {file}",
            colour="red",
            leave=False,
        ) as delete_progress:
            file_path.unlink()
            delete_progress.update(1)


class MinIO(SrcHandler):
    def __init__(
        self,
        full_path: str,
        endpoint: str,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = True,
        cert_check: bool = True,
    ) -> None:
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            cert_check=cert_check,
        )
        self.bucket, self.path = self._extract_bucket_and_path(full_path)

    def _extract_bucket_and_path(self, full_path: str) -> tuple[str, str]:
        """Extracts the bucket name and path from the full path.

        Args:
            full_path (str): The full path of the directory.

        Returns:
            tuple[str, str]: The bucket name and path.
        """
        parts = full_path.split("/", 1)
        return parts[0], parts[1] if len(parts) > 1 else ""

    def list_filtered_objects(self, extension: str) -> list[str]:
        """Lists all the objects in the path with the specified extension.

        Args:
            bucket (str): The name of the bucket.
            extension (str): The extension of the objects.

        Returns:
            list[str]: The list of object names.
        """
        return [
            obj.object_name
            for obj in self.client.list_objects(
                self.bucket,
                prefix=self.path,
                recursive=True,
            )
            if obj.object_name.endswith(extension)
        ]

    def read(self, file: str) -> BytesIO:
        """Downloads a object from the bucket.

        Args:
            file (str): The name of the object.

        Returns:
            BytesIO: The buffer containing the object.
        """
        logger.debug("Downloading %s", file)
        buffer = BytesIO()
        try:
            size = self.client.stat_object(self.bucket, file).size
        except S3Error as e:
            logger.debug(e)
            size = int(
                self.client.get_object(self.bucket, file).getheader("Content-Length"),
            )
        with tqdm(
            total=size,
            unit="B",
            unit_scale=True,
            desc=f'Downloading {file.split("/")[-1]}',
            colour="blue",
            leave=False,
        ) as download_progress:
            for data_chunk in self.client.get_object(self.bucket, file):
                buffer.write(data_chunk)
                download_progress.update(len(data_chunk))
        return buffer

    def write(self, file: str, buffer: BytesIO) -> None:
        """Uploads a buffer to the bucket.

        Args:
            file (str): The name of the object.
            buffer (BytesIO): The buffer containing the object.
            content_type (str): The content type of the object.
        """
        upload_progress = Progress(
            total=buffer.getbuffer().nbytes,
            unit="B",
            unit_scale=True,
            colour="magenta",
            leave=False,
        )
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=file,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/vnd.apache.parquet",
            progress=upload_progress,
        )

    def remove(self, file: str) -> None:
        """Deletes an object from the bucket.

        Args:
            file (str): The name of the object.
        """
        with tqdm(
            total=1,
            desc=f"Deleting {file.split(" / ")[-1]}",
            colour="red",
            leave=False,
        ) as delete_progress:
            self.client.remove_object(self.bucket, file)
            delete_progress.update(1)
