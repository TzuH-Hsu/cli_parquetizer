import json
from io import BytesIO, StringIO

import pandas as pd
from tqdm.auto import tqdm


def csv2parquet(buffer: BytesIO, file_name: str) -> BytesIO:
    """Converts csv to parquet and verifies the conversion.

    Args:
        buffer (BytesIO): The buffer containing the csv file.
        file_name (str): The name of the csv file.

    Returns:
        BytesIO: The buffer containing the parquet file if conversion is verified.
    """
    with tqdm(
        total=3,
        desc=f"Converting {file_name}",
        colour="yellow",
        leave=False,
    ) as convert_progress:
        # Step 1: Read CSV from buffer
        buffer.seek(0)
        csv_df = pd.read_csv(buffer)
        convert_progress.update(1)

        # Step 2: Convert DataFrame to Parquet format
        parquet_buffer = BytesIO()
        csv_df.to_parquet(parquet_buffer, compression="gzip")
        convert_progress.update(1)

        # Step 3: Verify the converted data by reading it back into a DataFrame
        # NOTE - Not sure if this is necessary
        verify_df = pd.read_parquet(parquet_buffer)
        if not csv_df.equals(verify_df):
            msg = "Parquet conversion verification failed"
            raise ValueError(msg)
        convert_progress.update(1)

        # Reset the buffer position to the beginning
        parquet_buffer.seek(0)

    return parquet_buffer


def lvm2parquet(buffer: BytesIO, file_name: str) -> tuple[BytesIO, BytesIO]:
    """Converts lvm to parquet and saves metadata as JSON.

    Args:
        buffer (BytesIO): The buffer containing the lvm file.
        file_name (str): The name of the lvm file.

    Returns:
        BytesIO: The buffer containing the parquet file.
    """
    with tqdm(
        total=3,
        desc=f"Converting {file_name}",
        colour="yellow",
        leave=False,
    ) as convert_progress:
        # Step 1: Read LVM data and metadata from buffer
        buffer.seek(0)
        f_content = buffer.read().decode("utf-8")
        header_end_token = "***End_of_Header***"  # noqa: S105 # nosec

        # Find the second occurrence of the header end token
        header_end_idx1 = f_content.find(header_end_token) + len(header_end_token)
        header_end_idx2 = f_content.find(header_end_token, header_end_idx1) + len(
            header_end_token,
        )

        metadata_str, data_str = (
            f_content[:header_end_idx2],
            f_content[(header_end_idx2 + 1) :],
        )
        metadata_lines = metadata_str.splitlines()
        metadata: dict = {}

        for line in metadata_lines:
            if line.strip() == header_end_token:
                continue
            if "\t" in line:
                parts = line.split("\t")
                key = parts.pop(0).strip()
                if len(parts) == 1:
                    metadata[key] = parts[0].strip()
                else:
                    metadata[key] = [part.strip() for part in parts]
        convert_progress.update(1)

        # Step 2: Convert the data string to DataFrame and then to Parquet format
        lvm_df = pd.read_csv(StringIO(data_str), sep="\t", header=1)
        parquet_buffer = BytesIO()
        lvm_df.to_parquet(parquet_buffer, compression="gzip")
        convert_progress.update(1)

        # Step 3: Save the metadata to a JSON file
        json_buffer = BytesIO()
        json_bytes = json.dumps(metadata, indent=4).encode("utf-8")
        json_buffer.write(json_bytes)
        convert_progress.update(1)

        # Reset the buffer position to the beginning
        parquet_buffer.seek(0)
        json_buffer.seek(0)

    return parquet_buffer, json_buffer
