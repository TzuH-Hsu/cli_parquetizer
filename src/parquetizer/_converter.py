from io import BytesIO

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
        total=4,
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

        # Step 3: Reset buffer cursor to the beginning for reading
        parquet_buffer.seek(0)
        convert_progress.update(1)

        # Step 4: Verify the converted data by reading it back into a DataFrame
        verify_df = pd.read_parquet(parquet_buffer)
        if not csv_df.equals(verify_df):
            msg = "Parquet conversion verification failed"
            raise ValueError(msg)
        convert_progress.update(1)

        # Reset buffer cursor to the beginning before returning
        parquet_buffer.seek(0)

    return parquet_buffer
