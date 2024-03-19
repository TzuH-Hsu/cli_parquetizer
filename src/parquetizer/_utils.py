from tqdm import tqdm


class Progress(tqdm):
    """A modified version of tqdm that meets the requirements of ProgressType in Minio Python SDK."""  # noqa: E501

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def set_meta(self, object_name: str, total_length: int) -> None:
        """Set process meta information.

        Args:
            object_name (str): The name of the object.
            total_length (int): The total length of the object.
        """
        self.set_description_str(f"Uploading {object_name.split('/')[-1]}")
        self.total = total_length
