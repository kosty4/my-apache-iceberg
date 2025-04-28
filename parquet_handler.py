
import pandas as pd

class ParquetHandler:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def read_parquet(self, columns=None) -> pd.DataFrame:
        """Reads a Parquet file and returns a DataFrame."""
        try:
            df = pd.read_parquet(self.file_path, columns=columns)
            print(f"Successfully read: {self.file_path}")
            return df
        except Exception as e:
            print(f"Error reading Parquet file: {e}")
            return pd.DataFrame()

    def write_parquet(self, df: pd.DataFrame, overwrite: bool = True):
        """Writes a DataFrame to a Parquet file."""
        try:
            if not overwrite:
                raise FileExistsError("Overwrite is disabled. File exists.")
            df.to_parquet(self.file_path, index=False)
            print(f"Successfully written to: {self.file_path}")
        except Exception as e:
            print(f"Error writing Parquet file: {e}")