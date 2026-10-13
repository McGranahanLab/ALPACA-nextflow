import os
import pandas as pd


def read_any_table(file_path: str) -> tuple:
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()
    if ext == ".csv":
        
        return pd.read_csv(file_path), 'csv'
    elif ext in [".tsv", ".txt"]:
        return pd.read_csv(file_path, sep="\t"), 'tsv'
    elif ext == ".parquet":
        return pd.read_parquet(file_path), 'parquet'
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def write_table(df, file_path: str, file_type: str = None) -> None:
    # if file_type is not defined, derive it from the file_path:
    if file_type is None:
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        if ext == ".csv":
            file_type = "csv"
        elif ext in [".tsv", ".txt"]:
            file_type = "tsv"
        elif ext == ".parquet":
            file_type = "parquet"
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

    if file_type == "csv":
        df.to_csv(file_path, index=False)
    elif file_type == "tsv":
        df.to_csv(file_path, sep="\t", index=False)
    elif file_type == "parquet":
        df.to_parquet(file_path, index=False)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

# Extensions the pipeline recognises as data tables when scanning directories.
# Ordered by preference when several files share the same stem.
TABLE_EXTENSIONS = {".parquet": "parquet", ".csv": "csv", ".tsv": "tsv"}


def table_type(file_path: str):
    """Return 'parquet', 'csv' or 'tsv' for a recognised table file, otherwise None."""
    _, ext = os.path.splitext(str(file_path))
    return TABLE_EXTENSIONS.get(ext.lower())


def is_table_file(file_path: str) -> bool:
    return table_type(file_path) is not None


def strip_table_ext(file_name: str) -> str:
    """Remove a recognised table extension from a file name, leaving other names untouched."""
    stem, ext = os.path.splitext(str(file_name))
    return stem if ext.lower() in TABLE_EXTENSIONS else str(file_name)


def with_table_ext(file_path: str, file_type: str) -> str:
    """Replace (or add) the extension of file_path so it matches file_type."""
    return f"{strip_table_ext(file_path)}.{file_type}"


def find_table(directory: str, stem: str):
    """Return the path of <directory>/<stem>.<ext> for the first existing table extension, or None."""
    for ext in TABLE_EXTENSIONS:
        candidate = os.path.join(directory, f"{stem}{ext}")
        if os.path.isfile(candidate):
            return candidate
    return None


def common_table_type(file_types) -> str:
    """Pick one output format for a set of inputs: keep it if all inputs agree, otherwise fall back to csv."""
    file_types = set(file_types)
    if len(file_types) == 1:
        return file_types.pop()
    return "csv"
