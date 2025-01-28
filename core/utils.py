import subprocess
import datetime
from dataclasses import dataclass
import os
from typing import Union, Optional, List, Callable, Dict
from multiprocessing import Pool
import glob
import pandas as pd

URL="URL"
RAW="raw"
FORMAT="format"
HEADER="header"
TEXT="text"
CATEGORY="category"
TIME="time"
CONTENT="content"
ERROR="error"
TEMP_FILE_FORMAT='temp_data_*.parquet'
TEMP_FILE=lambda i:f'temp_data_{i}.parquet'


def html2markdown(html_content: Union[str, bytes]) -> str:
    """
    Convert HTML content to Markdown format using html2markdown command.

    Args:
        html_content: HTML content as string or bytes

    Returns:
        Markdown formatted string

    Raises:
        RuntimeError: If conversion fails
    """
    try:
        # Ensure content is string
        if isinstance(html_content, bytes):
            html_content = html_content.decode('utf-8')

        result = subprocess.run(
            ["html2markdown"],
            input=html_content,
            text=True,
            capture_output=True
        )

        if result.returncode == 0:
            return result.stdout.strip()
        else:
            raise RuntimeError(f"html2markdown failed: {result.stderr}")

    except Exception as e:
        raise RuntimeError(f"Error converting HTML to Markdown: {str(e)}")


def run_processes(data: List[str] | pd.DataFrame, chunk_size: int, temp_dir: str, num_processes: int, process_chunk: Callable):
    # Split data into chunks
    url_chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    # Generate temporary file paths for each chunk
    temp_files = [os.path.join(temp_dir, TEMP_FILE(i)) for i in range(len(url_chunks))]

    # Use multiprocessing to process chunks
    with Pool(num_processes) as pool:
        pool.starmap(process_chunk, zip(url_chunks, temp_files))


def merge_temp_files(temp_dir: str, output_path: str, operation: str, logger):
    try:
        temp_files = glob.glob(f"{temp_dir}/{TEMP_FILE_FORMAT}")
        all_data = pd.concat([pd.read_parquet(file) for file in temp_files], ignore_index=True)
        if os.path.exists(output_path):
            out_pd = pd.read_parquet(output_path)
            all_data = pd.concat([all_data, out_pd], ignore_index=True)
            all_data = all_data.drop_duplicates(subset=[URL])

        all_data.to_parquet(output_path, index=False)
        logger.info(f"Saved final {operation} data to {output_path}")

        # Clean up temporary files
        for file in temp_files:
            os.remove(file)
        logger.info("Cleaned up temporary files.")
    except Exception as e:
        logger.error(f"Error merging temporary files: {e}")


def save_temp(local_metadata: List[Dict], temp_file: str) -> None:
    temp_df = pd.DataFrame(local_metadata)
    if os.path.exists(temp_file):
        temp_df = pd.concat([pd.read_parquet(temp_file), temp_df])
    temp_df.to_parquet(temp_file, index=False)


def get_backup_urls(output_path, temp_dir):
    if os.path.exists(output_path):
        out_pd = pd.read_parquet(output_path)
        if len(out_pd) == 0:
            return []
        return out_pd[out_pd[ERROR].isna()][URL].tolist()

    files = glob.glob(f"{temp_dir}/{TEMP_FILE_FORMAT}")
    if len(files) > 0:
        out_pd = pd.concat([pd.read_parquet(file_i) for file_i in files])
        if len(out_pd) == 0:
            return []
        return out_pd[out_pd[ERROR].isna()][URL].tolist()
    return []


@dataclass
class ParsedData:
    """
    Data structure for parsed content.
    """
    URL: str
    raw: bytes
    format: str
    header: Optional[str]
    text: str
    category: Optional[List[str]]
    time: Optional[datetime.datetime]
    error: str | None = None


    def to_dict(self):
        return {
            URL: self.URL,
            RAW: self.raw,
            FORMAT: self.format,
            TEXT: self.text,
            HEADER: self.header,
            CATEGORY: self.category,
            TIME: self.time,
            ERROR: self.error
        }

@dataclass
class ScrapeData:
    url: str
    content: bytes | None
    content_format: str | None
    error: str | None

    def to_dict(self):
        return {
            URL: self.url,
            CONTENT: self.content,
            FORMAT: self.content_format,
            ERROR: self.error,
        }

@dataclass
class CrawlData:
    url: str
    error: str | None = None

    def to_dict(self):
        return {
            URL: self.url,
            ERROR: self.error,
        }
