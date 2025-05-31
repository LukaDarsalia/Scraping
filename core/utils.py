"""
Core utility functions and data structures for the web scraping pipeline.
This module provides common functionality used across crawler, scraper, and parser components.
"""

import subprocess
import datetime
from dataclasses import dataclass
import os
from typing import Union, Optional, List, Callable, Dict, TypeVar, Any
from multiprocessing import Pool
import glob
import pandas as pd
import random

# Type variables for generic type hints
T = TypeVar('T')

# Constants for DataFrame column names and file operations
URL = "URL"  # URL field name in DataFrames
RAW = "raw"  # Raw content field name
FORMAT = "format"  # Content format field name
HEADER = "header"  # Header field name
TEXT = "text"  # Text content field name
CATEGORY = "category"  # Category field name
TIME = "time"  # Timestamp field name
CONTENT = "content"  # Content field name
ERROR = "error"  # Error field name

# Constants for translation datasets
SOURCE_TEXT = "source_text"  # Source language text
TARGET_TEXT = "target_text"  # Target language text
SOURCE_LANG = "source_lang"  # Source language code
TARGET_LANG = "target_lang"  # Target language code
ALIGNMENT_INFO = "alignment_info"  # Alignment information
QUALITY_SCORE = "quality_score"  # Quality score for translation pair
TRANSLATION_ID = "translation_id"  # Unique identifier for translation pair

# Constants for temporary file naming
TEMP_FILE_FORMAT = 'temp_data_*.parquet'  # Pattern for temporary files
TEMP_FILE = lambda i: f'temp_data_{i}.parquet'  # Function to generate temp file names


def html2markdown(html_content: Union[str, bytes]) -> str:
    """
    Convert HTML content to Markdown format using the html2markdown command-line tool.

    Args:
        html_content (Union[str, bytes]): HTML content to convert, either as string or bytes

    Returns:
        str: Converted markdown content

    Raises:
        RuntimeError: If conversion fails or html2markdown command fails
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


def run_processes(data: Union[List[str], pd.DataFrame],
                  chunk_size: int,
                  temp_dir: str,
                  num_processes: int,
                  process_chunk: Callable[[Union[List[str], pd.DataFrame], str], None]) -> None:
    """
    Split data into chunks and process them using multiple processes.

    Args:
        data: List of strings or DataFrame to be processed
        chunk_size: Size of each chunk for processing
        temp_dir: Directory for storing temporary files
        num_processes: Number of parallel processes to use
        process_chunk: Function to process each chunk, should accept (chunk, temp_file_path)

    The function splits the input data into chunks and processes them in parallel,
    saving results to temporary files in the specified directory.
    """
    # Split data into chunks
    url_chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    # Generate temporary file paths for each chunk
    temp_files = [os.path.join(temp_dir, TEMP_FILE(i)) for i in range(len(url_chunks))]

    # Use multiprocessing to process chunks
    with Pool(num_processes) as pool:
        pool.starmap(process_chunk, zip(url_chunks, temp_files))


def merge_temp_files(temp_dir: str, output_path: str, operation: str, logger: Any) -> None:
    """
    Merge temporary parquet files into a single output file.

    Args:
        temp_dir: Directory containing temporary files
        output_path: Path for the merged output file
        operation: Name of the operation (for logging)
        logger: Logger instance for status messages

    The function handles deduplication based on URL and cleanup of temporary files.
    """
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
    """
    Save or append metadata to a temporary parquet file.

    Args:
        local_metadata: List of dictionaries containing metadata
        temp_file: Path to the temporary file

    If the temporary file exists, new data is appended to it.
    """
    temp_df = pd.DataFrame(local_metadata)
    if os.path.exists(temp_file):
        temp_df = pd.concat([pd.read_parquet(temp_file), temp_df])
    temp_df.to_parquet(temp_file, index=False)


def get_backup_urls(output_path: str, temp_dir: str) -> List[str]:
    """
    Retrieve previously processed URLs from output file or temporary files.

    Args:
        output_path: Path to the main output file
        temp_dir: Directory containing temporary files

    Returns:
        List[str]: List of URLs that have been successfully processed

    This function helps resume interrupted operations by identifying already processed URLs.
    """
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


def get_initial_backoff(backoff_min: float, backoff_max: float) -> float:
    """
    Generate an initial backoff time between minimum and maximum values.

    Args:
        backoff_min: Minimum backoff time in seconds
        backoff_max: Maximum backoff time in seconds

    Returns:
        float: Initial backoff time in seconds
    """
    return random.uniform(backoff_min, backoff_max)


def get_backoff_time(attempt: int, initial_backoff: float, backoff_factor: float) -> float:
    """
    Calculate the backoff time for the current attempt using exponential backoff with jitter.

    Args:
        attempt: Current attempt number (0-based)
        initial_backoff: Initial backoff time for this URL
        backoff_factor: Multiplicative factor for exponential growth

    Returns:
        float: Time to wait in seconds

    The function implements exponential backoff with ±10% jitter to prevent synchronized retries.
    """
    # Calculate exponential backoff using the provided initial time
    backoff = initial_backoff * (backoff_factor ** attempt)

    # Add jitter (±10% of the calculated backoff)
    jitter = random.uniform(-0.1 * backoff, 0.1 * backoff)
    return backoff + jitter


@dataclass
class ParsedData:
    """
    Data structure for storing parsed content from web pages.

    Attributes:
        URL: Source URL of the content
        raw: Raw content bytes
        format: Format of the content (e.g., 'html', 'json')
        header: Optional page header or title
        text: Extracted text content (for monolingual data)
        category: Optional list of content categories
        time: Optional timestamp of the content
        error: Optional error message if parsing failed

        # Translation-specific fields
        source_text: Source language text for translation pairs
        target_text: Target language text for translation pairs
        source_lang: Source language code (e.g., 'en')
        target_lang: Target language code (e.g., 'ka')
        alignment_info: Optional alignment information between source and target
        quality_score: Optional quality score for the translation pair
        translation_id: Unique identifier for the translation pair
    """
    URL: str
    raw: bytes
    format: str
    header: Optional[str]
    text: str
    category: Optional[List[str]]
    time: Optional[datetime.datetime]
    error: Optional[str] = None

    # Translation-specific fields
    source_text: Optional[str] = None
    target_text: Optional[str] = None
    source_lang: Optional[str] = None
    target_lang: Optional[str] = None
    alignment_info: Optional[Dict[str, Any]] = None
    quality_score: Optional[float] = None
    translation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the ParsedData instance to a dictionary format."""
        return {
            URL: self.URL,
            RAW: self.raw,
            FORMAT: self.format,
            TEXT: self.text,
            HEADER: self.header,
            CATEGORY: self.category,
            TIME: self.time,
            ERROR: self.error,
            SOURCE_TEXT: self.source_text,
            TARGET_TEXT: self.target_text,
            SOURCE_LANG: self.source_lang,
            TARGET_LANG: self.target_lang,
            ALIGNMENT_INFO: self.alignment_info,
            QUALITY_SCORE: self.quality_score,
            TRANSLATION_ID: self.translation_id
        }


@dataclass
class TranslationPair:
    """
    Data structure specifically for translation pairs.

    Attributes:
        source_text: Text in the source language
        target_text: Text in the target language
        source_lang: Source language code
        target_lang: Target language code
        alignment_info: Optional word/phrase alignment information
        quality_score: Optional quality score (0.0-1.0)
        domain: Optional domain/category (e.g., 'news', 'legal', 'medical')
        metadata: Optional additional metadata
    """
    source_text: str
    target_text: str
    source_lang: str
    target_lang: str
    alignment_info: Optional[Dict[str, Any]] = None
    quality_score: Optional[float] = None
    domain: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the TranslationPair instance to a dictionary format."""
        return {
            SOURCE_TEXT: self.source_text,
            TARGET_TEXT: self.target_text,
            SOURCE_LANG: self.source_lang,
            TARGET_LANG: self.target_lang,
            ALIGNMENT_INFO: self.alignment_info,
            QUALITY_SCORE: self.quality_score,
            CATEGORY: self.domain,  # Reuse category field for domain
            "metadata": self.metadata
        }


@dataclass
class ScrapeData:
    """
    Data structure for storing scraped content from web pages.

    Attributes:
        url: Source URL
        content: Raw content bytes or None if scraping failed
        content_format: Format of the content or None if unknown
        error: Optional error message if scraping failed
    """
    url: str
    content: Optional[bytes]
    content_format: Optional[str]
    error: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert the ScrapeData instance to a dictionary format."""
        return {
            URL: self.url,
            CONTENT: self.content,
            FORMAT: self.content_format,
            ERROR: self.error,
        }


@dataclass
class CrawlData:
    """
    Data structure for storing crawled URL information.

    Attributes:
        url: Discovered URL
        error: Optional error message if crawling failed
    """
    url: str
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the CrawlData instance to a dictionary format."""
        return {
            URL: self.url,
            ERROR: self.error,
        }
