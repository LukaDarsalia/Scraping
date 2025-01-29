"""
Abstract Base Class for content parsers in the pipeline.

This module provides the base parser functionality for processing scraped content,
with support for parallel processing, checkpointing, and error handling.
All website-specific parsers should inherit from this class and implement
the required abstract methods.
"""

import datetime
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union
from tqdm import tqdm

import pandas as pd

from core.utils import (
    run_processes, merge_temp_files, TEMP_FILE, ERROR,
    save_temp, get_backup_urls, URL
)


class ParserABC(ABC):
    """
    Abstract base class defining the interface and common functionality for content parsers.

    This class implements a parsing framework with features like:
    - Parallel processing support
    - Progress tracking and checkpointing
    - Error handling and logging
    - Automatic backup and recovery

    Attributes:
        checkpoint_time (int): Number of items to process before saving checkpoint
        input_path (str): Path to input file containing scraped content
        raw_data_dir (str): Directory containing raw scraped data files
        output_path (str): Path where parsed results will be saved
        temp_dir (str): Directory for temporary files
        num_processes (int): Number of parallel parsing processes
        logger (logging.Logger): Logger instance for this parser
    """

    def __init__(self,
                 input_path: str,
                 raw_data_dir: str,
                 output_path: str,
                 temp_dir: str,
                 num_processes: int = 4,
                 checkpoint_time: int = 100) -> None:
        """
        Initialize the parser with configuration parameters.

        Args:
            input_path: Path to input file containing scraped content metadata
            raw_data_dir: Directory containing raw scraped data files
            output_path: Path where parsed results will be saved
            temp_dir: Directory for temporary files
            num_processes: Number of parallel parsing processes
            checkpoint_time: Number of items to process before saving checkpoint
        """
        self.checkpoint_time = checkpoint_time
        self.input_path = input_path
        self.raw_data_dir = raw_data_dir
        self.output_path = output_path
        self.temp_dir = temp_dir
        self.num_processes = num_processes

        # Create temporary directory if it doesn't exist
        os.makedirs(self.temp_dir, exist_ok=True)

        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup_logger()

    def setup_logger(self) -> None:
        """Configure logging for the parser instance."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    @abstractmethod
    def parse_file(self, data: Dict[str, Any]) -> Union[Dict[str, Any], List[Dict[str, Any]], None]:
        """
        Abstract method to parse a single file's content.

        Args:
            data: Dictionary containing metadata about the file to parse,
                 including URL, file path, and any additional metadata

        Returns:
            Parsed data as a dictionary, list of dictionaries for multi-document files,
            or None if parsing fails or should be skipped

        Raises:
            NotImplementedError: Must be implemented by subclasses
            Exception: Any parsing-related errors should be handled by implementation
        """
        pass

    def process_chunk(self,
                      metadata_chunk: pd.DataFrame,
                      temp_file: str) -> None:
        """
        Process a chunk of metadata and save parsed results to a temporary file.

        Args:
            metadata_chunk: DataFrame containing metadata for files to parse
            temp_file: Path where temporary results will be saved

        The method tracks progress and saves checkpoints at regular intervals
        defined by self.checkpoint_time.
        """
        parsed_data: List[Dict[str, Any]] = []
        counter = 0

        for _, row in tqdm(metadata_chunk.iterrows(), total=len(metadata_chunk)):
            # Skip rows with errors from previous pipeline stages
            if row[ERROR]:
                self.logger.warning(f"Skipping row due to previous error: {row}")
                continue

            try:
                parsed_result = self.parse_file(row.to_dict())
                if parsed_result:
                    # Handle both single and multi-document results
                    if isinstance(parsed_result, list):
                        parsed_data.extend(parsed_result)
                    else:
                        parsed_data.append(parsed_result)

                counter += 1
                # Save checkpoint if needed
                if counter % self.checkpoint_time == 0:
                    save_temp(parsed_data, temp_file)
                    parsed_data = []
                    self.logger.info(f"Saved checkpoint metadata for chunk to {temp_file}")

            except Exception as e:
                self.logger.error(f"Error parsing url {row[URL]}: {e}")

        # Save remaining parsed data
        save_temp(parsed_data, temp_file)
        self.logger.info(f"Saved parsed chunk to {temp_file}")

    def run(self) -> None:
        """
        Execute the parsing pipeline with all configured parameters.

        This method:
        1. Loads metadata from input file
        2. Checks for already processed files
        3. Divides work into chunks for parallel processing
        4. Processes chunks using multiple processes
        5. Merges results into final output file

        The method handles both single-process and multi-process scenarios
        efficiently based on the configuration.
        """
        self.logger.info("Starting parsing...")

        try:
            # Load metadata from the input parquet file
            metadata_df = pd.read_parquet(self.input_path)
            urls = metadata_df[URL].tolist()

            # Load backup urls (if exists)
            completed_urls = get_backup_urls(self.output_path, self.temp_dir)

            # Exclude already processed urls
            urls = list(set(urls) - set(completed_urls))
            if not urls:
                self.logger.info("All chunks are already processed. Exiting.")
                return
            else:
                self.logger.info(f"With backup we have to parse {len(urls)} urls!")
            metadata_df = metadata_df[metadata_df[URL].isin(urls)]

            # Calculate chunk size for parallel processing
            chunk_size = len(urls) // self.num_processes
            if chunk_size == 0:
                chunk_size = len(urls)
                self.num_processes = 1

            # Handle single process case
            if self.num_processes == 1:
                self.process_chunk(
                    metadata_df,
                    os.path.join(self.temp_dir, TEMP_FILE(0))
                )
                merge_temp_files(
                    self.temp_dir,
                    self.output_path,
                    'Parser',
                    self.logger
                )
                return

            # Handle multi-process case
            run_processes(
                metadata_df,
                chunk_size,
                self.temp_dir,
                self.num_processes,
                self.process_chunk
            )

            # Merge all temporary files into final output
            merge_temp_files(
                self.temp_dir,
                self.output_path,
                'Parser',
                self.logger
            )

        except Exception as e:
            self.logger.error(f"Error during parsing: {e}")
