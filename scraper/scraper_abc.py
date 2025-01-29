"""
Abstract Base Class for web scrapers in the pipeline.

This module provides the base scraper functionality with robust error handling,
retry mechanisms with exponential backoff, and multiprocessing capabilities.
All website-specific scrapers should inherit from this class and implement
the required abstract methods.
"""

import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Tuple, List, Dict, Any

import pandas as pd
from tqdm import tqdm

from core.utils import (
    URL, ScrapeData, run_processes, merge_temp_files, TEMP_FILE,
    save_temp, get_backup_urls, get_initial_backoff, get_backoff_time
)


class ScraperABC(ABC):
    """
    Abstract base class defining the interface and common functionality for web scrapers.

    This class implements a robust scraping framework with features like:
    - Exponential backoff retry mechanism
    - Multiprocessing support
    - Progress tracking and checkpointing
    - Error handling and logging

    Attributes:
        checkpoint_time (int): Number of items to process before saving checkpoint
        input_path (str): Path to input file containing URLs to scrape
        output_path (str): Path where scraped data will be saved
        temp_dir (str): Directory for temporary files
        max_retries (int): Maximum number of retry attempts
        backoff_min (float): Minimum initial backoff time in seconds
        backoff_max (float): Maximum initial backoff time in seconds
        backoff_factor (float): Multiplicative factor for exponential backoff
        num_processes (int): Number of parallel scraping processes
        logger (logging.Logger): Logger instance for this scraper
    """

    def __init__(self,
                 input_path: str,
                 output_path: str,
                 temp_dir: str,
                 backoff_min: float = 1,
                 backoff_max: float = 5,
                 backoff_factor: float = 2,
                 max_retries: int = 3,
                 num_processes: int = 4,
                 checkpoint_time: int = 100) -> None:
        """
        Initialize the scraper with configuration parameters.

        Args:
            input_path: Path to input file containing URLs to scrape
            output_path: Path where scraped data will be saved
            temp_dir: Directory for temporary files
            backoff_min: Minimum initial backoff time in seconds
            backoff_max: Maximum initial backoff time in seconds
            backoff_factor: Multiplicative factor for exponential backoff
            max_retries: Maximum number of retry attempts
            num_processes: Number of parallel scraping processes
            checkpoint_time: Number of items to process before saving checkpoint
        """
        self.checkpoint_time = checkpoint_time
        self.input_path = input_path
        self.output_path = output_path
        self.temp_dir = temp_dir
        self.max_retries = max_retries
        self.backoff_min = backoff_min
        self.backoff_max = backoff_max
        self.backoff_factor = backoff_factor
        self.num_processes = num_processes

        # Create temporary directory if it doesn't exist
        os.makedirs(self.temp_dir, exist_ok=True)

        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup_logger()

    def setup_logger(self) -> None:
        """Configure logging for the scraper instance."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    @abstractmethod
    def scrape_url(self, url: str) -> Tuple[str, bytes]:
        """
        Abstract method to scrape a single URL.

        Args:
            url: URL to scrape

        Returns:
            Tuple containing:
                - Content format (e.g., 'html', 'json')
                - Raw content bytes

        Raises:
            NotImplementedError: Must be implemented by subclasses
            Exception: Any scraping-related errors
        """
        pass

    def scrape_with_retries(self, url: str) -> ScrapeData:
        """
        Attempt to scrape a URL with automatic retries and exponential backoff.

        Args:
            url: URL to scrape

        Returns:
            ScrapeData object containing either the scraped content or error information

        The method implements exponential backoff with jitter to handle failures gracefully
        and avoid overwhelming target servers.
        """
        # Generate initial backoff time for this URL
        initial_backoff = get_initial_backoff(self.backoff_min, self.backoff_max)

        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Scraping (Attempt {attempt + 1}/{self.max_retries}): {url}")
                content_format, file_content = self.scrape_url(url)
                return ScrapeData(
                    url=url,
                    content=file_content,
                    content_format=content_format,
                    error=None
                )
            except Exception as e:
                self.logger.error(f"Error scraping {url}: {e}")
                backoff_time = get_backoff_time(attempt, initial_backoff, self.backoff_factor)
                self.logger.info(f"Backing off for {backoff_time:.2f} seconds before retry...")
                time.sleep(backoff_time)
                self.logger.info(f"Retrying {url}...")

        self.logger.warning(f"Failed to scrape {url} after {self.max_retries} attempts.")
        return ScrapeData(
            url=url,
            error="Failed after retries",
            content=None,
            content_format=None
        )

    def process_chunk(self, urls: List[str], temp_file: str) -> None:
        """
        Process a chunk of URLs and save results to a temporary file.

        Args:
            urls: List of URLs to process
            temp_file: Path where temporary results will be saved

        The method tracks progress and saves checkpoints at regular intervals
        defined by self.checkpoint_time.
        """
        local_metadata: List[Dict[str, Any]] = []
        counter = 0

        for url in tqdm(urls):
            result = self.scrape_with_retries(url)
            local_metadata.append(result.to_dict())
            counter += 1

            # Save checkpoint if needed
            if counter % self.checkpoint_time == 0:
                save_temp(local_metadata, temp_file)
                local_metadata = []
                self.logger.info(f"Saved checkpoint metadata for chunk to {temp_file}")

        # Save remaining metadata
        save_temp(local_metadata, temp_file)
        self.logger.info(f"Saved metadata for chunk to {temp_file}")

    def run(self) -> None:
        """
        Execute the scraping pipeline with all configured parameters.

        This method:
        1. Loads URLs from input file
        2. Checks for already processed URLs
        3. Divides work into chunks for parallel processing
        4. Processes chunks using multiple processes
        5. Merges results into final output file

        The method handles both single-process and multi-process scenarios
        efficiently based on the configuration.
        """
        self.logger.info("Starting scraping...")
        try:
            # Load URLs from the input parquet file
            urls = pd.read_parquet(self.input_path)[URL].tolist()

            # Load backup urls (if exists)
            completed_urls = get_backup_urls(self.output_path, self.temp_dir)

            # Exclude already done urls
            urls = list(set(urls) - set(completed_urls))
            if not urls:
                self.logger.info("All chunks are already processed. Exiting.")
                return
            else:
                self.logger.info(f"With backup we have to scrape {len(urls)} urls!")

            # Calculate chunk size for parallel processing
            chunk_size = len(urls) // self.num_processes
            if chunk_size == 0:
                chunk_size = len(urls)
                self.num_processes = 1

            # Handle single process case
            if self.num_processes == 1:
                self.process_chunk(urls, os.path.join(self.temp_dir, TEMP_FILE(0)))
                merge_temp_files(
                    self.temp_dir,
                    self.output_path,
                    'Scraper',
                    self.logger
                )
                return

            # Handle multi-process case
            run_processes(
                urls,
                chunk_size,
                self.temp_dir,
                self.num_processes,
                self.process_chunk
            )

            # Merge all temporary files into final output
            merge_temp_files(
                self.temp_dir,
                self.output_path,
                'Scraper',
                self.logger
            )

        except Exception as e:
            self.logger.error(f"Error running scraper: {e}")
