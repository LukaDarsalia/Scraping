import logging
import os
import random
import time
from abc import ABC, abstractmethod
from typing import Tuple

import pandas as pd
from tqdm import tqdm

from core.utils import URL, ScrapeData, run_processes, merge_temp_files, TEMP_FILE, save_temp, \
    get_backup_urls


class ScraperABC(ABC):
    def __init__(self, input_path, output_path, temp_dir, backoff_min=1, backoff_max=5, backoff_factor=2,
                 max_retries=3, sleep_time=2, num_processes=4, checkpoint_time=100):
        """
        Initialize the scraper.
        :param temp_dir: Directory for storing temporary files.
        :param sleep_time: Time to sleep between retries.
        :param input_path: Path to the input parquet file containing URLs.
        :param output_path: Path where the output parquet file (metadata) will be saved.
        :param max_retries: Maximum number of retries for failed URLs.
        :param backoff_min: Minimum initial backoff time in seconds.
        :param backoff_max: Maximum initial backoff time in seconds.
        :param backoff_factor: Multiplicative factor for exponential backoff.
        :param num_processes: Number of parallel processes for scraping.
        :param checkpoint_time: Saves checkpoint in that steps.
        """
        self.checkpoint_time = checkpoint_time
        self.sleep_time = sleep_time
        self.input_path = input_path
        self.output_path = output_path
        self.temp_dir = temp_dir
        self.max_retries = max_retries
        self.backoff_min = backoff_min
        self.backoff_max = backoff_max
        self.backoff_factor = backoff_factor
        self.num_processes = num_processes
        os.makedirs(self.temp_dir, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup_logger()

    def setup_logger(self):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    @abstractmethod
    def scrape_url(self, url) -> Tuple[str, bytes]:
        """
        Abstract method to scrape a single URL.
        Must be implemented by subclasses.
        """
        pass

    def get_initial_backoff(self):
        """
        Generate an initial backoff time between min and max values.
        :return: Initial backoff time in seconds
        """
        return random.uniform(self.backoff_min, self.backoff_max)

    def get_backoff_time(self, attempt, initial_backoff):
        """
        Calculate the backoff time for the current attempt using exponential backoff
        with jitter.
        :param attempt: Current attempt number (0-based)
        :param initial_backoff: Initial backoff time for this URL
        :return: Time to wait in seconds
        """
        # Calculate exponential backoff using the provided initial time
        backoff = initial_backoff * (self.backoff_factor ** attempt)

        # Add jitter (±10% of the calculated backoff)
        jitter = random.uniform(-0.1 * backoff, 0.1 * backoff)
        return backoff + jitter

    def scrape_with_retries(self, url) -> ScrapeData:
        # Generate initial backoff time for this URL
        initial_backoff = self.get_initial_backoff()

        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Scraping (Attempt {attempt + 1}/{self.max_retries}): {url}")
                content_format, file_content = self.scrape_url(url)
                return ScrapeData(url=url,
                                  content=file_content,
                                  content_format=content_format,
                                  error=None,)
            except Exception as e:
                self.logger.error(f"Error scraping {url}: {e}")
                backoff_time = self.get_backoff_time(attempt, initial_backoff)
                self.logger.info(f"Backing off for {backoff_time:.2f} seconds before retry...")
                time.sleep(backoff_time)
            self.logger.info(f"Retrying {url}...")

        self.logger.warning(f"Failed to scrape {url} after {self.max_retries} attempts.")
        return ScrapeData(url=url, error="Failed after retries", content=None, content_format=None, )

    def process_chunk(self, urls, temp_file):
        """
        Process a chunk of URLs and save the metadata to a temporary file.
        """
        local_metadata = []
        counter = 0
        for url in tqdm(urls):
            result = self.scrape_with_retries(url)
            local_metadata.append(result.to_dict())
            counter += 1
            if counter % self.checkpoint_time == 0:
                save_temp(local_metadata, temp_file)
                local_metadata = []
                self.logger.info(f"Saved checkpoint metadata for chunk to {temp_file}")
        save_temp(local_metadata, temp_file)
        self.logger.info(f"Saved metadata for chunk to {temp_file}")

    def run(self):
        """
        Core scraping logic: read input URLs, scrape data, and save metadata using multiprocessing.
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

            # Get each chunk size
            chunk_size = len(urls) // self.num_processes
            if chunk_size == 0:
                chunk_size = len(urls)
                self.num_processes = 1

            # Handle one process
            if self.num_processes == 1:
                self.process_chunk(urls, os.path.join(self.temp_dir, TEMP_FILE(0)))
                merge_temp_files(self.temp_dir,
                                 self.output_path,
                                 'Scraper',
                                 self.logger)
                return

            run_processes(urls, chunk_size, self.temp_dir, self.num_processes, self.process_chunk)
            # Merge temporary files into the final output
            merge_temp_files(self.temp_dir,
                             self.output_path,
                             'Scraper',
                             self.logger)
        except Exception as e:
            self.logger.error(f"Error running scraper: {e}")
