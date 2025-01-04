import time
from abc import ABC, abstractmethod
import os
import pandas as pd
import logging
from multiprocessing import Pool


class CrawlerABC(ABC):
    """
    Abstract base class for a web crawler.
    All custom crawlers must inherit from this class and implement the required methods.
    """

    def __init__(self, start_urls, output_path, temp_dir, max_retries=3, time_sleep=3, num_processes=4):
        """
        Initialize the crawler.
        :param start_urls: List of URLs to start crawling from.
        :param output_path: Path where the output parquet file will be saved.
        :param temp_dir: Directory for storing temporary files.
        :param max_retries: Maximum number of retries for failed URLs.
        :param time_sleep: Time to sleep between retries.
        :param num_processes: Number of parallel processes for crawling.
        """
        self.start_urls = start_urls
        self.output_path = output_path
        self.temp_dir = temp_dir
        self.max_retries = max_retries
        self.time_sleep = time_sleep
        self.num_processes = num_processes
        self.visited_urls = set()
        os.makedirs(self.temp_dir, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup_logger()

    def setup_logger(self):
        """Set up logging for the crawler."""
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    @abstractmethod
    def fetch_links(self, url):
        """
        Abstract method to fetch links from a given URL.
        Must be implemented by subclasses.
        :param url: URL to crawl.
        :return: List of URLs found on the page.
        """
        pass

    def fetch_links_with_retries(self, url):
        """
        Fetch links from a URL with retry logic.
        :param url: URL to crawl.
        :return: List of URLs found on the page, or an empty list if all retries fail.
        """
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Fetching links (Attempt {attempt + 1}/{self.max_retries}): {url}")
                return self.fetch_links(url)
            except Exception as e:
                self.logger.error(f"Error fetching links from {url}: {e}")
                time.sleep(self.time_sleep)
            self.logger.info(f"Retrying {url}...")

        self.logger.warning(f"Failed to fetch links from {url} after {self.max_retries} attempts.")
        return []

    def process_chunk(self, chunk, temp_file):
        """
        Process a chunk of URLs and save the results to a temporary file.
        :param chunk: List of URLs to crawl.
        :param temp_file: Path to the temporary file for storing results.
        """
        local_urls = []
        queue = list(chunk)

        while queue:
            url = queue.pop(0)
            if url in self.visited_urls:
                continue

            self.logger.info(f"Crawling: {url}")
            try:
                new_links = self.fetch_links_with_retries(url)
                local_urls.extend(new_links)
                queue.extend(new_links)
                self.visited_urls.add(url)
            except Exception as e:
                self.logger.error(f"Unexpected error crawling {url}: {e}")

        # Save the results for this chunk
        pd.DataFrame({'url': list(set(local_urls))}).to_parquet(temp_file, index=False)
        self.logger.info(f"Saved chunk results to {temp_file}")

    def run(self):
        """
        Entry point to start the crawler using multiprocessing.
        """
        self.logger.info("Starting crawl...")

        # Split the start URLs into chunks
        chunk_size = max(1, len(self.start_urls) // self.num_processes)
        url_chunks = [self.start_urls[i:i + chunk_size] for i in range(0, len(self.start_urls), chunk_size)]
        temp_files = [os.path.join(self.temp_dir, f"temp_crawl_{i}.parquet") for i in range(len(url_chunks))]

        try:
            # Use multiprocessing to process each chunk
            with Pool(self.num_processes) as pool:
                pool.starmap(self.process_chunk, zip(url_chunks, temp_files))

            # Merge all temporary files into the final output
            self.merge_temp_files(temp_files)
        except Exception as e:
            self.logger.error(f"Error during crawl: {e}")

    def merge_temp_files(self, temp_files):
        """
        Merge all temporary files into the final output parquet file.
        :param temp_files: List of paths to the temporary files.
        """
        try:
            all_urls = pd.concat([pd.read_parquet(file) for file in temp_files], ignore_index=True)
            all_urls = all_urls.drop_duplicates(subset=['url'])
            all_urls.to_parquet(self.output_path, index=False)
            self.logger.info(f"Saved final crawled URLs to {self.output_path}")

            # Clean up temporary files
            for file in temp_files:
                os.remove(file)
            self.logger.info("Cleaned up temporary files.")
        except Exception as e:
            self.logger.error(f"Error merging temporary files: {e}")