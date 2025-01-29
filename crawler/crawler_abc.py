"""
Abstract Base Class for web crawlers in the pipeline.

This module provides the base crawler functionality with features like:
- Parallel processing with worker pools
- URL deduplication
- Progress tracking
- Exponential backoff retry mechanism
- Checkpointing and recovery

All website-specific crawlers should inherit from this class and implement
the required abstract methods.
"""

import logging
import multiprocessing
import os
import time
from abc import ABC, abstractmethod
from multiprocessing import Manager, Lock, Value, Queue
from queue import Empty
from typing import Tuple, List, Dict

import pandas as pd

from core.utils import (
    merge_temp_files, CrawlData, TEMP_FILE,
    get_initial_backoff, get_backoff_time
)


def worker_loop(
        crawler: 'CrawlerABC',
        task_queue: Queue,
        urls: List[str],
        visited_urls: Dict[str, bool],
        active_workers: Value,
        lock: Lock,
        backoff_min: float,
        backoff_max: float,
        backoff_factor: float,
        max_retries: int
) -> None:
    """
    Worker function for parallel URL crawling.

    This function runs in a separate process and continuously processes URLs
    from the task queue until termination conditions are met.

    Args:
        crawler: Instance of CrawlerABC subclass
        task_queue: Queue containing URLs to process
        urls: Shared list for storing discovered URLs
        visited_urls: Shared dictionary tracking processed URLs
        active_workers: Shared counter of active workers
        lock: Lock for synchronizing access to shared resources
        backoff_min: Minimum initial backoff time in seconds
        backoff_max: Maximum initial backoff time in seconds
        backoff_factor: Multiplicative factor for exponential backoff
        max_retries: Maximum number of retry attempts per URL

    The function implements exponential backoff with jitter for failed requests
    and uses locks to safely manage shared resources across processes.
    """
    while True:
        with lock:
            # First check termination condition
            if task_queue.empty() and active_workers.value == 0:
                break

            # Atomic queue access + worker count update
            try:
                url = task_queue.get_nowait()
                active_workers.value += 1  # Increment INSIDE lock
            except Empty:
                url = None

        if url is None:
            time.sleep(0.1)
            continue

        initial_backoff = get_initial_backoff(backoff_min, backoff_max)

        for attempt in range(max_retries + 1):
            try:
                # Fetch new URLs from current URL
                key_urls, value_urls = crawler.fetch_links(url)

                with lock:
                    # Add new URLs to queue if not visited
                    for new_url in key_urls:
                        if new_url not in visited_urls:
                            task_queue.put(new_url)
                            visited_urls[new_url] = True
                    urls.extend(value_urls)
                    break

            except Exception as e:
                crawler.logger.error(
                    f"Crawling attempt {attempt + 1}/{max_retries} failed for {url}: {str(e)}"
                )
                backoff_time = get_backoff_time(attempt, initial_backoff, backoff_factor)
                crawler.logger.info(f"Backing off for {backoff_time:.2f} seconds before retry...")
                time.sleep(backoff_time)
                crawler.logger.info(f"Retrying {url}...")

        with lock:
            active_workers.value -= 1


class CrawlerABC(ABC):
    """
    Abstract base class defining the interface and common functionality for web crawlers.

    This class implements a crawler framework with features like:
    - Parallel processing with worker pools
    - URL deduplication
    - Progress tracking
    - Exponential backoff retry mechanism
    - Checkpointing and recovery

    Attributes:
        start_urls (List[str]): Initial URLs to start crawling from
        output_path (str): Path where crawled URLs will be saved
        temp_dir (str): Directory for temporary files
        backoff_min (float): Minimum initial backoff time in seconds
        backoff_max (float): Maximum initial backoff time in seconds
        backoff_factor (float): Multiplicative factor for exponential backoff
        max_retries (int): Maximum number of retry attempts
        time_sleep (float): Base time to sleep between retries
        num_processes (int): Number of parallel crawling processes
        checkpoint_time (int): Number of items to process before saving checkpoint
    """

    def __init__(self,
                 start_urls: List[str],
                 output_path: str,
                 temp_dir: str,
                 backoff_min: float = 1,
                 backoff_max: float = 5,
                 backoff_factor: float = 2,
                 max_retries: int = 3,
                 time_sleep: float = 3,
                 num_processes: int = 4,
                 checkpoint_time: int = 100) -> None:
        """
        Initialize the crawler with configuration parameters.

        Args:
            start_urls: List of URLs to start crawling from
            output_path: Path where crawled URLs will be saved
            temp_dir: Directory for temporary files
            backoff_min: Minimum initial backoff time in seconds
            backoff_max: Maximum initial backoff time in seconds
            backoff_factor: Multiplicative factor for exponential backoff
            max_retries: Maximum number of retry attempts
            time_sleep: Base time to sleep between retries
            num_processes: Number of parallel crawling processes
            checkpoint_time: Number of items to process before saving checkpoint
        """
        self.start_urls = start_urls
        self.output_path = output_path
        self.temp_dir = temp_dir
        self.backoff_min = backoff_min
        self.backoff_max = backoff_max
        self.backoff_factor = backoff_factor
        self.max_retries = max_retries
        self.time_sleep = time_sleep
        self.num_processes = num_processes
        self.checkpoint_time = checkpoint_time

        # Create temporary directory if it doesn't exist
        os.makedirs(self.temp_dir, exist_ok=True)
        self.setup_logger()

    def setup_logger(self) -> None:
        """
        Configure logging for the crawler instance.

        Sets up a logger with appropriate format and handlers if they don't exist.
        """
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False

    @property
    def logger(self) -> logging.Logger:
        """Get the logger instance for this crawler."""
        return logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def fetch_links(self, url: str) -> Tuple[List[str], List[str]]:
        """
        Abstract method to fetch and process links from a given URL.

        Args:
            url: URL to fetch links from

        Returns:
            Tuple containing:
                - List of URLs to be added to the crawling queue
                - List of URLs to be saved as discovered content URLs

        Raises:
            NotImplementedError: Must be implemented by subclasses
            Exception: Any crawling-related errors should be handled by implementation
        """
        pass

    def save_temp(self, urls: List[str]) -> None:
        """
        Save list of URLs to a temporary file.

        Args:
            urls: List of URLs to save
        """
        data = [CrawlData(u).to_dict() for u in urls]
        temp_file = str(os.path.join(self.temp_dir, TEMP_FILE(0)))
        pd.DataFrame(data).to_parquet(temp_file, index=False)

    def run(self) -> None:
        """
        Execute the crawling pipeline with parallel processing and failure tolerance.

        This method:
        1. Sets up shared resources for parallel processing
        2. Initializes worker processes
        3. Monitors progress and handles checkpointing
        4. Manages graceful shutdown
        5. Merges results into final output

        The method includes proper error handling and cleanup, even in case
        of interruption.
        """
        self.logger.info("Starting crawl...")

        # Check if output already exists
        if os.path.exists(self.output_path):
            self.logger.info(f"{self.output_path} already exists!")
            self.logger.info("Crawl completed!")
            return

        # Initialize shared resources
        manager = Manager()
        task_queue: Queue = manager.Queue()
        urls: List[str] = manager.list()
        visited_urls: Dict[str, bool] = manager.dict()
        active_workers: Value = manager.Value('i', 0)
        lock: Lock = manager.Lock()

        # Initialize task queue with start URLs
        for url in self.start_urls:
            task_queue.put(url)

        # Prepare worker arguments
        worker_args = (
            self,
            task_queue,
            urls,
            visited_urls,
            active_workers,
            lock,
            self.backoff_min,
            self.backoff_max,
            self.backoff_factor,
            self.max_retries,
        )

        # Handle single process case
        if self.num_processes == 1:
            worker_loop(*worker_args)
            self.save_temp(urls)
            merge_temp_files(
                self.temp_dir,
                self.output_path,
                'Crawler',
                self.logger
            )
            self.logger.info("Crawl completed!")
            return

        # Handle multi-process case
        processes = []
        try:
            current = 0
            # Start worker processes
            for _ in range(self.num_processes):
                p = multiprocessing.Process(target=worker_loop, args=worker_args)
                p.start()
                processes.append(p)

            # Monitor progress and handle checkpointing
            while any(p.is_alive() for p in processes):
                time.sleep(1)
                with lock:
                    qsize = task_queue.qsize()
                    self.logger.info(
                        f"Progress: {len(visited_urls)} processed, "
                        f"{len(urls)} fetched urls {qsize} queued, "
                        f"{active_workers.value} active workers"
                    )

                    # Save checkpoint if needed
                    if current < len(visited_urls) / self.checkpoint_time:
                        current = len(visited_urls) / self.checkpoint_time
                        self.save_temp(urls)

        except KeyboardInterrupt:
            self.logger.warning("Received interrupt, terminating workers...")
        finally:
            # Clean up processes
            for p in processes:
                if p.is_alive():
                    p.terminate()

            # Save final results
            self.save_temp(urls)
            merge_temp_files(
                self.temp_dir,
                self.output_path,
                'Crawler',
                self.logger
            )
            self.logger.info("Crawl completed!")