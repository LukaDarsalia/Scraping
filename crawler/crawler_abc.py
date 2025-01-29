import multiprocessing
import time
import os
from abc import ABC, abstractmethod
from multiprocessing import Manager, Lock
from queue import Empty
import pandas as pd
import logging
from typing import Tuple, List

from core.utils import merge_temp_files, CrawlData, TEMP_FILE, get_initial_backoff, get_backoff_time


def worker_loop(crawler, task_queue, urls, visited_urls, active_workers, lock, backoff_min, backoff_max, backoff_factor, max_retries):
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
                key_urls, value_urls = crawler.fetch_links(url)
                with lock:
                    for new_url in key_urls:
                        if new_url not in visited_urls:
                            task_queue.put(new_url)
                            visited_urls[new_url] = True
                    urls.extend(value_urls)
                    break
            except Exception as e:
                crawler.logger.error(f"Crawling attempt {attempt + 1}/{max_retries} failed for {url}: {str(e)}")
                backoff_time = get_backoff_time(attempt, initial_backoff, backoff_factor)
                crawler.logger.info(f"Backing off for {backoff_time:.2f} seconds before retry...")
                time.sleep(backoff_time)

        with lock:
            active_workers.value -= 1


class CrawlerABC(ABC):
    def __init__(self, start_urls, output_path, temp_dir, backoff_min=1, backoff_max=5, backoff_factor=2, max_retries=3, time_sleep=3, num_processes=4, checkpoint_time=100):
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
        os.makedirs(self.temp_dir, exist_ok=True)
        self.setup_logger()

    def setup_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False

    @property
    def logger(self):
        return logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def fetch_links(self, url) -> Tuple[List[str], List[str]]:
        pass

    def save_temp(self, urls):
        data = [CrawlData(u).to_dict() for u in urls]
        temp_file = str(os.path.join(self.temp_dir, TEMP_FILE(0)))
        pd.DataFrame(data).to_parquet(temp_file, index=False)

    def run(self):
        """Start parallel processing with failure tolerance"""
        self.logger.info("Starting crawl...")
        if os.path.exists(self.output_path):
            self.logger.info(f"{self.output_path} already exists!")
            self.logger.info("Crawl completed!")
            return
        manager = Manager()
        task_queue = manager.Queue()
        urls = manager.list()
        visited_urls = manager.dict()
        active_workers = manager.Value('i', 0)
        lock = manager.Lock()

        for url in self.start_urls:
            task_queue.put(url)

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

        if self.num_processes == 1:
            worker_loop(*worker_args)
            self.save_temp(urls)
            merge_temp_files(self.temp_dir, self.output_path, 'Crawler', self.logger)
            self.logger.info("Crawl completed!")
            return

        processes = []
        try:
            current = 0
            for _ in range(self.num_processes):
                p = multiprocessing.Process(target=worker_loop, args=worker_args)
                p.start()
                processes.append(p)

            while any(p.is_alive() for p in processes):
                time.sleep(1)
                with lock:
                    qsize = task_queue.qsize()
                    self.logger.info(f"Progress: {len(visited_urls)} processed, {len(urls)} fetched urls {qsize} queued, {active_workers.value} active workers")

                    if current < len(visited_urls) / self.checkpoint_time:
                        current = len(visited_urls) / self.checkpoint_time
                        self.save_temp(urls)

        except KeyboardInterrupt:
            self.logger.warning("Received interrupt, terminating workers...")
        finally:
            for p in processes:
                if p.is_alive():
                    p.terminate()
            self.save_temp(urls)
            merge_temp_files(self.temp_dir, self.output_path, 'Crawler', self.logger)
            self.logger.info("Crawl completed!")
