"""
Tests for scraper consistency and parallel processing behavior.
"""

import os
import random
import tempfile
import time
import logging

import pandas as pd
import pytest

from scraper.scraper_abc import ScraperABC


@pytest.mark.skip(reason="Mock class for testing, not a test class")
class MockScraper(ScraperABC):
    """Mock scraper implementation for testing"""

    def __init__(self, *args, add_delays=False, error_rate=0.0, content_size=1000, **kwargs):
        self.add_delays = add_delays
        self.error_rate = error_rate
        self.content_size = content_size
        self.scrape_count = 0

        super().__init__(*args, **kwargs)

    def scrape_url(self, url):
        """Mock URL scraping with configurable behavior"""
        self.scrape_count += 1

        # Simulate random errors
        if self.error_rate > 0 and random.random() < self.error_rate:
            raise Exception(f"Simulated error scraping {url}")

        # Simulate network delay
        if self.add_delays:
            time.sleep(random.uniform(0.05, 0.2))

        # Generate mock content
        content = f"Content for {url}" * self.content_size
        return "html", content.encode('utf-8')


@pytest.fixture
def temp_dir():
    """Provides temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


@pytest.fixture
def base_config(temp_dir):
    """Provides base configuration for scraper tests"""
    return {
        'input_path': os.path.join(temp_dir, 'input.parquet'),
        'output_path': os.path.join(temp_dir, 'output.parquet'),
        'temp_dir': os.path.join(temp_dir, 'temp'),
        'max_retries': 10,
        'backoff_min': 0.1,
        'backoff_max': 0.1,
        'backoff_factor': 1,
    }


@pytest.fixture
def input_urls(base_config):
    """Creates input URLs file for testing"""
    urls = [f"https://test.com/{i}" for i in range(100)]
    df = pd.DataFrame({'URL': urls})
    df.to_parquet(base_config['input_path'])
    return urls


def test_scraper_consistency_multiple_runs(base_config, input_urls, caplog):
    """Test scraper consistency across multiple runs with different worker counts"""
    caplog.set_level(logging.INFO)
    worker_counts = [1, 2, 4, 8]
    runs_per_worker = 10
    all_results = []
    test_start = time.time()

    logging.info("\nStarting consistency tests with multiple workers:")

    for num_workers in worker_counts:
        worker_start = time.time()
        logging.info(f"\nTesting {num_workers} worker{'s' if num_workers > 1 else ''}:")
        worker_results = []

        config = base_config.copy()
        config['num_processes'] = num_workers

        for run in range(runs_per_worker):
            run_start = time.time()
            logging.info(f"  Run {run + 1:2d}/{runs_per_worker}...", )

            scraper = MockScraper(**config)
            scraper.run()

            # Verify results
            df = pd.read_parquet(scraper.output_path)
            contents = {row['URL']: row['content'] for _, row in df.iterrows()}
            worker_results.append(contents)

            run_time = time.time() - run_start
            logging.info(f" ✓ ({run_time:.2f}s)")

        worker_time = time.time() - worker_start
        logging.info(f"  Completed {runs_per_worker} runs in {worker_time:.2f}s")
        all_results.append(worker_results)

    # Cross-verify all runs produced identical results
    first_run = all_results[0][0]
    for worker_runs in all_results[1:]:
        for run_contents in worker_runs:
            assert run_contents == first_run, "Results differ between runs"

    total_time = time.time() - test_start
    total_runs = len(worker_counts) * runs_per_worker
    logging.info(f"\nAll {total_runs} runs consistent! Total time: {total_time:.2f}s ✓")


def test_scraper_with_random_delays(base_config, input_urls, caplog):
    """Test scraper behavior with random network delays"""
    caplog.set_level(logging.INFO)
    logging.info("\nTesting with random delays:")
    start_time = time.time()

    config = base_config.copy()
    config['num_processes'] = 4

    scraper = MockScraper(**config, add_delays=True)
    scraper.run()

    df = pd.read_parquet(scraper.output_path)
    assert len(df) == len(input_urls), "Not all URLs were scraped"
    assert not df['error'].any(), "Unexpected errors occurred"

    duration = time.time() - start_time
    logging.info(f"Completed with random delays in {duration:.2f}s ✓")


def test_scraper_error_handling(base_config, input_urls, caplog):
    """Test scraper's error handling and retry mechanism"""
    caplog.set_level(logging.INFO)
    logging.info("\nTesting error handling:")
    start_time = time.time()

    config = base_config.copy()
    config['num_processes'] = 2

    # The probability of failing one or more task is 5*10^{-9} which is very low
    scraper = MockScraper(**config, error_rate=0.1)  # 10% error rate
    scraper.run()

    df = pd.read_parquet(scraper.output_path)
    assert len(df) == len(input_urls), "Not all URLs were scraped"
    assert not df['error'].any(), "Errors persisted after retries"

    duration = time.time() - start_time
    logging.info(f"Completed error handling test in {duration:.2f}s ✓")


def test_scraper_performance_comparison(base_config, caplog):
    """Compare performance between single worker and multiple workers"""
    caplog.set_level(logging.INFO)
    import os
    logging.info("\nPerformance Test - 1000 URLs:")
    results = {}
    url_count = 1000

    # Create large input file
    urls = [f"https://test.com/{i}" for i in range(url_count)]
    df = pd.DataFrame({'URL': urls})
    df.to_parquet(base_config['input_path'])

    # Test configurations
    configs = {
        "single": 1,
        "multi": os.cpu_count()
    }

    for mode, num_workers in configs.items():
        logging.info(f"\nTesting with {num_workers} worker{'s' if num_workers > 1 else ''}:")
        config = base_config.copy()
        config['num_processes'] = num_workers

        start_time = time.time()
        scraper = MockScraper(**config, add_delays=True)
        scraper.run()

        # Verify results
        df = pd.read_parquet(scraper.output_path)
        assert len(df) == url_count, "Not all URLs were scraped"
        assert not df['error'].any(), "Unexpected errors occurred"

        duration = time.time() - start_time
        results[mode] = duration
        logging.info(f"  Completed in {duration:.2f} seconds")
        logging.info(f"  URLs per second: {url_count / duration:.2f}")

    # Compare results
    speedup = results['single'] / results['multi']
    logging.info(f"\nPerformance Summary:")
    logging.info(f"  Single Worker: {results['single']:.2f} seconds")
    logging.info(f"  {os.cpu_count()} Workers: {results['multi']:.2f} seconds")
    logging.info(f"  Speedup: {speedup:.2f}x")

    assert speedup > 1, "Multiple workers should be faster than single worker"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])