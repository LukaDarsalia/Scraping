"""
Tests for crawler consistency and parallel processing behavior.
"""

import os
import random
import tempfile
import time

import pandas as pd
import pytest

from crawler.crawler_abc import CrawlerABC

@pytest.mark.skip(reason="Mock class for testing, not a test class")
class MockCrawler(CrawlerABC):
    """Mock crawler implementation for testing"""
    def __init__(self, *args, max_urls=100, add_delays=False, error_rate=0.0, **kwargs):
        self.max_urls = max_urls
        self.add_delays = add_delays
        self.error_rate = error_rate
        self.fetch_count = 0

        super().__init__(*args, **kwargs)

    def fetch_links(self, url):
        """Generate URLs in sequence up to max_urls."""
        self.fetch_count += 1

        # Simulate random errors if error_rate is set
        if self.error_rate > 0 and random.random() < self.error_rate:
            raise Exception("Simulated random error")

        current_num = int(url.split('/')[-1])

        # Simulate network delay if enabled
        if self.add_delays:
            time.sleep(random.uniform(0.01, 0.1))

        # Base case
        if current_num >= self.max_urls:
            return [], [url]

        # Generate next URL in sequence
        next_num = current_num + 1
        next_url = [f"https://test.com/{min(next_num+i, self.max_urls)}" for i in range(os.cpu_count())]

        return next_url, [url]


@pytest.fixture
def temp_dir():
    """Provides temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


@pytest.fixture
def base_config(temp_dir):
    """Provides base configuration for crawler tests."""
    return {
        'start_urls': ['https://test.com/0'],
        'output_path': os.path.join(temp_dir, 'output.parquet'),
        'temp_dir': os.path.join(temp_dir, 'temp'),
        'max_retries': 10,
        'backoff_min': 0.1,
        'backoff_max': 0.2,
        'backoff_factor': 1,
    }


def verify_urls(urls, expected_urls):
    """Helper function to verify URLs match expected set."""
    assert urls == expected_urls, (
        f"URL mismatch. Found {len(urls)} URLs, expected {len(expected_urls)}"
    )


def test_crawler_consistency_multiple_runs(base_config):
    """Test crawler consistency across multiple runs with different worker counts."""
    worker_counts = [1, 2, 4, 8]
    runs_per_worker = 100
    all_results = []
    test_start = time.time()

    print("\nStarting consistency tests with multiple workers:")

    for num_workers in worker_counts:
        worker_start = time.time()
        print(f"\nTesting {num_workers} worker{'s' if num_workers > 1 else ''}:")
        worker_results = []

        config = base_config.copy()
        config['num_processes'] = num_workers

        for run in range(runs_per_worker):
            run_start = time.time()
            print(f"  Run {run + 1:2d}/{runs_per_worker}...", end='', flush=True)

            crawler = MockCrawler(**config, max_urls=100)
            crawler.run()

            # Collect results
            df = pd.read_parquet(crawler.output_path)
            urls = set(df['URL'].tolist())
            worker_results.append(urls)

            run_time = time.time() - run_start
            print(f" ✓ ({run_time:.2f}s)")

        worker_time = time.time() - worker_start
        print(f"  Completed {runs_per_worker} runs in {worker_time:.2f}s")
        all_results.append(worker_results)

    # Verify results
    expected_urls = {f"https://test.com/{i}" for i in range(101)}

    print("\nVerifying results:")

    # Check individual runs
    for worker_idx, worker_runs in enumerate(all_results):
        worker_count = worker_counts[worker_idx]
        print(f"\n  {worker_count} worker{'s' if worker_count > 1 else ''}:")

        for run_idx, urls in enumerate(worker_runs):
            verify_urls(urls, expected_urls)
            print(f"    Run {run_idx + 1:2d}: {len(urls)} URLs ✓")

    # Cross-verify all runs
    print("\nCross-verifying all runs...")
    first_run = all_results[0][0]
    for worker_idx, worker_runs in enumerate(all_results):
        for run_idx, urls in enumerate(worker_runs):
            verify_urls(urls, first_run)

    total_time = time.time() - test_start
    total_runs = len(worker_counts) * runs_per_worker
    print(f"\nAll {total_runs} runs consistent! Total time: {total_time:.2f}s ✓")


def test_crawler_with_random_delays(base_config):
    """Test crawler behavior with random network delays."""
    print("\nTesting with random delays:")
    start_time = time.time()

    config = base_config.copy()
    config['num_processes'] = 8

    crawler = MockCrawler(**config, max_urls=100, add_delays=True)
    crawler.run()

    df = pd.read_parquet(crawler.output_path)
    urls = set(df['URL'].tolist())
    expected_urls = {f"https://test.com/{i}" for i in range(101)}
    verify_urls(urls, expected_urls)

    duration = time.time() - start_time
    print(f"Completed with random delays in {duration:.2f}s ✓")


def test_crawler_error_handling(base_config):
    """Test crawler's error handling and retry mechanism."""
    print("\nTesting error handling:")
    start_time = time.time()

    config = base_config.copy()
    config['num_processes'] = 2

    # The probability of failing one or more task is 5*10^{-9} which is very low
    crawler = MockCrawler(**config, max_urls=50, error_rate=0.1)  # 10% error rate
    crawler.run()

    df = pd.read_parquet(crawler.output_path)
    urls = set(df['URL'].tolist())
    expected_urls = {f"https://test.com/{i}" for i in range(51)}
    verify_urls(urls, expected_urls)

    duration = time.time() - start_time
    print(f"Completed error handling test in {duration:.2f}s ✓")


def test_crawler_performance_comparison(base_config):
    """
    Compare performance between single worker and multiple workers (os.cpu_count)
    for crawling 100_000 URLs.
    """
    import os
    print("\nPerformance Test - 100_000 URLs:")
    results = {}
    url_count = 100_000

    # Test configurations
    configs = {
        "single": 1,
        "multi": os.cpu_count()
    }

    for mode, num_workers in configs.items():
        print(f"\nTesting with {num_workers} worker{'s' if num_workers > 1 else ''}:")
        config = base_config.copy()
        config['num_processes'] = num_workers

        # Run crawler and measure time
        start_time = time.time()
        crawler = MockCrawler(**config, max_urls=url_count)
        crawler.run()

        # Verify results
        df = pd.read_parquet(crawler.output_path)
        urls = set(df['URL'].tolist())
        expected_urls = {f"https://test.com/{i}" for i in range(url_count + 1)}
        verify_urls(urls, expected_urls)

        end_time = time.time()
        duration = end_time - start_time
        results[mode] = duration
        print(f"  Completed in {duration:.2f} seconds")
        print(f"  URLs per second: {url_count / duration:.2f}")

    # Compare results
    speedup = results['single'] / results['multi']
    print(f"\nPerformance Summary:")
    print(f"  Single Worker: {results['single']:.2f} seconds")
    print(f"  {os.cpu_count()} Workers: {results['multi']:.2f} seconds")
    print(f"  Speedup: {speedup:.2f}x")

    # We expect some speedup with multiple workers
    assert speedup > 1, "Multiple workers should be faster than single worker"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])