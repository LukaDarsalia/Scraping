"""
Tests for parser consistency and parallel processing behavior.
"""

import os
import random
import tempfile
import time
import logging
import json

import pandas as pd
import pytest

from parser.parser_abc import ParserABC


@pytest.mark.skip(reason="Mock class for testing, not a test class")
class MockParser(ParserABC):
    """Mock parser implementation for testing"""
    def __init__(self, *args, add_delays=False, error_rate=0.0, **kwargs):
        self.add_delays = add_delays
        self.error_rate = error_rate
        self.parse_count = 0
        super().__init__(*args, **kwargs)

    def parse_file(self, data):
        """Mock file parsing with configurable behavior"""
        self.parse_count += 1

        # Simulate random errors
        if self.error_rate > 0 and random.random() < self.error_rate:
            raise Exception(f"Simulated error parsing {data['URL']}")

        # Simulate processing delay
        if self.add_delays:
            time.sleep(random.uniform(0.01, 0.1))

        # Generate deterministic parsed content
        return {
            'URL': data['URL'],
            'title': f"Title for {data['URL']}",
            'content': f"Parsed content for {data['URL']}",
            'timestamp': time.time(),
            'error': None
        }


def setup_directories(config):
    """Create necessary directories"""
    os.makedirs(config['raw_data_dir'], exist_ok=True)
    os.makedirs(config['temp_dir'], exist_ok=True)
    os.makedirs(os.path.dirname(config['output_path']), exist_ok=True)


@pytest.fixture
def temp_dir():
    """Provides temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


@pytest.fixture
def base_config(temp_dir):
    """Provides base configuration for parser tests"""
    config = {
        'input_path': os.path.join(temp_dir, 'input.parquet'),
        'output_path': os.path.join(temp_dir, 'output.parquet'),
        'raw_data_dir': os.path.join(temp_dir, 'raw'),
        'temp_dir': os.path.join(temp_dir, 'temp')
    }
    setup_directories(config)
    return config


@pytest.fixture
def input_data(base_config):
    """Creates input data for testing"""
    urls = [f"https://test.com/{i}" for i in range(100)]
    data = []

    for url in urls:
        # Create raw file
        content = {
            'url': url,
            'raw_content': f"Raw content for {url}",
            'metadata': {'timestamp': time.time()}
        }
        file_name = f"{abs(hash(url))}.json"
        file_path = os.path.join(base_config['raw_data_dir'], file_name)

        with open(file_path, 'w') as f:
            json.dump(content, f)

        # Add to input DataFrame
        data.append({
            'URL': url,
            'file_path': file_path,
            'content_type': 'json',
            'error': None  # Add error column
        })

    df = pd.DataFrame(data)
    df.to_parquet(base_config['input_path'])
    return urls


def create_test_data(config, count):
    """Create test data with specified count"""
    urls = [f"https://test.com/{i}" for i in range(count)]
    data = []

    for url in urls:
        content = {
            'url': url,
            'raw_content': f"Raw content for {url}",
            'metadata': {'timestamp': time.time()}
        }
        file_name = f"{abs(hash(url))}.json"
        file_path = os.path.join(config['raw_data_dir'], file_name)

        with open(file_path, 'w') as f:
            json.dump(content, f)

        data.append({
            'URL': url,
            'file_path': file_path,
            'content_type': 'json',
            'error': None
        })

    df = pd.DataFrame(data)
    df.to_parquet(config['input_path'])
    return urls


def test_parser_consistency_multiple_runs(base_config, input_data, caplog):
    """Test parser consistency across multiple runs with different worker counts"""
    caplog.set_level(logging.INFO)
    worker_counts = [1, 2, 4, 8]
    runs_per_worker = 5  # Reduced for faster testing
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
            logging.info(f"  Run {run + 1:2d}/{runs_per_worker}...")

            parser = MockParser(**config)
            parser.run()

            # Verify results exist
            assert os.path.exists(parser.output_path), f"Output file not created: {parser.output_path}"

            # Verify results
            df = pd.read_parquet(parser.output_path)
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


def test_parser_with_random_delays(base_config, input_data, caplog):
    """Test parser behavior with random processing delays"""
    caplog.set_level(logging.INFO)
    logging.info("\nTesting with random delays:")
    start_time = time.time()

    config = base_config.copy()
    config['num_processes'] = 4

    parser = MockParser(**config, add_delays=True)
    parser.run()

    # Verify results exist
    assert os.path.exists(parser.output_path), f"Output file not created: {parser.output_path}"

    df = pd.read_parquet(parser.output_path)
    assert len(df) == len(input_data), "Not all files were parsed"
    assert (df['error'].isna() | (df['error'] == '')).all(), "Unexpected errors occurred"

    duration = time.time() - start_time
    logging.info(f"Completed with random delays in {duration:.2f}s ✓")


def test_parser_performance_comparison(base_config, caplog):
    """Compare performance between single worker and multiple workers"""
    caplog.set_level(logging.INFO)
    logging.info("\nPerformance Test - 1000 files:")
    results = {}
    file_count = 1000

    # Create large input dataset
    urls = create_test_data(base_config, file_count)

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
        parser = MockParser(**config, add_delays=True)
        parser.run()

        # Verify results exist
        assert os.path.exists(parser.output_path), f"Output file not created: {parser.output_path}"

        # Verify results
        df = pd.read_parquet(parser.output_path)
        assert len(df) == file_count, "Not all files were parsed"
        assert (df['error'].isna() | (df['error'] == '')).all(), "Unexpected errors occurred"

        duration = time.time() - start_time
        results[mode] = duration
        logging.info(f"  Completed in {duration:.2f} seconds")
        logging.info(f"  Files per second: {file_count/duration:.2f}")

    # Compare results
    speedup = results['single'] / results['multi']
    logging.info(f"\nPerformance Summary:")
    logging.info(f"  Single Worker: {results['single']:.2f} seconds")
    logging.info(f"  {os.cpu_count()} Workers: {results['multi']:.2f} seconds")
    logging.info(f"  Speedup: {speedup:.2f}x")

    assert speedup > 1, "Multiple workers should be faster than single worker"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])