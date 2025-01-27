import logging
import os
import random
import time
from abc import ABC, abstractmethod
from multiprocessing import Pool
import pandas as pd


class BasePipelineComponent(ABC):
    """
    Base class for all pipeline components (crawler, scraper, parser).
    Implements common functionality like multiprocessing, retries, and file handling.
    """

    def __init__(self,
                 output_path: str,
                 temp_dir: str,
                 num_processes: int = 4,
                 max_retries: int = 3,
                 backoff_min: float = 1,
                 backoff_max: float = 5,
                 backoff_factor: float = 2,
                 checkpoint_time: int = 100):
        """
        Initialize base pipeline component.

        Args:
            output_path: Path where the output parquet file will be saved
            temp_dir: Directory for storing temporary files
            num_processes: Number of parallel processes
            max_retries: Maximum number of retry attempts
            backoff_min: Minimum initial backoff time in seconds
            backoff_max: Maximum initial backoff time in seconds
            backoff_factor: Multiplicative factor for exponential backoff
            checkpoint_time: Save checkpoint after this many operations
        """
        self.output_path = output_path
        self.temp_dir = temp_dir
        self.num_processes = num_processes
        self.max_retries = max_retries
        self.backoff_min = backoff_min
        self.backoff_max = backoff_max
        self.backoff_factor = backoff_factor
        self.checkpoint_time = checkpoint_time

        os.makedirs(self.temp_dir, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup_logger()

    def setup_logger(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    def get_backoff_time(self, attempt: int, initial_backoff: float) -> float:
        """
        Calculate backoff time with exponential increase and jitter.

        Args:
            attempt: Current attempt number (0-based)
            initial_backoff: Initial backoff time for this operation

        Returns:
            Time to wait in seconds
        """
        backoff = initial_backoff * (self.backoff_factor ** attempt)
        jitter = random.uniform(-0.1 * backoff, 0.1 * backoff)
        return backoff + jitter

    def get_initial_backoff(self) -> float:
        """Generate initial backoff time between min and max values."""
        return random.uniform(self.backoff_min, self.backoff_max)

    def operation_with_retry(self, operation_name: str, operation_func, *args, **kwargs):
        """
        Execute an operation with retry logic.

        Args:
            operation_name: Name of the operation for logging
            operation_func: Function to execute
            *args, **kwargs: Arguments to pass to operation_func

        Returns:
            Result of the operation or None if all retries fail
        """
        initial_backoff = self.get_initial_backoff()

        for attempt in range(self.max_retries):
            try:
                self.logger.info(
                    f"Executing {operation_name} "
                    f"(Attempt {attempt + 1}/{self.max_retries})"
                )
                return operation_func(*args, **kwargs)
            except Exception as e:
                self.logger.error(f"Error in {operation_name}: {e}")
                if attempt < self.max_retries - 1:
                    backoff_time = self.get_backoff_time(attempt, initial_backoff)
                    self.logger.info(
                        f"Backing off for {backoff_time:.2f} seconds before retry..."
                    )
                    time.sleep(backoff_time)
                    self.logger.info(f"Retrying {operation_name}...")

        self.logger.warning(
            f"Failed {operation_name} after {self.max_retries} attempts."
        )
        return None

    def merge_temp_files(self, temp_files: list):
        """
        Merge temporary parquet files into final output.

        Args:
            temp_files: List of temporary file paths
        """
        try:
            # Merge all temporary files
            all_data = pd.concat(
                [pd.read_parquet(file) for file in temp_files],
                ignore_index=True
            )

            # Handle existing output file if it exists
            if os.path.exists(self.output_path):
                existing_data = pd.read_parquet(self.output_path)
                all_data = pd.concat([all_data, existing_data], ignore_index=True)
                all_data = all_data.drop_duplicates()

            # Save merged data
            all_data.to_parquet(self.output_path, index=False)
            self.logger.info(f"Saved final data to {self.output_path}")

            # Cleanup
            for file in temp_files:
                os.remove(file)
            self.logger.info("Cleaned up temporary files.")

        except Exception as e:
            self.logger.error(f"Error merging temporary files: {e}")

    def process_chunk(self, chunk_data, temp_file: str):
        """
        Process a chunk of data and save results to temporary file.
        Must be implemented by subclasses.
        """
        pass

    def run(self):
        """
        Main execution method. Override in subclasses if different
        behavior is needed.
        """
        try:
            chunks = self.prepare_chunks()
            if not chunks:
                self.logger.info("No data to process. Exiting.")
                return

            temp_files = [
                os.path.join(self.temp_dir, f"temp_data_{i}.parquet")
                for i in range(len(chunks))
            ]

            if self.num_processes == 1:
                self.process_chunk(chunks[0], temp_files[0])
                self.merge_temp_files([temp_files[0]])
                return

            with Pool(self.num_processes) as pool:
                pool.starmap(self.process_chunk, zip(chunks, temp_files))

            self.merge_temp_files(temp_files)

        except Exception as e:
            self.logger.error(f"Error in pipeline execution: {e}")

    @abstractmethod
    def prepare_chunks(self):
        """
        Prepare data chunks for processing.
        Must be implemented by subclasses.

        Returns:
            List of data chunks to be processed
        """
        pass