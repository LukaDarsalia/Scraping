import datetime
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List
from tqdm import tqdm

import pandas as pd

from core.utils import run_processes, merge_temp_files, TEMP_FILE, ERROR, save_temp, get_backup_urls, URL


@dataclass
class ParsedData:
    """
    Data structure for parsed content.
    """
    URL: str
    raw: bytes
    format: str
    header: Optional[str]
    text: str
    category: Optional[List[str]]
    time: Optional[datetime.datetime]



    def to_dict(self):
        return {
            "url": self.URL,
            "raw": self.raw,
            "format": self.format,
            "text": self.text,
            "header": self.header,
            "category": self.category,
            "time": self.time
        }


class ParserABC(ABC):
    """
    Abstract base class for a data parser.
    All custom parsers must inherit from this class and implement the required methods.
    """

    def __init__(self, input_path, raw_data_dir, output_path, temp_dir, num_processes=4, checkpoint_time=100):
        """
        Initialize the parser.
        :param input_path: Path to the input parquet file (scraped metadata).
        :param raw_data_dir: Directory containing raw scraped data files.
        :param output_path: Path where the final parsed parquet file will be saved.
        :param temp_dir: Directory for storing temporary files.
        :param num_processes: Number of parallel processes for parsing.
        :param checkpoint_time: Saves checkpoint in that steps.
        """
        self.checkpoint_time = checkpoint_time
        self.input_path = input_path
        self.raw_data_dir = raw_data_dir
        self.output_path = output_path
        self.temp_dir = temp_dir
        self.num_processes = num_processes
        os.makedirs(self.temp_dir, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup_logger()

    def setup_logger(self):
        """Set up logging for the parser."""
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    @abstractmethod
    def parse_file(self, data):
        """
        Abstract method to parse a single raw file.
        Must be implemented by subclasses.
        :param data: data associated with the file (e.g., URL, file_name).
        :return: Parsed data as a dictionary.
        """
        pass

    def process_chunk(self, metadata_chunk, temp_file):
        """
        Process a chunk of metadata and save the parsed results to a temporary file.
        :param metadata_chunk: Chunk of metadata rows to process.
        :param temp_file: Path to the temporary file for storing parsed results.
        """
        parsed_data = []
        counter=0
        for _, row in tqdm(metadata_chunk.iterrows(), total=len(metadata_chunk)):
            if row[ERROR]:
                self.logger.warning(f"Skipping row due to missing content or url: {row}")
                continue
            try:
                parsed_result = self.parse_file(row.to_dict())
                if parsed_result:
                    if type(parsed_result) == list:
                        parsed_data.extend(parsed_result)
                    else:
                        parsed_data.append(parsed_result)
                counter += 1
                if counter % self.checkpoint_time == 0:
                    save_temp(parsed_data, temp_file)
                    parsed_data = []
                    self.logger.info(f"Saved checkpoint metadata for chunk to {temp_file}")
            except Exception as e:
                self.logger.error(f"Error parsing url {row[URL]}: {e}")

        # Save parsed data for this chunk to a temporary file
        save_temp(parsed_data, temp_file)
        self.logger.info(f"Saved parsed chunk to {temp_file}")

    def run(self):
        """
        Core parsing logic using multiprocessing.
        """
        self.logger.info("Starting parsing...")

        try:
            # Load metadata from the input parquet file
            metadata_df = pd.read_parquet(self.input_path)
            urls = metadata_df[URL].tolist()
            # Load backup urls (if exists)
            completed_urls = get_backup_urls(self.output_path, self.temp_dir)

            # Exclude already done urls
            urls = list(set(urls) - set(completed_urls))
            if not urls:
                self.logger.info("All chunks are already processed. Exiting.")
                return
            else:
                self.logger.info(f"With backup we have to parse {len(urls)} urls!")
            metadata_df = metadata_df[metadata_df[URL].isin(urls)]

            # Split metadata into chunks
            chunk_size = len(urls) // self.num_processes
            if chunk_size == 0:
                chunk_size = len(urls)
                self.num_processes = 1

            # Handle one process
            if self.num_processes == 1:
                self.process_chunk(metadata_df, os.path.join(self.temp_dir, TEMP_FILE(0)))
                merge_temp_files(self.temp_dir,
                                 self.output_path,
                                 'Scraper',
                                 self.logger)
                return

            run_processes(
                metadata_df,
                chunk_size,
                self.temp_dir,
                self.num_processes,
                self.process_chunk)

            merge_temp_files(self.temp_dir,
                             self.output_path,
                             'Scraper',
                             self.logger)
        except Exception as e:
            self.logger.error(f"Error during parsing: {e}")
