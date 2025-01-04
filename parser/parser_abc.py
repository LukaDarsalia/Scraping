import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from multiprocessing import Pool
from typing import Optional, Dict, List

import pandas as pd


@dataclass
class Metadata:
    """
    Metadata structure for parsed data.
    """
    author: Optional[str] = None
    category: Optional[List[str]] = None
    scraped_at: Optional[str] = None  # ISO format timestamp
    additional_info: Dict[str, str] = field(default_factory=dict)

    def to_dict(self):
        return {
            "author": self.author,
            "category": self.category,
            "scraped_at": self.scraped_at,
            **self.additional_info,
        }


@dataclass
class ParsedData:
    """
    Data structure for parsed content.
    """
    url: str
    title: Optional[str] = None
    body: Optional[str] = None
    metadata: Metadata = field(default_factory=Metadata)

    def to_dict(self):
        return {
            "url": self.url,
            "title": self.title,
            "body": self.body,
            "metadata": self.metadata.to_dict(),
        }


class ParserABC(ABC):
    """
    Abstract base class for a data parser.
    All custom parsers must inherit from this class and implement the required methods.
    """

    def __init__(self, input_path, raw_data_dir, output_path, temp_dir, num_processes=4):
        """
        Initialize the parser.
        :param input_path: Path to the input parquet file (scraped metadata).
        :param raw_data_dir: Directory containing raw scraped data files.
        :param output_path: Path where the final parsed parquet file will be saved.
        :param temp_dir: Directory for storing temporary files.
        :param num_processes: Number of parallel processes for parsing.
        """
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
    def parse_file(self, file_path, metadata):
        """
        Abstract method to parse a single raw file.
        Must be implemented by subclasses.
        :param file_path: Path to the raw data file.
        :param metadata: Metadata associated with the file (e.g., URL, file_name).
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

        for _, row in metadata_chunk.iterrows():
            if not row['file_name']:
                self.logger.warning(f"Skipping row due to missing file_name or url: {row}")
                continue
            file_path = os.path.join(self.raw_data_dir, row['file_name'])
            try:
                parsed_result = self.parse_file(file_path, row.to_dict())
                if parsed_result:
                    parsed_data.append(parsed_result)
            except Exception as e:
                self.logger.error(f"Error parsing file {file_path}: {e}")

        # Save parsed data for this chunk to a temporary file
        pd.DataFrame(parsed_data).to_parquet(temp_file, index=False)
        self.logger.info(f"Saved parsed chunk to {temp_file}")

    def run(self):
        """
        Core parsing logic using multiprocessing.
        """
        self.logger.info("Starting parsing...")

        try:
            # Load metadata from the input parquet file
            metadata_df = pd.read_parquet(self.input_path)

            # Split metadata into chunks
            chunk_size = max(1, len(metadata_df) // self.num_processes)
            metadata_chunks = [
                metadata_df[i:i + chunk_size] for i in range(0, len(metadata_df), chunk_size)
            ]
            temp_files = [
                os.path.join(self.temp_dir, f"temp_parsed_{i}.parquet") for i in range(len(metadata_chunks))
            ]

            # Use multiprocessing to process chunks
            with Pool(self.num_processes) as pool:
                pool.starmap(self.process_chunk, zip(metadata_chunks, temp_files))

            # Merge all temporary files into the final output
            self.merge_temp_files(temp_files)
        except Exception as e:
            self.logger.error(f"Error during parsing: {e}")

    def merge_temp_files(self, temp_files):
        """
        Merge all temporary files into the final parsed parquet file.
        :param temp_files: List of paths to temporary files.
        """
        try:
            all_parsed_data = pd.concat([pd.read_parquet(file) for file in temp_files], ignore_index=True)
            all_parsed_data.to_parquet(self.output_path, index=False)
            self.logger.info(f"Saved final parsed data to {self.output_path}")

            # Clean up temporary files
            for file in temp_files:
                os.remove(file)
            self.logger.info("Cleaned up temporary files.")
        except Exception as e:
            self.logger.error(f"Error merging temporary files: {e}")
