import datetime
import os

from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional, List

import pandas as pd

from core.base_pipeline import BasePipelineComponent


class ParserABC(BasePipelineComponent):
    """
    Abstract base class for data parsers.
    Inherits common pipeline functionality from BasePipelineComponent.
    """

    def __init__(self, input_path, raw_data_dir, output_path, temp_dir, **kwargs):
        """
        Initialize the parser.

        Args:
            input_path: Path to the input parquet file (scraped metadata)
            raw_data_dir: Directory containing raw scraped data files
            output_path: Path where the final parsed parquet file will be saved
            temp_dir: Directory for storing temporary files
            **kwargs: Additional arguments passed to BasePipelineComponent
        """
        super().__init__(output_path=output_path, temp_dir=temp_dir, **kwargs)
        self.input_path = input_path
        self.raw_data_dir = raw_data_dir

    @abstractmethod
    def parse_file(self, file_path, metadata):
        """
        Abstract method to parse a single raw file.
        Must be implemented by subclasses.

        Args:
            file_path: Path to the raw data file
            metadata: Metadata associated with the file (e.g., URL, file_name)

        Returns:
            Parsed data as a dictionary or list of dictionaries
        """
        pass

    def process_chunk(self, metadata_chunk, temp_file):
        """
        Process a chunk of metadata and save parsed results.

        Args:
            metadata_chunk: Chunk of metadata rows to process
            temp_file: Path to save temporary results
        """
        parsed_data = []

        for _, row in metadata_chunk.iterrows():
            if not row['file_name']:
                self.logger.warning(
                    f"Skipping row due to missing file_name: {row}"
                )
                continue

            file_path = os.path.join(self.raw_data_dir, row['file_name'])

            try:
                parsed_result = self.operation_with_retry(
                    operation_name=f"parse_file for {file_path}",
                    operation_func=self.parse_file,
                    file_path=file_path,
                    metadata=row.to_dict()
                )

                if parsed_result:
                    if isinstance(parsed_result, list):
                        parsed_data.extend(parsed_result)
                    else:
                        parsed_data.append(parsed_result)

            except Exception as e:
                self.logger.error(f"Error parsing file {file_path}: {e}")

        # Save parsed data for this chunk
        pd.DataFrame(parsed_data).to_parquet(temp_file, index=False)
        self.logger.info(f"Saved parsed chunk to {temp_file}")

    def prepare_chunks(self):
        """
        Prepare metadata chunks for processing.

        Returns:
            List of metadata chunks
        """
        try:
            metadata_df = pd.read_parquet(self.input_path)
            chunk_size = max(1, len(metadata_df) // self.num_processes)

            return [
                metadata_df[i:i + chunk_size]
                for i in range(0, len(metadata_df), chunk_size)
            ]

        except Exception as e:
            self.logger.error(f"Error preparing metadata chunks: {e}")
            return None
