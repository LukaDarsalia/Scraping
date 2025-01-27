from abc import abstractmethod

from core.base_pipeline import BasePipelineComponent
import os
import pandas as pd


class ScraperABC(BasePipelineComponent):
    """
    Abstract base class for web scrapers.
    Inherits common pipeline functionality from BasePipelineComponent.
    """

    def __init__(self, input_path, output_path, raw_data_dir, temp_dir, **kwargs):
        """
        Initialize the scraper.

        Args:
            input_path: Path to the input parquet file containing URLs
            output_path: Path where the output parquet file will be saved
            raw_data_dir: Directory where raw scraped data files will be saved
            temp_dir: Directory for storing temporary files
            **kwargs: Additional arguments passed to BasePipelineComponent
        """
        super().__init__(output_path=output_path, temp_dir=temp_dir, **kwargs)
        self.input_path = input_path
        self.raw_data_dir = raw_data_dir
        os.makedirs(self.raw_data_dir, exist_ok=True)

    @abstractmethod
    def scrape_url(self, url):
        """
        Abstract method to scrape a single URL.
        Must be implemented by subclasses.

        Args:
            url: URL to scrape

        Returns:
            Tuple of (file_name, file_content)
        """
        pass

    def save_raw_data(self, file_name, content):
        """
        Save raw scraped content to file.

        Args:
            file_name: Name of the file to save
            content: Content to save

        Returns:
            Path to the saved file or None if save failed
        """
        try:
            file_path = os.path.join(self.raw_data_dir, file_name)
            with open(file_path, "wb") as f:
                f.write(content)
            return file_path
        except Exception as e:
            self.logger.error(f"Error saving raw data to {file_name}: {e}")
            return None

    def scrape_with_retry(self, url):
        """
        Scrape a URL with retry logic.

        Args:
            url: URL to scrape

        Returns:
            Dictionary containing scraping results
        """
        result = self.operation_with_retry(
            operation_name=f"scrape_url for {url}",
            operation_func=self.scrape_url,
            url=url
        )

        if result:
            file_name, file_content = result
            if file_name and file_content:
                file_path = self.save_raw_data(file_name, file_content)
                return {
                    "url": url,
                    "file_name": file_name,
                    "file_path": file_path
                }

        return {
            "url": url,
            "file_name": None,
            "file_path": None,
            "error": "Failed after retries"
        }

    def process_chunk(self, urls, temp_file):
        """
        Process a chunk of URLs and save metadata.

        Args:
            urls: List of URLs to process
            temp_file: Path to save temporary results
        """
        local_metadata = []
        counter = 0

        for url in urls:
            result = self.scrape_with_retry(url)
            local_metadata.append(result)
            counter += 1

            if counter % self.checkpoint_time == 0:
                temp_df = pd.DataFrame(local_metadata)
                temp_df.to_parquet(temp_file, index=False)
                self.logger.info(
                    f"Saved checkpoint metadata for chunk to {temp_file}"
                )

        # Save final results for this chunk
        temp_df = pd.DataFrame(local_metadata)
        temp_df.to_parquet(temp_file, index=False)
        self.logger.info(f"Saved metadata for chunk to {temp_file}")

    def prepare_chunks(self):
        """
        Prepare URL chunks for processing.
        Handles resuming from previous runs and checkpoints.

        Returns:
            List of URL chunks or None if no URLs to process
        """
        try:
            # Load URLs from input file
            urls = pd.read_parquet(self.input_path)['url'].tolist()

            # Remove already processed URLs from previous runs
            if os.path.exists(self.output_path):
                processed_df = pd.read_parquet(self.output_path)
                processed_urls = processed_df[
                    ~processed_df['file_path'].isna()
                ]['url'].tolist()
                urls = list(set(urls) - set(processed_urls))

                if not urls:
                    self.logger.info("All URLs have been processed. Exiting.")
                    return None

                self.logger.info(f"Found {len(urls)} unprocessed URLs.")

            # Handle existing temporary files (resume from checkpoint)
            checkpoint_files = [
                f for f in os.listdir(self.temp_dir)
                if f.startswith("temp_metadata_")
            ]
            if checkpoint_files:
                checkpoint_dfs = [
                    pd.read_parquet(os.path.join(self.temp_dir, f))
                    for f in checkpoint_files
                ]
                if checkpoint_dfs:
                    completed_urls = pd.concat(checkpoint_dfs)[
                        ~pd.concat(checkpoint_dfs)['file_path'].isna()
                    ]['url'].tolist()
                    urls = list(set(urls) - set(completed_urls))

                    if not urls:
                        self.logger.info(
                            "All URLs have been processed in checkpoints. Exiting."
                        )
                        return None

                    self.logger.info(
                        f"After checking checkpoints, found {len(urls)} URLs to process."
                    )

            # Split remaining URLs into chunks
            chunk_size = max(1, len(urls) // self.num_processes)
            return [
                urls[i:i + chunk_size]
                for i in range(0, len(urls), chunk_size)
            ]

        except Exception as e:
            self.logger.error(f"Error preparing URL chunks: {e}")
            return None