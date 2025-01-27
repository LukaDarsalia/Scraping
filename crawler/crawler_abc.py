from core.base_pipeline import BasePipelineComponent
import pandas as pd
from abc import abstractmethod


class CrawlerABC(BasePipelineComponent):
    """
    Abstract base class for web crawlers.
    Inherits common pipeline functionality from BasePipelineComponent.
    """

    def __init__(self, start_urls, output_path, temp_dir, **kwargs):
        """
        Initialize the crawler.

        Args:
            start_urls: List of URLs to start crawling from
            output_path: Path where the output parquet file will be saved
            temp_dir: Directory for storing temporary files
            **kwargs: Additional arguments passed to BasePipelineComponent
        """
        super().__init__(output_path=output_path, temp_dir=temp_dir, **kwargs)
        self.start_urls = start_urls
        self.visited_urls = set()

    @abstractmethod
    def fetch_links(self, links):
        """
        Abstract method to fetch links from given URLs.
        Must be implemented by subclasses.

        Args:
            links: URL or list of URLs to crawl

        Returns:
            List of URLs found
        """
        pass

    def process_chunk(self, chunk, temp_file):
        """
        Process a chunk of URLs and save results.

        Args:
            chunk: List of URLs to process
            temp_file: Path to save temporary results
        """
        local_urls = []
        queue = list(chunk)

        while queue:
            url = queue.pop(0)
            if url in self.visited_urls:
                continue

            self.logger.info(f"Crawling: {url}")

            # Use the common retry logic from base class, but pass URL directly
            new_links = self.operation_with_retry(
                operation_name=f"fetch_links for {url}",
                operation_func=self.fetch_links,
                links=url  # Pass URL as 'links' parameter
            )

            if new_links:
                local_urls.extend(new_links)
                queue.extend(new_links)
                self.visited_urls.add(url)

        # Save results for this chunk
        pd.DataFrame({'url': list(set(local_urls))}).to_parquet(
            temp_file,
            index=False
        )
        self.logger.info(f"Saved chunk results to {temp_file}")

    def prepare_chunks(self):
        """
        Prepare URL chunks for processing.

        Returns:
            List of URL chunks
        """
        chunk_size = max(1, len(self.start_urls) // self.num_processes)
        return [
            self.start_urls[i:i + chunk_size]
            for i in range(0, len(self.start_urls), chunk_size)
        ]