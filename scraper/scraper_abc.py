import time
from abc import ABC, abstractmethod
import os
import pandas as pd
import logging
from multiprocessing import Pool


class ScraperABC(ABC):
    def __init__(self, input_path, output_path, raw_data_dir, temp_dir, max_retries=3, sleep_time=2, num_processes=4,
                 checkpoint_time=100):
        """
        Initialize the scraper.
        :param temp_dir: Directory for storing temporary files.
        :param sleep_time: Time to sleep between retries.
        :param input_path: Path to the input parquet file containing URLs.
        :param output_path: Path where the output parquet file (metadata) will be saved.
        :param raw_data_dir: Directory where raw scraped data files will be saved.
        :param max_retries: Maximum number of retries for failed URLs.
        :param num_processes: Number of parallel processes for scraping.
        :param checkpoint_time: Saves checkpoint in that steps.
        """
        self.checkpoint_time = checkpoint_time
        self.sleep_time = sleep_time
        self.input_path = input_path
        self.output_path = output_path
        self.raw_data_dir = raw_data_dir
        self.temp_dir = temp_dir
        self.max_retries = max_retries
        self.num_processes = num_processes
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup_logger()

    def setup_logger(self):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    @abstractmethod
    def scrape_url(self, url):
        """
        Abstract method to scrape a single URL.
        Must be implemented by subclasses.
        """
        pass

    def save_raw_data(self, file_name, content):
        try:
            file_path = os.path.join(self.raw_data_dir, file_name)
            with open(file_path, "wb") as f:
                f.write(content)
            return file_path
        except Exception as e:
            self.logger.error(f"Error saving raw data to {file_name}: {e}")
            return None

    def scrape_with_retries(self, url):
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Scraping (Attempt {attempt + 1}/{self.max_retries}): {url}")
                file_name, file_content = self.scrape_url(url)
                if file_name and file_content:
                    file_path = self.save_raw_data(file_name, file_content)
                    return {"url": url, "file_name": file_name, "file_path": file_path}
            except Exception as e:
                self.logger.error(f"Error scraping {url}: {e}")
                time.sleep(self.sleep_time)
            self.logger.info(f"Retrying {url}...")

        self.logger.warning(f"Failed to scrape {url} after {self.max_retries} attempts.")
        return {"url": url, "file_name": None, "file_path": None, "error": "Failed after retries"}

    def process_chunk(self, urls, temp_file):
        """
        Process a chunk of URLs and save the metadata to a temporary file.
        """
        local_metadata = []
        counter=0
        for url in urls:
            result = self.scrape_with_retries(url)
            local_metadata.append(result)
            counter += 1
            if counter % self.checkpoint_time == 0:
                temp_df = pd.DataFrame(local_metadata)
                temp_df.to_parquet(temp_file, index=False)
                self.logger.info(f"Saved checkpoint metadata for chunk to {temp_file}")
        temp_df = pd.DataFrame(local_metadata)
        temp_df.to_parquet(temp_file, index=False)
        self.logger.info(f"Saved metadata for chunk to {temp_file}")

    def run(self):
        """
        Core scraping logic: read input URLs, scrape data, and save metadata using multiprocessing.
        """
        self.logger.info("Starting scraping...")
        try:
            # Load URLs from the input parquet file
            urls = pd.read_parquet(self.input_path)['url'].tolist()
            if os.path.exists(self.output_path):
                out_pd = pd.read_parquet(self.output_path)
                out_pd = out_pd[~out_pd['file_path'].isna()]['url'].tolist()

                urls = list(set(urls) - set(out_pd))

                if not urls:
                    self.logger.info("All chunks are already processed. Exiting.")
                    return
                else:
                    self.logger.info(f"With backup we have to scrape {len(urls)} urls!")
            chunk_size = len(urls) // self.num_processes
            obj_to_conc=[pd.read_parquet(f) for f in os.listdir(self.temp_dir) if f.startswith("temp_metadata_")]
            if len(obj_to_conc) > 0:
                completed_files = pd.concat(obj_to_conc)
                completed_urls = completed_files[~completed_files['file_path'].isna()]['url'].tolist()

                urls = list(set(urls) - set(completed_urls))
                if not urls:
                    self.logger.info("All chunks are already processed. Exiting.")
                    return
                else:
                    self.logger.info(f"With backup we have to scrape {len(urls)} urls!")

            if self.num_processes == 1:
                self.process_chunk(urls, os.path.join(self.temp_dir, "temp_metadata_0.parquet"))
                self.merge_temp_files([os.path.join(self.temp_dir, "temp_metadata_0.parquet")])
                return

            # Split URLs into chunks
            url_chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]

            # Generate temporary file paths for each chunk
            temp_files = [os.path.join(self.temp_dir, f"temp_metadata_{i}.parquet") for i in range(len(url_chunks))]

            # Use multiprocessing to process chunks
            with Pool(self.num_processes) as pool:
                pool.starmap(self.process_chunk, zip(url_chunks, temp_files))

            # Merge temporary files into the final output
            self.merge_temp_files(temp_files)
        except Exception as e:
            self.logger.error(f"Error running scraper: {e}")

    def merge_temp_files(self, temp_files):
        """
        Merge all temporary files into the final output parquet file.
        """
        try:
            all_metadata = pd.concat([pd.read_parquet(file) for file in temp_files], ignore_index=True)
            if os.path.exists(self.output_path):
                out_pd = pd.read_parquet(self.output_path)
                all_metadata = pd.concat([all_metadata, out_pd], ignore_index=True)
            all_metadata.to_parquet(self.output_path, index=False)
            self.logger.info(f"Saved final metadata to {self.output_path}")

            # Clean up temporary files
            for file in temp_files:
                os.remove(file)
            self.logger.info("Cleaned up temporary files.")
        except Exception as e:
            self.logger.error(f"Error merging temporary files: {e}")