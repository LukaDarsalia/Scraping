"""
Pipeline Runner for Web Scraping Framework.

This module serves as the main entry point for the web scraping pipeline,
providing functionality to:
- Load and validate configuration from YAML files
- Dynamically import and initialize pipeline components
- Execute the pipeline stages in sequence
- Handle logging and error management

The pipeline consists of three main stages:
1. Crawler: Discovers and collects URLs
2. Scraper: Downloads content from discovered URLs
3. Parser: Processes and extracts structured data from downloaded content

Each stage is configurable through the YAML configuration file and can be
customized for different websites by implementing the appropriate abstract
base classes.
"""

import importlib
import logging
from typing import Any, Dict, Type, Optional

import yaml


class PipelineRunner:
    """
    Main class for executing the web scraping pipeline.

    This class handles:
    - Configuration loading and validation
    - Dynamic component loading
    - Pipeline execution orchestration
    - Error handling and logging

    Attributes:
        config_path (str): Path to the YAML configuration file
        logger (logging.Logger): Logger instance for this runner
    """

    def __init__(self, config_path: str) -> None:
        """
        Initialize the pipeline runner.

        Args:
            config_path: Path to the YAML configuration file that defines
                        the pipeline stages and their parameters
        """
        self.config_path = config_path
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup_logger()

    def setup_logger(self) -> None:
        """Configure logging for the pipeline runner."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    def load_config(self) -> Dict[str, Any]:
        """
        Load and validate the pipeline configuration from YAML file.

        Returns:
            Dict containing the pipeline configuration

        Raises:
            Exception: If the configuration file cannot be loaded or is invalid
        """
        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)
                self.logger.info(f"Loaded configuration from {self.config_path}")
                return config
        except Exception as e:
            self.logger.error(f"Error loading configuration file: {e}")
            raise

    def dynamic_import(self, module_name: str, class_name: str) -> Type[Any]:
        """
        Dynamically import a class from a specified module.

        Args:
            module_name: Name of the module to import from
            class_name: Name of the class to import

        Returns:
            The imported class

        Raises:
            Exception: If the module or class cannot be imported
        """
        try:
            module = importlib.import_module(module_name)
            return getattr(module, class_name)
        except Exception as e:
            self.logger.error(f"Error importing {class_name} from {module_name}: {e}")
            raise

    def run(self) -> None:
        """
        Execute the pipeline according to the configuration.

        This method:
        1. Loads the configuration
        2. Identifies the target website
        3. Executes each pipeline stage in sequence
        4. Handles errors and logging for each stage

        The pipeline stages are executed in order: Crawler -> Scraper -> Parser
        Each stage's output serves as input for the next stage.
        """
        config = self.load_config()
        website = config.get("pipeline", {}).get("website")
        steps = config.get("pipeline", {}).get("steps", [])

        for step in steps:
            step_name = step["name"]
            self.logger.info(f"Starting step: {step_name}")

            input_path = step.get("input")
            output_path = step.get("output")
            step_config = step.get("config", {})

            try:
                if step_name == "Crawler":
                    self.run_crawler(website, output_path, step_config)
                elif step_name == "Scraper":
                    self.run_scraper(website, input_path, output_path, step_config)
                elif step_name == "Parser":
                    self.run_parser(website, input_path, output_path, step_config)
                else:
                    self.logger.error(f"Unknown pipeline step: {step_name}")
            except Exception as e:
                self.logger.error(f"Error in {step_name} step: {e}")
                raise

    def run_crawler(self, website: str, output_path: str, config: Dict[str, Any]) -> None:
        """
        Execute the crawler stage of the pipeline.

        Args:
            website: Name of the website module to use
            output_path: Path where crawler results will be saved
            config: Configuration parameters for the crawler

        This stage discovers and collects URLs to be processed by later stages.
        """
        crawler_class = self.dynamic_import(f"crawler.{website}", "CustomCrawler")
        crawler = crawler_class(
            start_urls=config["start_urls"],
            output_path=output_path,
            temp_dir=config["temp_dir"],
            backoff_min=config.get("backoff_min", 1),
            backoff_max=config.get("backoff_max", 5),
            backoff_factor=config.get("backoff_factor", 2),
            max_retries=config["max_retries"],
            num_processes=config["num_processes"],
            checkpoint_time=config.get("checkpoint_time", 100)
        )
        crawler.run()

    def run_scraper(self,
                    website: str,
                    input_path: str,
                    output_path: str,
                    config: Dict[str, Any]) -> None:
        """
        Execute the scraper stage of the pipeline.

        Args:
            website: Name of the website module to use
            input_path: Path to crawler output containing URLs to scrape
            output_path: Path where scraper results will be saved
            config: Configuration parameters for the scraper

        This stage downloads content from the URLs discovered by the crawler.
        """
        scraper_class = self.dynamic_import(f"scraper.{website}", "CustomScraper")
        scraper = scraper_class(
            input_path=input_path,
            output_path=output_path,
            temp_dir=config["temp_dir"],
            max_retries=config.get("max_retries", 3),
            backoff_min=config.get("backoff_min", 1),
            backoff_max=config.get("backoff_max", 5),
            backoff_factor=config.get("backoff_factor", 2),
            num_processes=config.get("num_processes", 4),
            checkpoint_time=config.get("checkpoint_time", 100)
        )
        scraper.run()

    def run_parser(self,
                   website: str,
                   input_path: str,
                   output_path: str,
                   config: Dict[str, Any]) -> None:
        """
        Execute the parser stage of the pipeline.

        Args:
            website: Name of the website module to use
            input_path: Path to scraper output containing downloaded content
            output_path: Path where parser results will be saved
            config: Configuration parameters for the parser

        This stage processes downloaded content and extracts structured data.
        """
        parser_class = self.dynamic_import(f"parser.{website}", "CustomParser")
        parser = parser_class(
            input_path=input_path,
            output_path=output_path,
            raw_data_dir=config["raw_data_dir"],
            temp_dir=config["temp_dir"],
            num_processes=config["num_processes"],
            checkpoint_time=config.get("checkpoint_time", 100)
        )
        parser.run()


if __name__ == '__main__':
    import argparse

    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(
        description="Run the web scraping pipeline with specified configuration."
    )
    parser.add_argument(
        '--config',
        required=True,
        help="Path to the YAML configuration file."
    )
    args = parser.parse_args()

    # Initialize and run the pipeline
    try:
        runner = PipelineRunner(config_path=args.config)
        runner.run()
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        raise