import importlib
import logging

import yaml


class PipelineRunner:
    def __init__(self, config_path):
        """
        Initialize the pipeline runner with the given configuration file.
        :param config_path: Path to the YAML configuration file.
        """
        self.config_path = config_path
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup_logger()

    def setup_logger(self):
        """Set up logging for the pipeline runner."""
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    def load_config(self):
        """Load the pipeline configuration from the YAML file."""
        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)
                self.logger.info(f"Loaded configuration from {self.config_path}")
                return config
        except Exception as e:
            self.logger.error(f"Error loading configuration file: {e}")
            raise

    def dynamic_import(self, module_name, class_name):
        """
        Dynamically import a class from a module.
        :param module_name: Module to import from.
        :param class_name: Class name to import.
        :return: Imported class.
        """
        try:
            module = importlib.import_module(module_name)
            return getattr(module, class_name)
        except Exception as e:
            self.logger.error(f"Error importing {class_name} from {module_name}: {e}")
            raise

    def run(self):
        """Run the pipeline based on the loaded configuration."""
        config = self.load_config()
        website = config.get("pipeline", {}).get("website")
        steps = config.get("pipeline", {}).get("steps", [])

        for step in steps:
            step_name = step["name"]
            self.logger.info(f"Starting step: {step_name}")

            input_path = step.get("input")
            output_path = step.get("output")
            step_config = step.get("config", {})

            if step_name == "Crawler":
                self.run_crawler(website, output_path, step_config)
            elif step_name == "Scraper":
                self.run_scraper(website, input_path, output_path, step_config)
            elif step_name == "Parser":
                self.run_parser(website, input_path, output_path, step_config)
            else:
                self.logger.error(f"Unknown pipeline step: {step_name}")

    def run_crawler(self, website, output_path, config):
        """Run the crawler step."""
        crawler_class = self.dynamic_import(f"crawler.{website}", "CustomCrawler")
        crawler = crawler_class(
            start_urls=config["start_urls"],
            output_path=output_path,
            temp_dir=config["temp_dir"],
            max_retries=config["max_retries"],
            time_sleep=config["time_sleep"],
            num_processes=config["num_processes"],
        )
        crawler.run()

    def run_scraper(self, website, input_path, output_path, config):
        """Run the scraper step with exponential backoff configuration."""
        scraper_class = self.dynamic_import(f"scraper.{website}", "CustomScraper")
        scraper = scraper_class(
            input_path=input_path,
            output_path=output_path,
            raw_data_dir=config["raw_data_dir"],
            temp_dir=config["temp_dir"],
            max_retries=config.get("max_retries", 3),
            backoff_min=config.get("backoff_min", 1),
            backoff_max=config.get("backoff_max", 5),
            backoff_factor=config.get("backoff_factor", 2),
            num_processes=config.get("num_processes", 4),
            checkpoint_time=config.get("checkpoint_time", 100)
        )
        scraper.run()

    def run_parser(self, website, input_path, output_path, config):
        """Run the parser step."""
        parser_class = self.dynamic_import(f"parser.{website}", "CustomParser")
        parser = parser_class(
            input_path=input_path,
            output_path=output_path,
            raw_data_dir=config["raw_data_dir"],
            temp_dir=config["temp_dir"],
            num_processes=config["num_processes"],
        )
        parser.run()


if __name__ == '__main__':
    import argparse

    # Command-line argument for the YAML configuration file
    parser = argparse.ArgumentParser(description="Run the data processing pipeline.")
    parser.add_argument('--config', required=True, help="Path to the YAML configuration file.")
    args = parser.parse_args()

    # Initialize and run the pipeline
    runner = PipelineRunner(config_path=args.config)
    runner.run()