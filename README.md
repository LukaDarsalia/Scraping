# Web Scraping Pipeline Framework

A robust, modular, and scalable web scraping pipeline designed for efficient data collection and processing. This framework provides a structured approach to crawling, scraping, and parsing web content with built-in support for parallel processing, error handling, and automatic retry mechanisms.

## 🌟 Key Features

- **Modular Architecture**: Extensible design with abstract base classes for crawlers, scrapers, and parsers
- **Parallel Processing**: Built-in multiprocessing support for improved performance
- **Smart Retry Logic**: Exponential backoff with jitter for graceful error handling
- **Progress Tracking**: Automatic checkpointing and progress monitoring
- **Flexible Configuration**: YAML-based configuration for easy customization
- **Robust Error Handling**: Comprehensive error handling and logging throughout the pipeline
- **Type Safety**: Full type hints support for better code reliability
- **Recovery Mechanism**: Automatic recovery from interruptions

## 📋 Prerequisites

- Python 3.8+
- Dependencies from `requirements.txt`
- [html-to-markdown](https://github.com/JohannesKaufmann/html-to-markdown) - Required for HTML content conversion:
  ```bash
  go get github.com/JohannesKaufmann/html-to-markdown
  ```
  Make sure the `html2markdown` binary is available in your system PATH.

## 🚀 Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a configuration file (e.g., `pipeline_config.yml`):
```yaml
pipeline:
  website: example_site
  steps:
    - name: Crawler
      output: crawled_urls.parquet
      config:
        start_urls:
          - "https://example.com/start"
        max_retries: 3
        num_processes: 4

    - name: Scraper
      input: crawled_urls.parquet
      output: scraped_content.parquet
      config:
        temp_dir: "scraper/"
        max_retries: 5
        num_processes: 4

    - name: Parser
      input: scraped_content.parquet
      output: parsed_data.parquet
      config:
        raw_data_dir: "raw_data/"
        temp_dir: "parser/"
        num_processes: 4
```

3. Run the pipeline:
```bash
python runner.py --config pipeline_config.yml
```

## 🏗 Project Structure

```
.
├── core/
│   ├── __init__.py
│   └── utils.py              # Core utilities and helper functions
├── crawler/
│   ├── __init__.py
│   ├── crawler_abc.py        # Abstract base class for crawlers
│   └── <website>.py         # Website-specific crawler implementations
├── scraper/
│   ├── __init__.py
│   ├── scraper_abc.py       # Abstract base class for scrapers
│   └── <website>.py         # Website-specific scraper implementations
├── parser/
│   ├── __init__.py
│   ├── parser_abc.py        # Abstract base class for parsers
│   └── <website>.py         # Website-specific parser implementations
├── runner.py                # Main pipeline execution script
├── pipeline_config.yml      # Pipeline configuration file
└── requirements.txt         # Project dependencies
```

## 📊 Pipeline Stages

### 1. Crawler
- Discovers and collects URLs following site-specific patterns
- Manages URL deduplication and crawling depth
- Supports parallel processing for faster URL discovery
- Configuration parameters:
  - `start_urls`: Initial URLs to begin crawling
  - `max_retries`: Maximum retry attempts for failed requests
  - `num_processes`: Number of parallel crawling processes
  - `time_sleep`: Delay between requests
  - `checkpoint_time`: Frequency of progress saves

### 2. Scraper
- Downloads content from discovered URLs
- Implements smart retry logic with exponential backoff
- Handles rate limiting and server load management
- Configuration parameters:
  - `backoff_min`: Minimum retry delay
  - `backoff_max`: Maximum retry delay
  - `backoff_factor`: Exponential growth factor
  - `max_retries`: Maximum retry attempts
  - `num_processes`: Parallel scraping processes

### 3. Parser
- Extracts structured data from downloaded content
- Supports multiple output formats
- Handles various content types and structures
- Configuration parameters:
  - `raw_data_dir`: Directory for storing raw content
  - `temp_dir`: Directory for temporary files
  - `num_processes`: Parallel parsing processes
  - `checkpoint_time`: Checkpoint frequency

## 🔄 Retry Strategy

The framework implements a sophisticated retry mechanism with exponential backoff and jitter:

1. Initial retry delay is randomized between `backoff_min` and `backoff_max`
2. Subsequent retries increase exponentially: `delay * (backoff_factor ^ attempt)`
3. Random jitter (±10%) prevents thundering herd problems
4. Per-URL consistent backoff progression

Example sequence for `backoff_min=1`, `backoff_max=5`, `backoff_factor=2`:
```
Initial failure → Random delay 1-5s
Retry 1 → Initial delay * 2 (± jitter)
Retry 2 → Initial delay * 4 (± jitter)
Retry 3 → Initial delay * 8 (± jitter)
```

## 🔧 Adding New Websites

1. Create website-specific implementations:
   - `crawler/<website>.py`
   - `scraper/<website>.py`
   - `parser/<website>.py`

2. Implement required abstract methods:
   ```python
   # crawler/<website>.py
   from crawler.crawler_abc import CrawlerABC

   class CustomCrawler(CrawlerABC):
       def fetch_links(self, url):
           # Implement URL discovery logic
           return new_urls_to_crawl, content_urls_found

   # scraper/<website>.py
   from scraper.scraper_abc import ScraperABC

   class CustomScraper(ScraperABC):
       def scrape_url(self, url):
           # Implement content download logic
           return content_format, content_bytes

   # parser/<website>.py
   from parser.parser_abc import ParserABC

   class CustomParser(ParserABC):
       def parse_file(self, data):
           # Implement content parsing logic
           return parsed_data_dict
   ```

3. Update configuration to use the new website:
   ```yaml
   pipeline:
     website: your_new_website
     steps:
       ...
   ```

## 📝 Logging

The framework provides comprehensive logging at each stage:

```
2025-01-29 10:00:00 - CrawlerABC - INFO - Starting crawl...
2025-01-29 10:00:01 - CrawlerABC - INFO - Progress: 100 URLs discovered
2025-01-29 10:00:02 - ScraperABC - WARNING - Retry attempt 1 for https://example.com
2025-01-29 10:00:03 - ParserABC - INFO - Successfully parsed 50 documents
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests.

## 📄 License

This project is licensed under the MIT License. See the `LICENSE` file for details.