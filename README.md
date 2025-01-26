# Scraping Pipeline

This repository provides a modular and scalable scraping pipeline designed for efficient web crawling, scraping, and parsing. The pipeline is highly configurable, allowing users to adapt it to different websites by implementing specific methods.

---

## Features

- **Modular Design**: Abstract base classes for crawlers, scrapers, and parsers ensure easy customization.
- **Configurable Pipeline**: Define your workflow in a YAML configuration file.
- **Multiprocessing**: Leverage multiprocessing for improved performance.
- **Checkpointing and Recovery**: Save progress periodically to avoid redundant processing.
- **Dynamic Imports**: Add new websites easily by implementing site-specific logic.
- **Smart Retry Handling**: Exponential backoff with randomization for failed requests.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Repository Structure](#repository-structure)
3. [Pipeline Configuration](#pipeline-configuration)
4. [Usage](#usage)
5. [Retry and Backoff Strategy](#retry-and-backoff-strategy)
6. [Customizing for New Websites](#customizing-for-new-websites)
7. [Logging](#logging)
8. [License](#license)

---

## Getting Started

### Prerequisites

- Python 3.8+
- Install dependencies using:

```bash
pip install -r requirements.txt
```

---

## Repository Structure

```plaintext
.
├── crawler/
│   ├── crawler_abc.py       # Abstract base class for crawlers
│   ├── bpn.py               # Crawler implementation for the `bpn` website
├── parser/
│   ├── parser_abc.py        # Abstract base class for parsers
│   ├── bpn.py               # Parser implementation for the `bpn` website
├── scraper/
│   ├── scraper_abc.py       # Abstract base class for scrapers
│   ├── bpn.py               # Scraper implementation for the `bpn` website
├── runner.py                # Main pipeline runner script
├── pipeline_config.yml      # Sample pipeline configuration file
└── README.md                # Project documentation (this file)
```

---

## Pipeline Configuration

The pipeline workflow is defined in `pipeline_config.yml`. It specifies the website, steps, and their configurations.

### Example Configuration

```yaml
pipeline:
  website: bpn
  steps:
    - name: Crawler
      input: null
      output: bpn_crawled_urls.parquet
      config:
        start_urls:
          - "https://www.bpn.ge/api/article/1"
        max_retries: 3
        time_sleep: 2
        temp_dir: "crawler/"
        num_processes: 1

    - name: Scraper
      input: bpn_crawled_urls.parquet
      output: bpn_scraped_metadata.parquet
      config:
        raw_data_dir: "bpn_raw_data/"
        temp_dir: "scraper/"
        max_retries: 5
        backoff_min: 1  # Minimum initial backoff time in seconds
        backoff_max: 5  # Maximum initial backoff time in seconds
        backoff_factor: 2  # Multiplicative factor for exponential backoff
        num_processes: 11
        checkpoint_time: 100

    - name: Parser
      input: bpn_scraped_metadata.parquet
      output: bpn_parsed_data.parquet
      config:
        raw_data_dir: "bpn_raw_data/"
        temp_dir: "parser/"
        num_processes: 64
```

---

## Retry and Backoff Strategy

The scraper implements a sophisticated retry mechanism with exponential backoff to handle failures gracefully and avoid overwhelming target servers.

### Configuration Parameters

- `max_retries`: Maximum number of retry attempts for failed requests
- `backoff_min`: Minimum initial backoff time in seconds
- `backoff_max`: Maximum initial backoff time in seconds
- `backoff_factor`: Multiplicative factor for exponential backoff

### How it Works

1. **Initial Backoff**: When a request fails, the scraper generates a random initial backoff time between `backoff_min` and `backoff_max` seconds.

2. **Exponential Growth**: For each subsequent retry:
   - The wait time increases exponentially: `initial_backoff * (backoff_factor ^ attempt_number)`
   - A small amount of jitter (±10%) is added to prevent synchronized retries

3. **Example Sequence**:
   For `backoff_min=1`, `backoff_max=5`, `backoff_factor=2`:
   - Initial attempt fails → Wait random time between 1-5 seconds
   - If initial backoff was 2 seconds:
     - Retry 1: Wait ~2 seconds (± jitter)
     - Retry 2: Wait ~4 seconds (± jitter)
     - Retry 3: Wait ~8 seconds (± jitter)

This strategy provides several benefits:
- Prevents overwhelming servers during retries
- Adds randomization to avoid thundering herd problems
- Maintains consistent backoff progression per URL
- Allows fine-tuning through configuration

---

## Customizing for New Websites

### Adding a New Website

For each new website, implement the following files:

1. **Crawler**:
   - Create a new file `crawler/<website>.py`.
   - Implement the `fetch_links` method in a class inheriting from `CrawlerABC`.

   Example:
   ```python
   from crawler.crawler_abc import CrawlerABC

   class CustomCrawler(CrawlerABC):
       def fetch_links(self, links):
           if type(links) == str:
               links = [links]
           if 135031 < int(links[0].split('/')[-1]) + 1:
               return []
           return ['/'.join(i.split('/')[:-1]) + '/' + str(int(i.split('/')[-1]) + 1) for i in links]
   ```

2. **Scraper**:
   - Create a new file `scraper/<website>.py`.
   - Implement the `scrape_url` method in a class inheriting from `ScraperABC`.

   Example:
   ```python
   import requests
   from scraper.scraper_abc import ScraperABC

   class CustomScraper(ScraperABC):
       def scrape_url(self, url):
           response = requests.get(url)
           response.raise_for_status()
           file_name = f"{url.split('/')[-1]}.json"
           return file_name, response.content
   ```

3. **Parser**:
   - Create a new file `parser/<website>.py`.
   - Implement the `parse_file` method in a class inheriting from `ParserABC`.

   Example:
   ```python
   import json
   from parser.parser_abc import ParserABC

   class CustomParser(ParserABC):
       def parse_file(self, file_path, metadata):
           with open(file_path, "r") as f:
               data = json.load(f)
           return {
               "url": metadata["url"],
               "title": data["title"],
               "content": data["fulltext"]
           }
   ```

### Updating Configuration

Update `pipeline_config.yml` to use the new website by setting the `website` field and modifying the steps if necessary.

---

## Logging

Logs are generated at each step of the pipeline and include detailed information about progress and errors.

Example log format:

```plaintext
2025-01-22 12:00:00 - CrawlerABC - INFO - Starting crawl...
2025-01-22 12:01:00 - ScraperABC - ERROR - Failed to fetch URL: https://example.com
2025-01-22 12:01:00 - ScraperABC - INFO - Backing off for 2.35 seconds before retry...
```

---

## License

This project is licensed under the MIT License. See `LICENSE` for details.