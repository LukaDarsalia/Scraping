# Scraping Pipeline

This repository provides a modular and scalable scraping pipeline designed for efficient web crawling, scraping, and parsing. The pipeline is highly configurable, allowing users to adapt it to different websites by implementing specific methods.

---

## Features

- **Modular Design**: Abstract base classes for crawlers, scrapers, and parsers ensure easy customization.
- **Configurable Pipeline**: Define your workflow in a YAML configuration file.
- **Multiprocessing**: Leverage multiprocessing for improved performance.
- **Checkpointing and Recovery**: Save progress periodically to avoid redundant processing.
- **Dynamic Imports**: Add new websites easily by implementing site-specific logic.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Repository Structure](#repository-structure)
3. [Pipeline Configuration](#pipeline-configuration)
4. [Usage](#usage)
5. [Customizing for New Websites](#customizing-for-new-websites)
6. [Logging](#logging)
7. [License](#license)

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
        sleep_time: 5
        num_processes: 11

    - name: Parser
      input: bpn_scraped_metadata.parquet
      output: bpn_parsed_data.parquet
      config:
        raw_data_dir: "bpn_raw_data/"
        temp_dir: "parser/"
        num_processes: 64
```

---

## Usage

### Running the Pipeline

To execute the pipeline:

```bash
python runner.py --config pipeline_config.yml
```

This will run all steps (Crawler, Scraper, and Parser) sequentially as defined in the configuration file.

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
```

---

## License

This project is licensed under the MIT License. See `LICENSE` for details.
