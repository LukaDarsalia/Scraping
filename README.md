# Web Scraping Pipeline Framework

A robust, modular, and scalable web scraping pipeline designed for efficient data collection and processing. This framework provides a structured approach to crawling, scraping, and parsing web content with built-in support for parallel processing, error handling, automatic retry mechanisms, and **parallel translation corpora collection**.

## 🌟 Key Features

- **Modular Architecture**: Extensible design with abstract base classes for crawlers, scrapers, and parsers
- **Dual-Mode Processing**: Supports both monolingual data collection and parallel translation corpora
- **Translation Support**: Built-in support for English-Georgian (en-ka) and other language pairs
- **Parallel Processing**: Built-in multiprocessing support for improved performance
- **Smart Retry Logic**: Exponential backoff with jitter for graceful error handling
- **Progress Tracking**: Automatic checkpointing and progress monitoring
- **Flexible Configuration**: YAML-based configuration for easy customization
- **Quality Assurance**: Translation quality estimation and data validation
- **Multiple Formats**: Supports JSON, HTML, and text-based parallel content
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

2. Create a configuration file for **monolingual data** (e.g., `monolingual_config.yml`):
```yaml
pipeline:
  website: rustavi2
  steps:
    - name: Crawler
      output: crawled_urls.parquet
      config:
        start_urls:
          - "https://rustavi2.ge/ka/news/302888"
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
        translation_mode: false  # Monolingual mode
```

3. Create a configuration file for **translation data** (e.g., `translation_config.yml`):
```yaml
pipeline:
  website: translation_site
  steps:
    - name: Crawler
      output: translation_urls.parquet
      config:
        start_urls:
          - "https://example-translation-site.com/parallel-corpus"
        max_retries: 3
        num_processes: 4

    - name: Scraper
      input: translation_urls.parquet
      output: translation_content.parquet
      config:
        temp_dir: "scraper/"
        max_retries: 5
        num_processes: 4

    - name: Parser
      input: translation_content.parquet
      output: translation_pairs.parquet
      config:
        raw_data_dir: "raw_data/"
        temp_dir: "parser/"
        num_processes: 4
        # Translation-specific configuration
        translation_mode: true
        source_lang: "en"
        target_lang: "ka"
```

4. Run the pipeline:
```bash
# For monolingual data
python runner.py --config monolingual_config.yml

# For translation data
python runner.py --config translation_config.yml
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
- **Supports both monolingual and translation modes**
- Handles various content types and structures
- **Built-in quality estimation for translation pairs**
- Configuration parameters:
  - `raw_data_dir`: Directory for storing raw content
  - `temp_dir`: Directory for temporary files
  - `num_processes`: Parallel parsing processes
  - `checkpoint_time`: Checkpoint frequency
  - **`translation_mode`**: Enable translation dataset processing
  - **`source_lang`**: Source language code (e.g., "en")
  - **`target_lang`**: Target language code (e.g., "ka")

## 🌍 Translation Dataset Support

### Supported Data Formats

The framework can extract translation pairs from multiple formats:

#### 1. JSON Format
```json
{
  "translations": [
    {
      "en": "Hello, how are you?",
      "ka": "გამარჯობა, როგორ ხარ?",
      "quality": 0.95,
      "domain": "greeting"
    }
  ]
}
```

#### 2. HTML Tables
```html
<table class="translation-table">
  <tr><th>English</th><th>Georgian</th></tr>
  <tr><td>Thank you</td><td>გმადლობთ</td></tr>
</table>
```

#### 3. Text Markers
```text
EN: The weather is nice today
KA: დღეს ამინდი კარგია
```

### Translation Output Schema

When `translation_mode: true`, the parser generates data with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `URL` | str | Source URL of the translation pair |
| `source_text` | str | Text in source language (e.g., English) |
| `target_text` | str | Text in target language (e.g., Georgian) |
| `source_lang` | str | Source language code (`en`) |
| `target_lang` | str | Target language code (`ka`) |
| `quality_score` | float | Quality score (0.0-1.0) |
| `alignment_info` | dict | Optional alignment metadata |
| `category` | str | Optional domain/category |
| `translation_id` | str | Unique identifier for the pair |
| `raw` | bytes | Original raw content |
| `format` | str | Content format (json, html, text) |
| `error` | str | Error message if parsing failed |

### Quality Assurance Features

- **Length Ratio Validation**: Flags pairs with unusual length ratios
- **Empty Text Detection**: Filters out empty or very short segments
- **Duplicate Detection**: Identifies identical source-target pairs
- **Encoding Validation**: Ensures proper UTF-8 encoding
- **Quality Scoring**: Built-in heuristics for translation quality assessment

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

### For Monolingual Data

1. Create website-specific implementations:
   - `crawler/<website>.py`
   - `scraper/<website>.py`
   - `parser/<website>.py`

2. Implement required abstract methods:
   ```python
   # parser/<website>.py
   from parser.parser_abc import ParserABC

   class CustomParser(ParserABC):
       def parse_file(self, data):
           # Implement content parsing logic
           return parsed_data_dict
   ```

### For Translation Data

1. Create translation-specific parser:
   ```python
   # parser/<translation_website>.py
   from parser.parser_abc import ParserABC
   from core.utils import TranslationPair

   class CustomParser(ParserABC):
       def parse_translation_file(self, data):
           """Extract translation pairs from content."""
           pairs = []
           # Your extraction logic here
           pairs.append(TranslationPair(
               source_text=english_text,
               target_text=georgian_text,
               source_lang=self.source_lang,
               target_lang=self.target_lang,
               quality_score=confidence_score
           ))
           return pairs

       def parse_file(self, data):
           # Fallback for monolingual mode
           if self.translation_mode:
               pairs = self.parse_translation_file(data)
               return [pair.to_dict() for pair in pairs] if pairs else None
           else:
               # Regular monolingual parsing
               return self.parse_monolingual_content(data)
   ```

2. Update configuration:
   ```yaml
   pipeline:
     website: your_translation_website
     steps:
       - name: Parser
         config:
           translation_mode: true
           source_lang: "en"
           target_lang: "ka"
   ```

## 📈 Use Cases

### Monolingual Data Collection
- **News Articles**: Collect and parse news content
- **Educational Content**: Extract structured learning materials
- **Government Documents**: Process official publications
- **Social Media**: Gather social media posts and comments

### Translation Corpora
- **News Translation**: Parallel news articles in multiple languages
- **Legal Documents**: Legal text translations with terminology consistency
- **Educational Materials**: Textbook and course translations
- **Government Publications**: Official document translations
- **Technical Documentation**: Software and API documentation pairs

## 📝 Logging

The framework provides comprehensive logging at each stage:

```
2025-01-29 10:00:00 - CrawlerABC - INFO - Starting crawl...
2025-01-29 10:00:01 - CrawlerABC - INFO - Progress: 100 URLs discovered
2025-01-29 10:00:02 - ScraperABC - WARNING - Retry attempt 1 for https://example.com
2025-01-29 10:00:03 - ParserABC - INFO - Parser initialized in translation mode: en -> ka
2025-01-29 10:00:04 - ParserABC - INFO - Successfully parsed 50 translation pairs
```

## 🎯 Example Workflows

### Collecting Georgian News Data
```bash
# 1. Configure for Georgian news
cat > georgian_news_config.yml << EOF
pipeline:
  website: rustavi2
  steps:
    - name: Crawler
      output: georgian_urls.parquet
      config:
        start_urls: ["https://rustavi2.ge/ka/news"]
        num_processes: 4
    - name: Scraper
      input: georgian_urls.parquet
      output: georgian_content.parquet
      config:
        num_processes: 2
    - name: Parser
      input: georgian_content.parquet
      output: georgian_articles.parquet
      config:
        translation_mode: false
        num_processes: 2
EOF

# 2. Run pipeline
python runner.py --config georgian_news_config.yml

# 3. Analyze results
python -c "
import pandas as pd
df = pd.read_parquet('georgian_articles.parquet')
print(f'Collected {len(df)} articles')
print(f'Average text length: {df[\"text\"].str.len().mean():.0f} characters')
"
```

### Building English-Georgian Translation Corpus
```bash
# 1. Configure for translation pairs
cat > translation_config.yml << EOF
pipeline:
  website: translation_source
  steps:
    - name: Crawler
      output: translation_urls.parquet
      config:
        start_urls: ["https://example-translations.com/en-ka"]
        num_processes: 4
    - name: Scraper
      input: translation_urls.parquet
      output: translation_content.parquet
      config:
        num_processes: 2
    - name: Parser
      input: translation_content.parquet
      output: en_ka_corpus.parquet
      config:
        translation_mode: true
        source_lang: "en"
        target_lang: "ka"
        num_processes: 2
EOF

# 2. Run pipeline
python runner.py --config translation_config.yml

# 3. Analyze corpus quality
python -c "
import pandas as pd
df = pd.read_parquet('en_ka_corpus.parquet')
print(f'Translation pairs: {len(df)}')
print(f'Average quality: {df[\"quality_score\"].mean():.2f}')
print(f'High quality pairs (>0.8): {(df[\"quality_score\"] > 0.8).sum()}')
print(f'Languages: {df[\"source_lang\"].iloc[0]} -> {df[\"target_lang\"].iloc[0]}')
"
```

## 📊 Performance Considerations

- **Memory Usage**: Translation pairs require ~2x memory compared to monolingual data
- **Processing Speed**: Quality estimation adds ~10-15% processing overhead
- **Storage**: Parallel corpora roughly double storage requirements
- **Indexing**: Consider indexing by language pair for faster queries
- **Quality Filtering**: Pre-filter low-quality pairs to reduce storage

## 🤝 Contributing

Contributions are welcome! When contributing:

### For General Features
- Follow existing code patterns
- Add comprehensive tests
- Update documentation
- Ensure backward compatibility

### For Translation Parsers
- Implement both `parse_file()` and `parse_translation_file()` methods
- Include quality estimation
- Support multiple input formats
- Add format documentation
- Test with sample data in both modes

### Contribution Guidelines
```bash
# 1. Fork and clone
git clone https://github.com/LukaDarsalia/Scraping

# 2. Create feature branch
git checkout -b feature/new-translation-parser

# 3. Implement changes
# - Add parser in parser/new_site.py
# - Add tests in tests/test_parser/test_new_site.py
# - Update documentation

# 4. Run tests
pytest tests/ -v

# 5. Submit pull request
```

## 📄 License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## 🔗 Related Projects

- [html-to-markdown](https://github.com/JohannesKaufmann/html-to-markdown) - HTML to Markdown converter
- [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/) - HTML parsing library
- [Pandas](https://pandas.pydata.org/) - Data manipulation and analysis
- [PyArrow](https://arrow.apache.org/docs/python/) - Columnar in-memory analytics

## 📞 Support

For questions, issues, or contributions:
- 🐛 **Bug Reports**: Open an issue with detailed reproduction steps
- 💡 **Feature Requests**: Describe your use case and proposed solution
- 🤝 **Pull Requests**: Follow contribution guidelines above
- 📖 **Documentation**: Help improve examples and explanations

---

**Happy scraping and corpus building!** 🚀🌍