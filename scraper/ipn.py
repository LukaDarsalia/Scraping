import time

import requests
from scraper.scraper_abc import ScraperABC


class CustomScraper(ScraperABC):
    """
    Simple scraper that downloads JSON content from URLs.
    """
    def scrape_url(self, url):
        """
        Download the JSON content of a URL.
        :param url: URL to scrape.
        :return: Tuple (file_name, file_content).
        """
        try:
            time.sleep(1)
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            return 'json', response.content
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            raise