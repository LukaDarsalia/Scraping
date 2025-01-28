import time
from fake_useragent import UserAgent
import requests
from scraper.scraper_abc import ScraperABC


class CustomScraper(ScraperABC):
    """
    Simple scraper that downloads HTML content from URLs.
    """
    def scrape_url(self, url):
        """
        Download the HTML content of a URL.
        :param url: URL to scrape.
        :return: Tuple (file_name, file_content).
        """
        try:
            time.sleep(0.5)
            ua = UserAgent()
            response = requests.get(url, headers={"User-Agent": ua.random}, timeout=10)
            response.raise_for_status()
            return 'html', response.content
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            raise