import time

import requests
from scraper.scraper_abc import ScraperABC
from requests_tor import RequestsTor

rt = RequestsTor(autochange_id=50)

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
            time.sleep(0.5)
            response = rt.get(url, timeout=10)
            response.raise_for_status()

            # Use URL hash to generate a unique file name
            file_name = f"{url.split('/')[-1]}.json"
            return file_name, response.content
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            raise