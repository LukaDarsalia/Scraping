import time

import requests
from scraper.scraper_abc import ScraperABC
from fake_useragent import UserAgent
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
        time.sleep(3)
        ua = UserAgent()
        try:
            response = rt.get(url, timeout=10, headers={
                "User-Agent": ua.random
            })
            response.raise_for_status()

            return 'json', response.content
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            raise