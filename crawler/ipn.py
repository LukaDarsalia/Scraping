from crawler.crawler_abc import CrawlerABC


class CustomCrawler(CrawlerABC):
    def fetch_links(self, url):
        if 826942 < int(url.split('/')[-1])+1:
            return [], []
        return (['/'.join(url.split('/')[:-1])+'/'+str(int(url.split('/')[-1])+1)],
                ['/'.join(url.split('/')[:-1])+'/'+str(int(url.split('/')[-1])+1)])