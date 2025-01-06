from crawler.crawler_abc import CrawlerABC


class CustomCrawler(CrawlerABC):
    def fetch_links(self, links):
        if type(links) == str: links = [links]
        if 826942 < int(links[0].split('/')[-1])+1:
            return []
        return ['/'.join(i.split('/')[:-1])+'/'+str(int(i.split('/')[-1])+1) for i in links]