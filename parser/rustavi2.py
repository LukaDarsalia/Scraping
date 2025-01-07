from parser.parser_abc import ParserABC
from bs4 import BeautifulSoup
from datetime import datetime


class CustomParser(ParserABC):
    def parse_file(self, file_path, metadata):
        """
        Parse the HTML file and extract content, title, author, and timestamp.
        :param file_path: Path to the HTML file.
        :param metadata: Metadata associated with the file (e.g., URL, file_name).
        :return: Parsed data as a dictionary.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                html_content = f.read()

            soup = BeautifulSoup(html_content, "html.parser")

            # Extract the main text content with paragraphs separated by new lines
            content_div = soup.find("div", {"id": "nw_txt"})
            if not content_div:
                raise ValueError("Content with id 'nw_txt' not found")

            # Combine all text from <p> tags, separating paragraphs by new lines
            paragraphs = [p.get_text() for p in content_div.find_all("p")]
            text = "\n".join(paragraphs)

            # Extract the title
            title_element = soup.find("div", {"class": "title"})
            title = title_element.get_text() if title_element else "Unknown Title"

            # Extract the author
            author_block = soup.find("div", {"class": "author_bl"})
            if author_block:
                author_span = author_block.find("span", style="color:#ee5700")
                author = author_span.get_text() if author_span else None
            else:
                author = None

            # Add scraped_at timestamp
            scraped_at = datetime.now().isoformat()

            return {
                "url": metadata["url"],
                "title": title,
                "body": text,
                "author": author,
                "scraped_at": scraped_at,
            }

        except Exception as e:
            self.logger.error(f"Error parsing file {file_path}: {e}")
            return None