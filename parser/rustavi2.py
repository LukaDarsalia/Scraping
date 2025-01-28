from parser.parser_abc import ParserABC
from core.utils import html2markdown, CONTENT, URL
from core.utils import ParsedData
from bs4 import BeautifulSoup
from datetime import datetime


class CustomParser(ParserABC):
    def parse_file(self, metadata):
        """
        Parse the HTML file and extract content, title, author, and timestamp.
        :param metadata: Metadata associated with the file (e.g., URL, file_name).
        :return: Parsed data as a dictionary.
        """
        try:

            soup = BeautifulSoup(metadata[CONTENT], "html.parser")

            # Extract the main text content with paragraphs separated by new lines
            content_div = soup.find("div", {"id": "nw_txt"})
            if not content_div:
                raise ValueError("Content with id 'nw_txt' not found")

            try:
                date_object = datetime.strptime(
                    soup.find("div", {"class": "l"}).find('div', {'itemprop': 'datePublished'}).text.strip(),
                    '%d-%m-%Y %H:%M')
            except:
                date_object = None

            # Combine all text from <p> tags, separating paragraphs by new lines
            content_div_html = content_div.prettify()
            text = html2markdown(content_div_html)

            # Extract the title
            title_element = soup.find("div", {"class": "title"})
            title = title_element.get_text() if title_element else "Unknown Title"


            parsed_data = ParsedData(text=text,
                                     time=date_object,
                                     URL=metadata[URL],
                                     header=title,
                                     raw=metadata[CONTENT],
                                     format="html",
                                     category=None
                                     )

            return parsed_data.to_dict()

        except Exception as e:
            self.logger.error(f"Error parsing url {metadata[URL]}: {e}")
            return None