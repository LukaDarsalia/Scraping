from bs4 import BeautifulSoup
from datetime import datetime
import html
import json

from parser.parser_abc import ParserABC, ParsedData, html2markdown


class CustomParser(ParserABC):
    def parse_file(self, file_path, metadata):
        """
        Parse the JSON file and extract content, title, metadata, and other relevant fields.
        :param file_path: Path to the JSON file.
        :param metadata: Metadata associated with the file (e.g., URL, file_name).
        :return: Parsed data as a dictionary.
        """
        try:
            # Read and parse the JSON file
            with open(file_path, "r", encoding="utf-8") as f:
                json_data = json.load(f)
            with open(file_path, 'rb') as file:
                file_bytes = file.read()

            if len(json_data) <= 1:
                return None
            results = []
            for i in json_data['data']:
                url = metadata['url']
                title = i.get("title", None)

                # Process the body content
                fulltext = i.get("text", "")
                fulltext.replace('\n', '')
                fulltext.replace('\r', '')
                text = html2markdown(fulltext)

                if i.get("publish_date"):
                    date_object = datetime.strptime(i.get("publish_date"), "%Y-%m-%d %H:%M:%S")
                else:
                    date_object = None


                # Return the parsed data as a dictionary
                item = ParsedData(
                    URL=url,
                    raw=file_bytes,
                    format="json",
                    header=title,
                    text=text,
                    time=date_object,
                    category=None
                ).to_dict()
                results.append(item)
            return results
        except Exception as e:
            self.logger.error(f"Error parsing file {file_path}: {e}")
            return None