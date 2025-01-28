import json
from datetime import datetime

from core.utils import html2markdown, CONTENT, URL
from parser.parser_abc import ParserABC
from core.utils import ParsedData


class CustomParser(ParserABC):
    def parse_file(self, metadata):
        """
        Parse the JSON file and extract content, title, metadata, and other relevant fields.
        :param metadata: Metadata associated with the file (e.g., URL, file_name).
        :return: Parsed data as a dictionary.
        """
        try:
            # Read and parse the JSON file
            json_data = json.loads(metadata[CONTENT])

            if len(json_data) == 0:
                return None
            # Extract relevant fields
            url = metadata[URL]
            title = json_data.get("title", None)

            # Process the body content
            fulltext = json_data.get("fulltext", "")
            fulltext.replace('\n', '')
            fulltext.replace('\r', '')

            text = html2markdown(fulltext)
            # Extract categories
            categories = [category.get("title") for category in json_data.get("categories", [])]
            try:
                date_object = datetime.strptime(json_data.get("pub_dt"), "%Y-%m-%dT%H:%M")
            except:
                date_object = None


            # Return the parsed data as a dictionary
            return ParsedData(
                URL=url,
                raw=metadata[CONTENT],
                format="json",
                header=title,
                text=text,
                time=date_object,
                category=categories
            ).to_dict()

        except Exception as e:
            self.logger.error(f"Error parsing url {metadata[URL]}: {e}")
            return None