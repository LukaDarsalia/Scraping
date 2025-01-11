import json
from datetime import datetime

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

            if len(json_data) == 0:
                return None
            # Extract relevant fields
            url = metadata['url']
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

            # Add scraped_at timestamp
            with open(file_path, 'rb') as file:
                file_bytes = file.read()

            # Return the parsed data as a dictionary
            return ParsedData(
                URL=url,
                raw=file_bytes,
                format="json",
                header=title,
                text=text,
                time=date_object,
                category=categories
            ).to_dict()

        except Exception as e:
            self.logger.error(f"Error parsing file {file_path}: {e}")
            return None