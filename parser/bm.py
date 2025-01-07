from bs4 import BeautifulSoup
from datetime import datetime
import html
import json

from parser.parser_abc import ParserABC, Metadata, ParsedData


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

            if len(json_data) <= 1:
                return None
            # Extract relevant fields
            url = "https://bm.ge/api/news/*/" + file_path.split('/')[-1].split('.')[0]
            title = json_data.get("title", None)

            # Process the body content
            fulltext = json_data.get("text", "")
            soup = BeautifulSoup(fulltext, "html.parser")
            paragraphs = soup.get_text(separator="\n").split("\n")

            # Decode HTML entities and clean up empty lines
            decoded_paragraphs = [html.unescape(paragraph.strip()) for paragraph in paragraphs if paragraph.strip()]

            body = "\n".join(decoded_paragraphs)

            # Extract categories
            categories = [category.get("name") for category in json_data.get("tags", [])]

            # Add scraped_at timestamp
            scraped_at = datetime.now().isoformat()

            author = json_data['user']

            if author:
                author = author['name']

            # Assemble the metadata
            parsed_metadata = Metadata(
                author=author,
                category=categories,
                scraped_at=scraped_at,
                additional_info={
                    "publish_date": json_data.get("publish_date"),
                }
            )

            # Return the parsed data as a dictionary
            return ParsedData(
                url=url,
                title=title,
                body=body,
                metadata=parsed_metadata
            ).to_dict()

        except Exception as e:
            self.logger.error(f"Error parsing file {file_path}: {e}")
            return None