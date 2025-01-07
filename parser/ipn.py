from bs4 import BeautifulSoup
from datetime import datetime
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

            if len(json_data) == 0:
                return None
            # Extract relevant fields
            url = "https://www.interpressnews.ge" + json_data.get("url", metadata.get("url"))
            title = json_data.get("title", None)

            # Process the body content
            fulltext = json_data.get("fulltext", "")
            soup = BeautifulSoup(fulltext, "html.parser")
            paragraphs = [p.get_text() for p in soup.find_all("p")]
            body = "\n".join(paragraphs)

            # Extract categories
            categories = [category.get("title") for category in json_data.get("categories", [])]

            # Add scraped_at timestamp
            scraped_at = datetime.now().isoformat()

            # Assemble the metadata
            parsed_metadata = Metadata(
                category=categories,
                scraped_at=scraped_at,
                additional_info={
                    "pub_dt": json_data.get("pub_dt"),
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