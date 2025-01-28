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

            if len(json_data) <= 1:
                return None

            results = []
            for i in json_data['data']:
                url = metadata[URL]
                title = i.get("title", None)

                # Process the body content
                fulltext = i.get("text", "")
                fulltext.replace('\n', '')
                fulltext.replace('\r', '')
                text = html2markdown(fulltext)

                try:
                    if i.get("publish_date"):
                        date_object = datetime.strptime(i.get("publish_date"), "%Y-%m-%d %H:%M:%S")
                    else:
                        date_object = None
                except:
                    date_object = None

                # Return the parsed data as a dictionary
                item = ParsedData(
                    URL=url,
                    raw=metadata[CONTENT],
                    format="json",
                    header=title,
                    text=text,
                    time=date_object,
                    category=None
                ).to_dict()
                results.append(item)
            return results
        except Exception as e:
            self.logger.error(f"Error parsing url {metadata[URL]}: {e}")
            return None
