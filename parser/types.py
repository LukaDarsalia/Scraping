from dataclasses import dataclass
from typing import Optional, List
import datetime


@dataclass
class ParsedData:
    """
    Data structure for parsed content from web scraping.

    Attributes:
        URL: Source URL of the content
        raw: Raw content in bytes
        format: Format of the content (e.g., 'html', 'json', 'text')
        header: Content header/title if available
        text: Main text content
        category: List of content categories if available
        time: Timestamp of the content if available

    Example:
        ```python
        data = ParsedData(
            URL="https://example.com/article/1",
            raw=b"<html>...</html>",
            format="html",
            header="Article Title",
            text="Article content...",
            category=["news", "technology"],
            time=datetime.datetime.now()
        )
        ```
    """
    URL: str
    raw: bytes
    format: str
    header: Optional[str]
    text: str
    category: Optional[List[str]]
    time: Optional[datetime.datetime]

    def __post_init__(self):
        """Validate the data after initialization."""
        if not self.URL:
            raise ValueError("URL cannot be empty")
        if not self.raw:
            raise ValueError("Raw content cannot be empty")
        if not self.format:
            raise ValueError("Format must be specified")
        if not self.text:
            raise ValueError("Text content cannot be empty")

        # Ensure category is a list of strings if provided
        if self.category is not None:
            if isinstance(self.category, str):
                self.category = [self.category]
            elif not isinstance(self.category, list):
                raise ValueError("Category must be either a string, list of strings, or None")

    def to_dict(self) -> dict:
        """
        Convert the parsed data to a dictionary format.

        Returns:
            Dictionary containing all fields of the parsed data
        """
        return {
            "url": self.URL,
            "raw": self.raw,
            "format": self.format,
            "text": self.text,
            "header": self.header,
            "category": self.category,
            "time": self.time
        }
