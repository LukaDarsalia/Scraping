import subprocess
from typing import Union


def html2markdown(html_content: Union[str, bytes]) -> str:
    """
    Convert HTML content to Markdown format using html2markdown command.

    Args:
        html_content: HTML content as string or bytes

    Returns:
        Markdown formatted string

    Raises:
        RuntimeError: If conversion fails
    """
    try:
        # Ensure content is string
        if isinstance(html_content, bytes):
            html_content = html_content.decode('utf-8')

        result = subprocess.run(
            ["html2markdown"],
            input=html_content,
            text=True,
            capture_output=True
        )

        if result.returncode == 0:
            return result.stdout.strip()
        else:
            raise RuntimeError(f"html2markdown failed: {result.stderr}")

    except Exception as e:
        raise RuntimeError(f"Error converting HTML to Markdown: {str(e)}")