import requests
import json
from typing import Any, Dict

class FetchOntologyService:
    """
    Service to fetch ontology content from a URL and validate it as JSON.
    """

    def fetch_ontology(self, url: str) -> Dict[str, Any]:
        """
        Calls the provided URL, fetches the content, validates it is valid JSON, and returns it.

        Args:
            url (str): The URL to fetch the ontology from.

        Returns:
            Dict[str, Any]: The parsed JSON content.

        Raises:
            ValueError: If the content is not valid JSON.
            requests.exceptions.RequestException: If the URL call fails.
        """
        try:
            response = requests.get(url, timeout=30)
            # Raise an exception for bad status codes (4xx or 5xx)
            response.raise_for_status()
            
            # Try to parse the content as JSON
            try:
                ontology_data = response.json()
                return ontology_data
            except json.JSONDecodeError as e:
                raise ValueError(f"Content from URL is not valid JSON: {str(e)}")
                
        except requests.exceptions.RequestException as e:
            # Re-raise with a bit more context if needed, or just let it bubble up
            raise Exception(f"Failed to fetch ontology from {url}: {str(e)}")

if __name__ == "__main__":
    # Quick manual test block
    service = FetchOntologyService()
    # Example URL (placeholder)
    # print(service.fetch_ontology("https://api.github.com"))
