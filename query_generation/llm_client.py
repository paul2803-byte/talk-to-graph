"""
Reusable LLM Client

Shared client logic extracted from QueryGenerator so that both SPARQL
generation and natural-language response generation can use the same
provider-agnostic interface.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class LLMClientError(Exception):
    """Raised when the LLM call fails."""
    pass


class LLMClient:
    """Provider-agnostic wrapper around OpenAI / Anthropic / Azure / Google LLMs."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
    ):
        self.api_key = api_key or os.getenv("LLM_API_KEY")
        self.provider = (provider or os.getenv("LLM_PROVIDER", "openai")).lower()
        self.model = model or os.getenv("LLM_MODEL", "gpt-4o")

        if not self.api_key:
            raise LLMClientError(
                "API key not provided. Set LLM_API_KEY environment variable "
                "or pass api_key parameter."
            )

        self._client = None

    # ── internal ────────────────────────────────────────────────────────

    def _get_client(self):
        """Lazily create the underlying provider client."""
        if self._client is not None:
            return self._client

        if self.provider == "openai":
            from openai import OpenAI
            self._client = OpenAI(api_key=self.api_key)
        elif self.provider == "anthropic":
            from anthropic import Anthropic
            self._client = Anthropic(api_key=self.api_key)
        elif self.provider == "azure":
            from openai import AzureOpenAI
            azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
            api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
            self._client = AzureOpenAI(
                api_key=self.api_key,
                azure_endpoint=azure_endpoint,
                api_version=api_version,
            )
        elif self.provider == "google":
            from google import genai
            self._client = genai.Client(api_key=self.api_key)
        else:
            raise LLMClientError(f"Unsupported LLM provider: {self.provider}")

        return self._client

    # ── public API ──────────────────────────────────────────────────────

    def call(self, system_prompt: str, user_message: str) -> str:
        """Send a system + user prompt to the configured LLM and return the
        raw text response.

        Raises ``LLMClientError`` on any provider error.
        """
        client = self._get_client()

        try:
            if self.provider == "anthropic":
                response = client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_message}],
                )
                return response.content[0].text

            elif self.provider == "google":
                response = client.models.generate_content(
                    model=self.model,
                    contents=user_message,
                    config={
                        "system_instruction": system_prompt,
                        "temperature": 0.1,
                    },
                )
                return response.text

            else:
                # OpenAI and Azure OpenAI share the same API surface
                response = client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                    temperature=0.1,
                )
                return response.choices[0].message.content

        except Exception as e:
            raise LLMClientError(f"LLM call failed: {e}") from e
