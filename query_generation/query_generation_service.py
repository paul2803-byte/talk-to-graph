"""
SPARQL Query Generator

This module provides the main functionality to generate SPARQL queries from
natural language questions using an LLM agent.
"""

import os
from typing import Optional

from .sparql_agent import get_sparql_agent_prompt, format_user_message
from models.ontology import Ontology


class QueryGeneratorError(Exception):
    """Custom exception for query generation errors."""
    pass


class QueryGenerator:
    """
    A class to generate SPARQL queries using an LLM.
    
    The generator uses environment variables for configuration:
    - LLM_API_KEY: API key for the LLM provider
    - LLM_PROVIDER: Provider name (openai, anthropic, azure, google)
    - LLM_MODEL: Model to use (e.g., gpt-4o, claude-3-opus, gemini-1.5-flash)
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None
    ):
        """
        Initialize the query generator.
        
        Args:
            api_key: Optional API key (defaults to LLM_API_KEY env var)
            provider: Optional provider name (defaults to LLM_PROVIDER env var)
            model: Optional model name (defaults to LLM_MODEL env var)
        """
        self.api_key = api_key or os.getenv("LLM_API_KEY")
        self.provider = (provider or os.getenv("LLM_PROVIDER", "openai")).lower()
        self.model = model or os.getenv("LLM_MODEL", "gpt-4o")
        
        if not self.api_key:
            raise QueryGeneratorError(
                "API key not provided. Set LLM_API_KEY environment variable or pass api_key parameter."
            )
        
        self._client = None
    
    def _get_client(self):
        """
        Get or create the LLM client based on the provider.
        
        Returns:
            The initialized LLM client.
        """
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
                api_version=api_version
            )
        elif self.provider == "google":
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)
            self._client = genai
        else:
            raise QueryGeneratorError(f"Unsupported provider: {self.provider}")
        
        return self._client
    
    def generate(self, ontology: Ontology, question: str) -> str:
        """
        Generate a SPARQL query from a natural language question.
        
        Args:
            ontology: The structured Ontology object.
            question: The user's natural language question.
        
        Returns:
            str: The generated SPARQL query.
        
        Raises:
            QueryGeneratorError: If query generation fails.
        """
        client = self._get_client()
        system_prompt = get_sparql_agent_prompt()
        user_message = format_user_message(ontology, question)
        
        try:
            if self.provider == "anthropic":
                response = client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=system_prompt,
                    messages=[
                        {"role": "user", "content": user_message}
                    ]
                )
                result = response.content[0].text
            elif self.provider == "google":
                model = client.GenerativeModel(
                    model_name=self.model,
                    system_instruction=system_prompt
                )
                response = model.generate_content(
                    user_message,
                    generation_config={"temperature": 0.1}
                )
                result = response.text
            else:
                # OpenAI and Azure OpenAI use the same API
                response = client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message}
                    ],
                    temperature=0.1  # Low temperature for more deterministic output
                )
                result = response.choices[0].message.content
            
            # Post-processing to remove markdown code blocks and extra whitespace
            if result:
                result = result.strip()
                # Remove ```sparql or ``` tags
                if result.startswith("```"):
                    lines = result.splitlines()
                    if lines[0].startswith("```"):
                        lines = lines[1:]
                    if lines and lines[-1].startswith("```"):
                        lines = lines[:-1]
                    result = "\n".join(lines).strip()
            
            return result
        except Exception as e:
            raise QueryGeneratorError(f"Failed to generate query: {str(e)}") from e


# Default generator instance (lazily initialized)
_default_generator: Optional[QueryGenerator] = None


def generate_sparql_query(ontology: Ontology, question: str) -> str:
    """
    Generate a SPARQL query from a natural language question.
    
    Args:
        ontology: The structured Ontology object.
        question: The user's natural language question.
    
    Returns:
        str: The generated SPARQL query.
    """
    global _default_generator
    
    if _default_generator is None:
        _default_generator = QueryGenerator()
    
    return _default_generator.generate(ontology, question)
