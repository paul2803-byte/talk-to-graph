"""
SPARQL Query Generator

This module provides the main functionality to generate SPARQL queries from
natural language questions using an LLM agent.  LLM interaction is delegated
to the shared ``LLMClient``; this module only handles SPARQL-specific
post-processing (stripping markdown code fences).
"""

from typing import Optional

from .llm_client import LLMClient, LLMClientError
from .sparql_agent import get_sparql_agent_prompt, format_user_message
from models.ontology import Ontology


class QueryGeneratorError(Exception):
    """Custom exception for query generation errors."""
    pass


class QueryGenerator:
    """
    Generate SPARQL queries from natural language questions via an LLM.

    Configuration is read from the standard ``LLM_*`` environment variables
    unless explicit values are passed to the constructor.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
    ):
        self._llm_client = LLMClient(
            api_key=api_key,
            provider=provider,
            model=model,
        )

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
        system_prompt = get_sparql_agent_prompt()
        user_message = format_user_message(ontology, question)

        try:
            result = self._llm_client.call(system_prompt, user_message)
        except LLMClientError as e:
            raise QueryGeneratorError(f"Failed to generate query: {e}") from e

        # ── SPARQL-specific post-processing ─────────────────────────────
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
