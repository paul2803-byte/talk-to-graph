"""
Query Generation Service

This package provides functionality to generate SPARQL queries from natural language
questions based on a provided ontology.
"""

from .generator import generate_sparql_query

__all__ = ["generate_sparql_query"]
