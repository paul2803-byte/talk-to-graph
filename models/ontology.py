from typing import List, Optional
from dataclasses import dataclass, field

@dataclass
class Attribute:
    """Represents an attribute/property of an ontology object."""
    name: str
    anonymization_type: str
    sensitivity_level: str
    min_value: Optional[float] = None
    max_value: Optional[float] = None

@dataclass
class OntologyObject:
    """Represents a class/base in the ontology."""
    name: str
    attributes: List[Attribute] = field(default_factory=list)

@dataclass
class Ontology:
    """Main container for the structured ontology representation."""
    prefix: str
    base_uri: str
    objects: List[OntologyObject] = field(default_factory=list)
