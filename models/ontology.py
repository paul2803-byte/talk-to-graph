from typing import List, Optional
from dataclasses import dataclass, field

@dataclass
class Attribute:
    """Represents an attribute/property of an ontology object.

    Attributes:
        name: The attribute name as defined in the ontology base.
        anonymization_type: The anonymization strategy (e.g. "generalization",
            "masking", "randomization").
        sensitivity_level: One of "sensitive", "semi-sensitive", "not-sensitive".
        attr_type: The raw type from the YAML definition (e.g. "String",
            "Address", "Integer", "Date").
        is_composite: True when ``attr_type`` references another ontology base,
            meaning this attribute has sub-attributes.
        children: Sub-attributes resolved from the referenced base.  Each child
            inherits the parent's ``sensitivity_level`` unless explicitly
            overridden by its own overlay.
        generalization_order: Position in the ``attributeOrder`` list defined
            in the overlay (0 = most specific).  ``None`` when no order is
            defined.
        min_value: Lower bound for numeric attributes (used for DP noise).
        max_value: Upper bound for numeric attributes (used for DP noise).
        number_buckets: Number of buckets for numeric grouping.
        date_granularity: Granularity for date grouping (e.g., 'YEAR', 'DECADE').
    """
    name: str
    anonymization_type: str
    sensitivity_level: str
    attr_type: str = "String"
    is_composite: bool = False
    children: List['Attribute'] = field(default_factory=list)
    generalization_order: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    number_buckets: Optional[int] = None
    date_granularity: Optional[str] = None

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
