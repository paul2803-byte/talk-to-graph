from typing import List, Optional
from dataclasses import dataclass, field

@dataclass
class Attribute:
    """Represents an attribute/property of an ontology object."""
    name: str
    type: str

@dataclass
class OntologyObject:
    """Represents a class/base in the ontology."""
    name: str
    attributes: List[Attribute] = field(default_factory=list)
    
    @property
    def url(self) -> str:
        """Helper to get the full URL of the object if we need it."""
        return f"/{self.name}"

@dataclass
class Ontology:
    """Main container for the structured ontology representation."""
    prefix: str
    base_uri: str
    objects: List[OntologyObject] = field(default_factory=list)

    def find_object(self, name: str) -> Optional[OntologyObject]:
        for obj in self.objects:
            if obj.name == name:
                return obj
        return None
