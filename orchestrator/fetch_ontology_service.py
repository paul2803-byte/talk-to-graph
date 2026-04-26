import logging
from typing import Dict, List, Optional, Set, Tuple

import requests
import yaml
from models.ontology import Ontology, OntologyObject, Attribute

logger = logging.getLogger(__name__)

# Primitive type names that are never treated as composite references
_PRIMITIVE_TYPES: Set[str] = {
    "String", "Integer", "Float", "Double", "Boolean",
    "Date", "DateTime", "Time", "Decimal", "Long",
}


class FetchOntologyService:
    """
    Service to fetch ontology content from a URL in YAML format and return an Ontology object.

    The parser performs two passes:

    1. **Index pass** — collects all base definitions and overlay
       classifications into lookup dictionaries.
    2. **Build pass** — constructs ``OntologyObject`` / ``Attribute``
       instances.  When an attribute's type references another base
       (composite type), the referenced base's attributes are attached
       as *children*, inheriting the parent's sensitivity level.
       This resolution is recursive so that deeply-nested composites
       (e.g. Address → GeoLocation → latitude) are handled.
    """

    def fetch_ontology(self, url: str) -> Ontology:
        """
        Calls the provided URL (appending /yaml), fetches the content, 
        validates it is valid YAML, and returns an Ontology structure.

        Args:
            url (str): The URL to fetch the ontology from.

        Returns:
            Ontology: The structured ontology object.

        Raises:
            ValueError: If the content is not valid YAML or missing required keys.
            requests.exceptions.RequestException: If the URL call fails.
        """
        # Append /yaml to the URL as requested
        yaml_url = url.rstrip('/') + '/yaml'
        
        try:
            response = requests.get(yaml_url, timeout=30)
            response.raise_for_status()
            
            try:
                # Use safe_load for security
                data = yaml.safe_load(response.text)
                return self._parse_ontology_data(data)
                
            except yaml.YAMLError as e:
                raise ValueError(f"Content from URL is not valid YAML: {str(e)}")
                
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch ontology from {yaml_url}: {str(e)}")

    def parse_ontology_from_dict(self, data: dict) -> Ontology:
        """Parse an ontology from an already-loaded YAML dict.

        Useful for testing without an HTTP call.
        """
        return self._parse_ontology_data(data)

    # ── internal parsing ────────────────────────────────────────────────

    def _parse_ontology_data(self, data: dict) -> Ontology:
        """Core parsing logic, independent of how the YAML was obtained."""
        if not data or 'content' not in data or 'meta' not in data:
            raise ValueError("Content or meta section missing in ontology YAML")

        meta_name = data.get('meta', {}).get('name', 'Anonymisation')
        base_uri = f"https://soya.ownyourdata.eu/{meta_name}/"

        content = data.get('content', {})
        bases = content.get('bases', [])
        overlays = content.get('overlays', [])

        # ── Pass 1: Index all bases and overlays ──────────────────────
        # base_defs:    {base_name: {attr_name: attr_type_string}}
        # overlay_defs: {base_name: {attr_name: overlay_values_list}}
        base_defs: Dict[str, Dict[str, str]] = {}
        overlay_defs: Dict[str, Dict[str, list]] = {}
        base_names: Set[str] = set()

        for base in bases:
            name = base.get('name')
            if not name:
                continue
            base_names.add(name)
            base_defs[name] = base.get('attributes', {})

        for overlay in overlays:
            if overlay.get('type') == 'OverlayClassification':
                base_name = overlay.get('base')
                if base_name:
                    overlay_defs[base_name] = overlay.get('attributes', {})

        # ── Pass 2: Build OntologyObject instances ────────────────────
        ontology = Ontology(prefix="oyd", base_uri=base_uri, objects=[])

        for base_name in base_defs:
            obj = OntologyObject(name=base_name, attributes=[])
            overlay_attrs = overlay_defs.get(base_name, {})
            attrs_def = base_defs[base_name]

            for attr_name, attr_type in attrs_def.items():
                overlay_values = overlay_attrs.get(attr_name, [])
                anonymization_type, sensitivity_level = self._parse_overlay_values(
                    overlay_values
                )
                min_value, max_value = self._parse_bounds(overlay_values)

                is_composite = self._is_composite_type(attr_type, base_names)

                children: List[Attribute] = []
                if is_composite:
                    children = self._resolve_children(
                        attr_type,
                        parent_sensitivity=sensitivity_level,
                        parent_anonymization=anonymization_type,
                        overlay_values=overlay_values,
                        base_defs=base_defs,
                        base_names=base_names,
                        overlay_defs=overlay_defs,
                        visited=set(),
                    )

                obj.attributes.append(Attribute(
                    name=attr_name,
                    anonymization_type=anonymization_type,
                    sensitivity_level=sensitivity_level,
                    attr_type=attr_type,
                    is_composite=is_composite,
                    children=children,
                    min_value=min_value,
                    max_value=max_value,
                ))

            ontology.objects.append(obj)

        return ontology

    # ── helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _parse_overlay_values(overlay_values: list) -> Tuple[str, str]:
        """Extract (anonymization_type, sensitivity_level) from overlay list."""
        if isinstance(overlay_values, list) and len(overlay_values) >= 2:
            # The overlay list may contain dicts (e.g. attributeOrder) mixed
            # with plain strings.  We need the first two *string* entries.
            strings = [v for v in overlay_values if isinstance(v, str)]
            if len(strings) >= 2:
                return strings[0], strings[1]
            elif len(strings) == 1:
                return strings[0], 'sensitive'
        return '', 'sensitive'

    @staticmethod
    def _parse_bounds(overlay_values: list) -> Tuple[Optional[float], Optional[float]]:
        """Extract optional (min, max) bounds from overlay list indices 2-3."""
        if isinstance(overlay_values, list) and len(overlay_values) >= 4:
            try:
                return float(overlay_values[2]), float(overlay_values[3])
            except (ValueError, TypeError):
                pass
        return None, None

    @staticmethod
    def _is_composite_type(attr_type: str, base_names: Set[str]) -> bool:
        """Return True if attr_type references another known base."""
        if attr_type in _PRIMITIVE_TYPES:
            return False
        return attr_type in base_names

    @staticmethod
    def _extract_generalization_order(overlay_values: list) -> Dict[str, int]:
        """Extract {child_attr_name: position} from attributeOrder in overlay.

        Position 0 is the most specific, higher positions are more general.
        """
        if not isinstance(overlay_values, list):
            return {}
        for item in overlay_values:
            if isinstance(item, dict) and 'attributeOrder' in item:
                order_list = item['attributeOrder'].get('list', [])
                return {name: idx for idx, name in enumerate(order_list)}
        return {}

    def _resolve_children(
        self,
        ref_base_name: str,
        parent_sensitivity: str,
        parent_anonymization: str,
        overlay_values: list,
        base_defs: Dict[str, Dict[str, str]],
        base_names: Set[str],
        overlay_defs: Dict[str, Dict[str, list]],
        visited: Set[str],
    ) -> List[Attribute]:
        """Recursively resolve children for a composite type.

        Each child inherits the *parent attribute's* sensitivity level.
        If the child itself is composite and references yet another base,
        the resolution recurses (with cycle detection via ``visited``).
        """
        if ref_base_name in visited:
            logger.warning(
                "Circular composite reference detected: %s — skipping",
                ref_base_name,
            )
            return []

        visited = visited | {ref_base_name}

        child_attrs_def = base_defs.get(ref_base_name, {})
        if not child_attrs_def:
            return []

        gen_order = self._extract_generalization_order(overlay_values)
        # Also check if the referenced base itself has an overlay
        child_overlay = overlay_defs.get(ref_base_name, {})

        children: List[Attribute] = []
        for child_name, child_type in child_attrs_def.items():
            # Check if child has its own explicit overlay on the referenced base
            child_overlay_values = child_overlay.get(child_name, [])
            if child_overlay_values:
                child_anon, child_sens = self._parse_overlay_values(child_overlay_values)
            else:
                # Inherit from parent
                child_anon = parent_anonymization
                child_sens = parent_sensitivity

            child_min, child_max = self._parse_bounds(child_overlay_values)

            child_is_composite = self._is_composite_type(child_type, base_names)
            grandchildren: List[Attribute] = []
            if child_is_composite:
                grandchildren = self._resolve_children(
                    child_type,
                    parent_sensitivity=child_sens,
                    parent_anonymization=child_anon,
                    overlay_values=child_overlay_values,
                    base_defs=base_defs,
                    base_names=base_names,
                    overlay_defs=overlay_defs,
                    visited=visited,
                )

            children.append(Attribute(
                name=child_name,
                anonymization_type=child_anon,
                sensitivity_level=child_sens,
                attr_type=child_type,
                is_composite=child_is_composite,
                children=grandchildren,
                generalization_order=gen_order.get(child_name),
                min_value=child_min,
                max_value=child_max,
            ))

        return children
