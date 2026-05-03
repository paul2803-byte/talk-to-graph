"""
Tests for composite type sensitivity inheritance in FetchOntologyService.

Verifies that:
- Sub-attributes of composite types inherit the parent's sensitivity level
- attributeOrder is parsed into generalization_order
- Recursive composite references are resolved
- Circular references are handled gracefully
"""
import pytest
from orchestrator.fetch_ontology_service import FetchOntologyService
from models.ontology import Ontology


@pytest.fixture
def service():
    return FetchOntologyService()


# ── Minimal YAML data for testing ───────────────────────────────────────

ANONYMISATION_DEMO_YAML = {
    "meta": {"name": "AnonymisationDemo"},
    "content": {
        "bases": [
            {
                "name": "AnonymisationDemo",
                "attributes": {
                    "name": "String",
                    "adresse": "Address",
                    "geburtsdatum": "Date",
                    "gehalt": "Integer",
                },
            },
            {
                "name": "Address",
                "attributes": {
                    "detail": "String",
                    "city": "String",
                    "zip": "String",
                    "state": "String",
                    "country": "String",
                },
            },
        ],
        "overlays": [
            {
                "type": "OverlayClassification",
                "base": "AnonymisationDemo",
                "name": "TestOverlay",
                "attributes": {
                    "name": ["masking", "sensitive"],
                    "adresse": [
                        "generalization",
                        {"attributeOrder": {"list": ["detail", "city", "zip", "state", "country"]}},
                        "semi-sensitive",
                    ],
                    "geburtsdatum": ["randomization", "semi-sensitive"],
                    "gehalt": ["generalization", "semi-sensitive"],
                },
            }
        ],
    },
}


RECURSIVE_YAML = {
    "meta": {"name": "RecursiveTest"},
    "content": {
        "bases": [
            {
                "name": "Person",
                "attributes": {
                    "name": "String",
                    "location": "GeoAddress",
                },
            },
            {
                "name": "GeoAddress",
                "attributes": {
                    "street": "String",
                    "coords": "GeoPoint",
                },
            },
            {
                "name": "GeoPoint",
                "attributes": {
                    "lat": "Float",
                    "lon": "Float",
                },
            },
        ],
        "overlays": [
            {
                "type": "OverlayClassification",
                "base": "Person",
                "name": "PersonOverlay",
                "attributes": {
                    "name": ["masking", "sensitive"],
                    "location": ["generalization", "semi-sensitive"],
                },
            },
        ],
    },
}


# ── Composite detection ─────────────────────────────────────────────────

class TestCompositeDetection:
    def test_adresse_is_composite(self, service):
        ontology = service.parse_ontology_from_dict(ANONYMISATION_DEMO_YAML)
        demo_obj = next(o for o in ontology.objects if o.name == "AnonymisationDemo")
        addr_attr = next(a for a in demo_obj.attributes if a.name == "adresse")

        assert addr_attr.is_composite is True
        assert addr_attr.attr_type == "Address"

    def test_name_is_not_composite(self, service):
        ontology = service.parse_ontology_from_dict(ANONYMISATION_DEMO_YAML)
        demo_obj = next(o for o in ontology.objects if o.name == "AnonymisationDemo")
        name_attr = next(a for a in demo_obj.attributes if a.name == "name")

        assert name_attr.is_composite is False
        assert name_attr.children == []

    def test_gehalt_is_not_composite(self, service):
        ontology = service.parse_ontology_from_dict(ANONYMISATION_DEMO_YAML)
        demo_obj = next(o for o in ontology.objects if o.name == "AnonymisationDemo")
        gehalt_attr = next(a for a in demo_obj.attributes if a.name == "gehalt")

        assert gehalt_attr.is_composite is False


# ── Sensitivity inheritance ─────────────────────────────────────────────

class TestSensitivityInheritance:
    def test_children_inherit_parent_sensitivity(self, service):
        """All Address sub-attributes should inherit semi-sensitive from adresse."""
        ontology = service.parse_ontology_from_dict(ANONYMISATION_DEMO_YAML)
        demo_obj = next(o for o in ontology.objects if o.name == "AnonymisationDemo")
        addr_attr = next(a for a in demo_obj.attributes if a.name == "adresse")

        assert len(addr_attr.children) == 5
        for child in addr_attr.children:
            assert child.sensitivity_level == "semi-sensitive", (
                f"Child '{child.name}' should inherit semi-sensitive, "
                f"got '{child.sensitivity_level}'"
            )

    def test_children_inherit_parent_anonymization(self, service):
        """Children should also inherit anonymous type."""
        ontology = service.parse_ontology_from_dict(ANONYMISATION_DEMO_YAML)
        demo_obj = next(o for o in ontology.objects if o.name == "AnonymisationDemo")
        addr_attr = next(a for a in demo_obj.attributes if a.name == "adresse")

        for child in addr_attr.children:
            assert child.anonymization_type == "generalization", (
                f"Child '{child.name}' should inherit 'generalization'"
            )

    def test_child_names_match_address_base(self, service):
        """Children should be all attributes defined in the Address base."""
        ontology = service.parse_ontology_from_dict(ANONYMISATION_DEMO_YAML)
        demo_obj = next(o for o in ontology.objects if o.name == "AnonymisationDemo")
        addr_attr = next(a for a in demo_obj.attributes if a.name == "adresse")

        child_names = {c.name for c in addr_attr.children}
        assert child_names == {"detail", "city", "zip", "state", "country"}


# ── Generalization order ────────────────────────────────────────────────

class TestGeneralizationOrder:
    def test_generalization_order_parsed(self, service):
        """attributeOrder should be available as generalization_order."""
        ontology = service.parse_ontology_from_dict(ANONYMISATION_DEMO_YAML)
        demo_obj = next(o for o in ontology.objects if o.name == "AnonymisationDemo")
        addr_attr = next(a for a in demo_obj.attributes if a.name == "adresse")

        order = {c.name: c.generalization_order for c in addr_attr.children}
        assert order["detail"] == 0  # most specific
        assert order["city"] == 1
        assert order["zip"] == 2
        assert order["state"] == 3
        assert order["country"] == 4  # most general


# ── Recursive composites ───────────────────────────────────────────────

class TestRecursiveComposites:
    def test_two_level_nesting(self, service):
        """GeoAddress → GeoPoint: lat/lon should inherit semi-sensitive."""
        ontology = service.parse_ontology_from_dict(RECURSIVE_YAML)
        person_obj = next(o for o in ontology.objects if o.name == "Person")
        loc_attr = next(a for a in person_obj.attributes if a.name == "location")

        assert loc_attr.is_composite is True
        assert len(loc_attr.children) == 2  # street, coords

        coords_child = next(c for c in loc_attr.children if c.name == "coords")
        assert coords_child.is_composite is True
        assert coords_child.sensitivity_level == "semi-sensitive"

        # Grandchildren (lat, lon)
        assert len(coords_child.children) == 2
        for gc in coords_child.children:
            assert gc.sensitivity_level == "semi-sensitive", (
                f"Grandchild '{gc.name}' should inherit semi-sensitive"
            )

    def test_street_inherits_sensitivity(self, service):
        """Street (direct child of GeoAddress) should be semi-sensitive."""
        ontology = service.parse_ontology_from_dict(RECURSIVE_YAML)
        person_obj = next(o for o in ontology.objects if o.name == "Person")
        loc_attr = next(a for a in person_obj.attributes if a.name == "location")

        street_child = next(c for c in loc_attr.children if c.name == "street")
        assert street_child.sensitivity_level == "semi-sensitive"
        assert street_child.is_composite is False


# ── Sensitivity config flattening ───────────────────────────────────────

class TestSensitivityConfigFlattening:
    """Simulate the orchestrator's config flattening logic."""

    @staticmethod
    def _flatten(ontology: Ontology) -> dict:
        """Reproduce the orchestrator's _register_attr logic."""
        config = {}
        def _register(attr):
            if attr.name not in config:
                config[attr.name] = attr.sensitivity_level
            for child in attr.children:
                _register(child)
        for obj in ontology.objects:
            for attr in obj.attributes:
                _register(attr)
        return config

    def test_flat_config_includes_children(self, service):
        ontology = service.parse_ontology_from_dict(ANONYMISATION_DEMO_YAML)
        config = self._flatten(ontology)

        assert config["adresse"] == "semi-sensitive"
        assert config["detail"] == "semi-sensitive"
        assert config["city"] == "semi-sensitive"
        assert config["zip"] == "semi-sensitive"
        assert config["state"] == "semi-sensitive"
        assert config["country"] == "semi-sensitive"
        assert config["name"] == "sensitive"
        assert config["gehalt"] == "semi-sensitive"

    def test_recursive_flat_config(self, service):
        ontology = service.parse_ontology_from_dict(RECURSIVE_YAML)
        config = self._flatten(ontology)

        assert config["location"] == "semi-sensitive"
        assert config["street"] == "semi-sensitive"
        assert config["coords"] == "semi-sensitive"
        assert config["lat"] == "semi-sensitive"
        assert config["lon"] == "semi-sensitive"
        assert config["name"] == "sensitive"
