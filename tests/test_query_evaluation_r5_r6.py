"""Tests for Rules R5 and R6 in QueryEvaluationService."""
import pytest
from models.attribute_config import AttributeConfig
from query_evaluation.query_evaluation_service import QueryEvaluationService


@pytest.fixture
def service():
    return QueryEvaluationService()


ATTRIBUTE_CONFIGS = {
    "gehalt": AttributeConfig("semi-sensitive", bounds=(1000.0, 10000.0)),
    "name": AttributeConfig("sensitive"),
    "alter": AttributeConfig("semi-sensitive", bounds=(18.0, 100.0)),
    "ort": AttributeConfig("not-sensitive"),
}

ATTRIBUTE_CONFIGS_NO_BOUNDS = {
    "gehalt": AttributeConfig("semi-sensitive"),
    "name": AttributeConfig("sensitive"),
    "alter": AttributeConfig("semi-sensitive"),
    "ort": AttributeConfig("not-sensitive"),
}


# ── Rule R5: Block MIN / MAX / Sample / GroupConcat ─────────────────────

class TestRuleR5:
    def test_min_on_semi_sensitive_rejected(self, service):
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (MIN(?gehalt) AS ?minSalary)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, ATTRIBUTE_CONFIGS)
        assert not ok
        assert "Rule R5" in msg

    def test_max_on_semi_sensitive_rejected(self, service):
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (MAX(?gehalt) AS ?maxSalary)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, ATTRIBUTE_CONFIGS)
        assert not ok
        assert "Rule R5" in msg

    def test_avg_on_semi_sensitive_allowed(self, service):
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (AVG(?gehalt) AS ?avgSalary) (COUNT(?s) AS ?cnt)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, ATTRIBUTE_CONFIGS)
        assert ok


# ── Rule R6: SUM/AVG require ontology bounds ───────────────────────────

class TestRuleR6:
    def test_sum_without_bounds_rejected(self, service):
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (SUM(?gehalt) AS ?totalSalary)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, ATTRIBUTE_CONFIGS_NO_BOUNDS)  # no bounds
        assert not ok
        assert "Rule R6" in msg

    def test_avg_without_bounds_rejected(self, service):
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (AVG(?gehalt) AS ?avgSalary)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, ATTRIBUTE_CONFIGS_NO_BOUNDS)
        assert not ok
        assert "Rule R6" in msg

    def test_count_without_bounds_allowed(self, service):
        """COUNT sensitivity is always 1 — no bounds needed."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (COUNT(?gehalt) AS ?cnt)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, ATTRIBUTE_CONFIGS_NO_BOUNDS)
        assert ok

    def test_sum_with_bounds_allowed(self, service):
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (SUM(?gehalt) AS ?totalSalary) (COUNT(?s) AS ?cnt)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, ATTRIBUTE_CONFIGS)
        assert ok


# ── Aggregate metadata output ──────────────────────────────────────────

class TestAggregateMetadata:
    def test_avg_metadata_returned(self, service):
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (AVG(?gehalt) AS ?avgSalary) (COUNT(?s) AS ?cnt)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, _, agg_info = service.evaluate_query(query, ATTRIBUTE_CONFIGS)
        assert ok
        assert len(agg_info) >= 1
        avg_entry = [a for a in agg_info if a["function"] == "avg"]
        assert len(avg_entry) == 1
        assert avg_entry[0]["attribute"] == "gehalt"

    def test_count_metadata_returned(self, service):
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (COUNT(?gehalt) AS ?cnt)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, _, agg_info = service.evaluate_query(query, ATTRIBUTE_CONFIGS)
        assert ok
        count_entries = [a for a in agg_info if a["function"] == "count"]
        assert len(count_entries) == 1
