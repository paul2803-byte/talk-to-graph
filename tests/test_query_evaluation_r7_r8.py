"""Tests for Rules R7, R2 enhancement (R2b), and R8 in QueryEvaluationService."""
import pytest
from query_evaluation.query_evaluation_service import QueryEvaluationService


@pytest.fixture
def service():
    return QueryEvaluationService()


SENSITIVITY = {
    "gehalt": "semi-sensitive",
    "name": "sensitive",
    "alter": "semi-sensitive",
    "ort": "not-sensitive",
}

BOUNDS = {
    "gehalt": (1000.0, 10000.0),
    "alter": (18.0, 100.0),
}


# ── Rule R7: Block concrete-subject access ──────────────────────────────

class TestRuleR7:
    def test_concrete_subject_semi_sensitive_rejected(self, service):
        """Direct access to a specific individual's semi-sensitive data."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT ?salary WHERE { oyd:object1-test117 oyd:gehalt ?salary . }
        """
        ok, msg, _ = service.evaluate_query(query, SENSITIVITY, BOUNDS)
        assert not ok
        assert "Rule R7" in msg

    def test_concrete_subject_with_aggregate_rejected(self, service):
        """Aggregating a single individual's data still reveals their value."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (AVG(?salary) AS ?avg) (COUNT(?salary) AS ?cnt)
            WHERE { oyd:person1 oyd:gehalt ?salary . }
        """
        ok, msg, _ = service.evaluate_query(query, SENSITIVITY, BOUNDS)
        assert not ok
        assert "Rule R7" in msg

    def test_concrete_subject_not_sensitive_allowed(self, service):
        """Accessing not-sensitive data for a specific individual is OK."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT ?city WHERE { oyd:person1 oyd:ort ?city . }
        """
        ok, msg, _ = service.evaluate_query(query, SENSITIVITY, BOUNDS)
        assert ok

    def test_variable_subject_allowed(self, service):
        """Normal aggregate over all individuals should pass."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (AVG(?gehalt) AS ?avgSalary) (COUNT(?s) AS ?cnt)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, SENSITIVITY, BOUNDS)
        assert ok

    def test_concrete_subject_sensitive_rejected(self, service):
        """Accessing sensitive data for a specific individual is also blocked."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT ?n WHERE { oyd:person1 oyd:name ?n . }
        """
        ok, msg, _ = service.evaluate_query(query, SENSITIVITY, BOUNDS)
        assert not ok
        assert "Rule R7" in msg


# ── Rule R2b: Literal constraint on semi-sensitive predicate ────────────

class TestRuleR2b:
    def test_literal_value_on_semi_sensitive_rejected(self, service):
        """A triple like ?s oyd:alter 30 constrains like FILTER."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (AVG(?gehalt) AS ?avg) (COUNT(?s) AS ?cnt)
            WHERE {
                ?s oyd:gehalt ?gehalt .
                ?s oyd:alter 30 .
            }
        """
        ok, msg, _ = service.evaluate_query(query, SENSITIVITY, BOUNDS)
        assert not ok
        assert "Rule R2" in msg

    def test_literal_value_on_not_sensitive_allowed(self, service):
        """Constraining not-sensitive data with a literal is fine."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (AVG(?gehalt) AS ?avg) (COUNT(?s) AS ?cnt)
            WHERE {
                ?s oyd:gehalt ?gehalt .
                ?s oyd:ort "Wien" .
            }
        """
        ok, msg, _ = service.evaluate_query(query, SENSITIVITY, BOUNDS)
        assert ok


# ── Rule R8: AVG/SUM on semi-sensitive requires COUNT ───────────────────

class TestRuleR8:
    def test_avg_without_count_rejected(self, service):
        """AVG on semi-sensitive without COUNT should be rejected."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (AVG(?gehalt) AS ?avgSalary)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, SENSITIVITY, BOUNDS)
        assert not ok
        assert "Rule R8" in msg

    def test_sum_without_count_rejected(self, service):
        """SUM on semi-sensitive without COUNT should be rejected."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (SUM(?gehalt) AS ?totalSalary)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, SENSITIVITY, BOUNDS)
        assert not ok
        assert "Rule R8" in msg

    def test_avg_with_count_allowed(self, service):
        """AVG on semi-sensitive with COUNT should pass."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (AVG(?gehalt) AS ?avgSalary) (COUNT(?s) AS ?cnt)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, SENSITIVITY, BOUNDS)
        assert ok

    def test_count_only_allowed(self, service):
        """COUNT alone (no AVG/SUM) does not require another COUNT."""
        query = """
            PREFIX oyd: <https://soya.ownyourdata.eu/Demo/>
            SELECT (COUNT(?gehalt) AS ?cnt)
            WHERE { ?s oyd:gehalt ?gehalt . }
        """
        ok, msg, _ = service.evaluate_query(query, SENSITIVITY, BOUNDS)
        assert ok
