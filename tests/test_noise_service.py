"""Tests for NoiseService — Laplace noise injection and small-group suppression."""
import pytest
from privacy.noise_service import NoiseService


@pytest.fixture
def service():
    """Create a NoiseService with a fixed seed for reproducibility."""
    return NoiseService(seed=42)


# ── COUNT noise ───────────────────────────────────────────────────────────

class TestCountNoise:
    def test_noise_is_added_to_count(self, service):
        rows = [{"cnt": 100}]
        agg = [{"variable": "cnt", "function": "count", "attribute": None}]
        result = service.add_noise(rows, agg, {}, epsilon_base=0.5)
        assert result[0]["cnt"] != 100, "Noise should change the value"

    def test_laplace_scale_for_count(self, service):
        """Scale should be 1/ε.  With many draws, mean should ≈ original."""
        rows = [{"cnt": 1000}] * 200
        agg = [{"variable": "cnt", "function": "count", "attribute": None}]
        result = service.add_noise(rows, agg, {}, epsilon_base=1.0)
        values = [r["cnt"] for r in result]
        mean_noise = sum(v - 1000 for v in values) / len(values)
        assert abs(mean_noise) < 5, f"Mean noise should be ~0, got {mean_noise}"

    def test_deterministic_seed(self):
        s1 = NoiseService(seed=99)
        s2 = NoiseService(seed=99)
        rows = [{"cnt": 50}]
        agg = [{"variable": "cnt", "function": "count", "attribute": None}]
        r1 = s1.add_noise(rows, agg, {}, epsilon_base=0.5)
        r2 = s2.add_noise([{"cnt": 50}], agg, {}, epsilon_base=0.5)
        assert r1[0]["cnt"] == r2[0]["cnt"]


# ── SUM noise ─────────────────────────────────────────────────────────────

class TestSumNoise:
    def test_noise_is_added_to_sum(self, service):
        rows = [{"total": 5000.0}]
        agg = [{"variable": "total", "function": "sum", "attribute": "gehalt"}]
        bounds = {"gehalt": (1000.0, 10000.0)}
        result = service.add_noise(rows, agg, bounds, epsilon_base=0.5)
        assert result[0]["total"] != 5000.0

    def test_no_bounds_skips_noise(self, service):
        rows = [{"total": 5000.0}]
        agg = [{"variable": "total", "function": "sum", "attribute": "gehalt"}]
        result = service.add_noise(rows, agg, {}, epsilon_base=0.5)
        # Without bounds, noise cannot be added — value should remain unchanged
        assert result[0]["total"] == 5000.0


# ── AVG noise (clipped-mean) ──────────────────────────────────────────────

class TestAvgNoise:
    def test_noise_is_added_to_avg(self, service):
        rows = [{"avgSalary": 5000.0}]
        agg = [{"variable": "avgSalary", "function": "avg", "attribute": "gehalt"}]
        bounds = {"gehalt": (1000.0, 10000.0)}
        result = service.add_noise(rows, agg, bounds, epsilon_base=0.5)
        assert result[0]["avgSalary"] != 5000.0

    def test_no_bounds_skips_avg_noise(self, service):
        rows = [{"avgSalary": 5000.0}]
        agg = [{"variable": "avgSalary", "function": "avg", "attribute": "gehalt"}]
        result = service.add_noise(rows, agg, {}, epsilon_base=0.5)
        assert result[0]["avgSalary"] == 5000.0


# ── Small-group suppression ──────────────────────────────────────────────

class TestSuppressSmallGroups:
    def test_groups_below_threshold_are_removed(self, service):
        rows = [
            {"city": "Vienna", "cnt": 10.5},
            {"city": "Graz", "cnt": 2.3},
            {"city": "Linz", "cnt": 7.0},
        ]
        result = service.suppress_small_groups(rows, count_var="cnt", min_group_size=5)
        cities = [r["city"] for r in result]
        assert "Graz" not in cities
        assert "Vienna" in cities
        assert "Linz" in cities

    def test_no_count_var_returns_all_rows(self, service):
        rows = [{"a": 1}, {"a": 2}]
        result = service.suppress_small_groups(rows, count_var=None, min_group_size=5)
        assert len(result) == 2

    def test_single_row_below_threshold(self, service):
        rows = [{"cnt": 1.0}]
        result = service.suppress_small_groups(rows, count_var="cnt", min_group_size=5)
        assert len(result) == 0
