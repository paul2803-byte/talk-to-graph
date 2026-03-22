"""Tests for NoiseService — Laplace noise injection and small-group suppression."""
import pytest
from privacy.noise_service import NoiseService
from models.noisy_result import NoisyResult


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
        assert result.rows[0]["cnt"] != 100, "Noise should change the value"

    def test_laplace_scale_for_count(self, service):
        """Scale should be 1/ε.  With many draws, mean should ≈ original."""
        rows = [{"cnt": 1000}] * 200
        agg = [{"variable": "cnt", "function": "count", "attribute": None}]
        result = service.add_noise(rows, agg, {}, epsilon_base=1.0)
        values = [r["cnt"] for r in result.rows]
        mean_noise = sum(v - 1000 for v in values) / len(values)
        assert abs(mean_noise) < 5, f"Mean noise should be ~0, got {mean_noise}"

    def test_deterministic_seed(self):
        s1 = NoiseService(seed=99)
        s2 = NoiseService(seed=99)
        rows = [{"cnt": 50}]
        agg = [{"variable": "cnt", "function": "count", "attribute": None}]
        r1 = s1.add_noise(rows, agg, {}, epsilon_base=0.5)
        r2 = s2.add_noise([{"cnt": 50}], agg, {}, epsilon_base=0.5)
        assert r1.rows[0]["cnt"] == r2.rows[0]["cnt"]


# ── SUM noise ─────────────────────────────────────────────────────────────

class TestSumNoise:
    def test_noise_is_added_to_sum(self, service):
        rows = [{"total": 5000.0}]
        agg = [{"variable": "total", "function": "sum", "attribute": "gehalt"}]
        bounds = {"gehalt": (1000.0, 10000.0)}
        result = service.add_noise(rows, agg, bounds, epsilon_base=0.5)
        assert result.rows[0]["total"] != 5000.0

    def test_no_bounds_skips_noise(self, service):
        rows = [{"total": 5000.0}]
        agg = [{"variable": "total", "function": "sum", "attribute": "gehalt"}]
        result = service.add_noise(rows, agg, {}, epsilon_base=0.5)
        # Without bounds, noise cannot be added — value should remain unchanged
        assert result.rows[0]["total"] == 5000.0


# ── AVG noise (clipped-mean) ──────────────────────────────────────────────

class TestAvgNoise:
    def test_noise_is_added_to_avg_with_count(self, service):
        """When a COUNT column is present the clipped-mean path is used."""
        rows = [{"avgSalary": 5000.0, "cnt": 100}]
        agg = [
            {"variable": "cnt", "function": "count", "attribute": None},
            {"variable": "avgSalary", "function": "avg", "attribute": "gehalt"},
        ]
        bounds = {"gehalt": (1000.0, 10000.0)}
        result = service.add_noise(rows, agg, bounds, epsilon_base=0.5)
        assert result.rows[0]["avgSalary"] != 5000.0

    def test_avg_without_count_falls_back(self, service):
        """Without a COUNT column the fallback (full-sensitivity noise) is used."""
        rows = [{"avgSalary": 5000.0}]
        agg = [{"variable": "avgSalary", "function": "avg", "attribute": "gehalt"}]
        bounds = {"gehalt": (1000.0, 10000.0)}
        result = service.add_noise(rows, agg, bounds, epsilon_base=0.5)
        assert result.rows[0]["avgSalary"] != 5000.0

    def test_no_bounds_skips_avg_noise(self, service):
        rows = [{"avgSalary": 5000.0}]
        agg = [{"variable": "avgSalary", "function": "avg", "attribute": "gehalt"}]
        result = service.add_noise(rows, agg, {}, epsilon_base=0.5)
        assert result.rows[0]["avgSalary"] == 5000.0

    def test_clipped_mean_produces_reasonable_values(self):
        """Over many draws the noisy mean should stay close to the true mean."""
        svc = NoiseService(seed=123)
        rows = [{"avg": 5000.0, "cnt": 200}] * 300
        agg = [
            {"variable": "cnt", "function": "count", "attribute": None},
            {"variable": "avg", "function": "avg", "attribute": "gehalt"},
        ]
        bounds = {"gehalt": (1000.0, 10000.0)}
        result = svc.add_noise(rows, agg, bounds, epsilon_base=1.0)
        mean_noisy = sum(r["avg"] for r in result.rows) / len(result.rows)
        assert abs(mean_noisy - 5000.0) < 500, (
            f"Noisy mean {mean_noisy:.1f} deviates too far from 5000"
        )

    def test_count_not_double_noised_when_avg_present(self):
        """When COUNT and AVG are both present, COUNT noise should be applied once."""
        svc = NoiseService(seed=42)
        rows = [{"avgSalary": 5000.0, "cnt": 100}] * 500
        agg = [
            {"variable": "cnt", "function": "count", "attribute": None},
            {"variable": "avgSalary", "function": "avg", "attribute": "gehalt"},
        ]
        bounds = {"gehalt": (1000.0, 10000.0)}
        result = svc.add_noise(rows, agg, bounds, epsilon_base=1.0)
        # With ε=1, Lap(1/ε)=Lap(1), variance=2. Over 500 draws mean noise ≈ 0
        mean_count_noise = sum(r["cnt"] - 100 for r in result.rows) / len(result.rows)
        assert abs(mean_count_noise) < 2, (
            f"Count noise mean {mean_count_noise:.2f} suggests double-noising"
        )


# ── Small-group suppression ──────────────────────────────────────────────

class TestSuppressSmallGroups:
    def test_groups_below_threshold_are_removed(self, service):
        rows = [
            {"city": "Vienna", "cnt": 10.5},
            {"city": "Graz", "cnt": 2.3},
            {"city": "Linz", "cnt": 7.0},
        ]
        nr = NoisyResult(rows=rows, aggregate_info=[{"variable": "cnt", "function": "count"}])
        result = service.suppress_small_groups(nr, min_group_size=5)
        cities = [r["city"] for r in result.rows]
        assert "Graz" not in cities
        assert "Vienna" in cities
        assert "Linz" in cities

    def test_no_count_var_returns_all_rows(self, service):
        rows = [{"a": 1}, {"a": 2}]
        nr = NoisyResult(rows=rows, aggregate_info=[])
        result = service.suppress_small_groups(nr, min_group_size=5)
        assert len(result.rows) == 2

    def test_single_row_below_threshold(self, service):
        rows = [{"cnt": 1.0}]
        nr = NoisyResult(rows=rows, aggregate_info=[{"variable": "cnt", "function": "count"}])
        result = service.suppress_small_groups(nr, min_group_size=5)
        assert len(result.rows) == 0
