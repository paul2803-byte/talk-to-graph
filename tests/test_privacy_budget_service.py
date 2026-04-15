"""Tests for PrivacyBudgetService."""
import pytest
from models.privacy_config import PrivacyConfig
from privacy.privacy_budget_service import PrivacyBudgetService


@pytest.fixture
def config():
    return PrivacyConfig(epsilon_total=1.0, epsilon_base=0.1, min_group_size=5, max_semi_sensitive_group_by=1)


@pytest.fixture
def service(config):
    return PrivacyBudgetService(config)


class TestCalculateQueryCost:
    def test_single_column(self, service):
        assert service.calculate_query_cost(1) == pytest.approx(0.1)

    def test_multiple_columns(self, service):
        assert service.calculate_query_cost(3) == pytest.approx(0.3)

    def test_zero_columns(self, service):
        assert service.calculate_query_cost(0) == 0.0

    def test_epsilon_override_single_column(self, service):
        """User-supplied epsilon should replace the default epsilon_base."""
        assert service.calculate_query_cost(1, epsilon_override=0.05) == pytest.approx(0.05)

    def test_epsilon_override_multiple_columns(self, service):
        assert service.calculate_query_cost(3, epsilon_override=0.2) == pytest.approx(0.6)

    def test_epsilon_override_none_uses_default(self, service):
        """Passing None should behave the same as omitting the argument."""
        assert service.calculate_query_cost(2, epsilon_override=None) == pytest.approx(0.2)


class TestBudgetChecking:
    def test_check_budget_ok(self, service):
        assert service.check_budget(0.5) is True

    def test_check_budget_exact(self, service):
        assert service.check_budget(1.0) is True

    def test_check_budget_exceeded(self, service):
        assert service.check_budget(1.1) is False


class TestBudgetDeduction:
    def test_deduct_reduces_remaining(self, service):
        service.deduct_budget(0.3)
        assert service.get_remaining() == pytest.approx(0.7)

    def test_multiple_deductions(self, service):
        service.deduct_budget(0.3)
        service.deduct_budget(0.4)
        assert service.get_remaining() == pytest.approx(0.3)

    def test_exhaustion_blocks_further_queries(self, service):
        service.deduct_budget(0.9)
        assert service.check_budget(0.2) is False

    def test_multi_column_exhaustion(self, service):
        """A query with 10 columns costs 1.0, exhausting the budget."""
        cost = service.calculate_query_cost(10)
        assert service.check_budget(cost) is True
        service.deduct_budget(cost)
        assert service.get_remaining() == pytest.approx(0.0)
        assert service.check_budget(0.1) is False


class TestReset:
    def test_reset_restores_budget(self, service):
        service.deduct_budget(0.8)
        service.reset()
        assert service.get_remaining() == pytest.approx(1.0)
