import logging
from models.privacy_config import PrivacyConfig

logger = logging.getLogger(__name__)


class PrivacyBudgetService:
    """Tracks and enforces a global differential-privacy budget.

    The budget is held in memory for this phase.  A server restart resets
    it (known limitation, to be addressed in a later phase).
    """

    def __init__(self, config: PrivacyConfig):
        self._config = config
        self._epsilon_spent: float = 0.0

    # ── public API ──────────────────────────────────────────────────────

    def calculate_query_cost(self, num_aggregate_columns: int) -> float:
        """Return the ε cost for a query with *num_aggregate_columns* aggregates."""
        if num_aggregate_columns <= 0:
            return 0.0
        return self._config.epsilon_base * num_aggregate_columns

    def check_budget(self, epsilon_query: float) -> bool:
        """Return True if the remaining budget can cover *epsilon_query*."""
        return (self._epsilon_spent + epsilon_query) <= self._config.epsilon_total

    def deduct_budget(self, epsilon_query: float) -> None:
        """Deduct *epsilon_query* from the remaining budget."""
        self._epsilon_spent += epsilon_query
        logger.info(
            "Budget deducted: %.4f | spent: %.4f / %.4f",
            epsilon_query,
            self._epsilon_spent,
            self._config.epsilon_total,
        )

    def get_remaining(self) -> float:
        """Return the remaining privacy budget."""
        return max(0.0, self._config.epsilon_total - self._epsilon_spent)

    def reset(self) -> None:
        """Reset the budget (for testing / demo restarts only)."""
        self._epsilon_spent = 0.0
        logger.info("Privacy budget reset to %.4f", self._config.epsilon_total)
