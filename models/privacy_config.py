import os
from dataclasses import dataclass


@dataclass
class PrivacyConfig:
    """Configuration for differential privacy parameters.

    Attributes:
        epsilon_total: Total privacy budget for the session.
        epsilon_base: Base privacy cost per aggregate column in a query.
        min_group_size: Minimum group size for small-group suppression.
        max_semi_sensitive_group_by: Maximum number of semi-sensitive
            attributes allowed in a GROUP BY clause.
    """
    epsilon_total: float
    epsilon_base: float
    min_group_size: int
    max_semi_sensitive_group_by: int

    @classmethod
    def from_env(cls) -> "PrivacyConfig":
        """Load configuration from environment variables with sensible defaults."""
        return cls(
            epsilon_total=float(os.getenv("EPSILON_TOTAL", "1.0")),
            epsilon_base=float(os.getenv("EPSILON_BASE", "0.1")),
            min_group_size=int(os.getenv("MIN_GROUP_SIZE", "5")),
            max_semi_sensitive_group_by=int(os.getenv("MAX_SEMI_SENSITIVE_GROUP_BY", "1")),
        )
