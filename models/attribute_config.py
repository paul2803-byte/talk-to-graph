from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class AttributeConfig:
    """Privacy-relevant configuration for a single ontology attribute.

    Consolidates the four separate maps previously maintained by the
    orchestrator (sensitivity_config, sensitivity_bounds,
    sensitivity_number_buckets, sensitivity_date_granularity) into one
    typed object.

    Attributes:
        sensitivity_level: One of "sensitive", "semi-sensitive", "not-sensitive".
        bounds: (min_value, max_value) for numeric attributes (used for DP noise).
        number_buckets: Number of buckets for numeric grouping.
        date_granularity: Granularity for date grouping (e.g. "YEAR", "DECADE").
    """
    sensitivity_level: str
    bounds: Optional[Tuple[float, float]] = None
    number_buckets: Optional[int] = None
    date_granularity: Optional[str] = None
