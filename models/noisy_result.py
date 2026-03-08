"""
NoisyResult — type-safe wrapper for differentially private query results.

Only ``NoiseService.add_noise()`` should produce instances of this type.
Downstream consumers (like ResponseGenerator) accept ``NoisyResult`` rather
than raw dicts, enforcing at the type level that un-noised data can never
reach the response layer.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass(frozen=True)
class NoisyResult:
    """Immutable container for post-noise, post-suppression query results.

    Attributes:
        rows: The noisy result rows (each row is a dict of column→value).
        aggregate_info: Metadata about the aggregate columns, as produced by
            ``QueryEvaluationService.evaluate_query()``.
    """

    rows: List[Dict[str, Any]] = field(default_factory=list)
    aggregate_info: List[Dict[str, str]] = field(default_factory=list)
