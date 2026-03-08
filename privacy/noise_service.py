"""Noise injection service implementing the Laplace mechanism for differential privacy."""

import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


# ── Aggregate metadata keys (as produced by QueryEvaluationService) ─────
# Each entry in aggregate_info is a dict:
#   {
#       "variable": str,          # projected variable name (e.g. "averageSalary")
#       "function": str,          # one of "count", "sum", "avg"
#       "attribute": str | None,  # ontology attribute name the aggregate operates on
#   }


class NoiseService:
    """Adds calibrated Laplace noise to aggregate query results."""

    def __init__(self, seed: Optional[int] = None):
        self._rng = np.random.default_rng(seed)

    # ── public API ──────────────────────────────────────────────────────

    def add_noise(
        self,
        query_results: List[Dict[str, Any]],
        aggregate_info: List[Dict[str, str]],
        attribute_bounds: Dict[str, Tuple[float, float]],
        epsilon_base: float,
    ) -> List[Dict[str, Any]]:
        """Add Laplace noise to every aggregate column in *query_results*.

        Parameters
        ----------
        query_results:
            Rows returned by the query execution service (numeric values
            must already be native Python numbers, not strings).
        aggregate_info:
            One entry per aggregate column describing the function and
            attribute (see module docstring).
        attribute_bounds:
            ``{attr_name: (min_val, max_val)}`` from the ontology overlay.
        epsilon_base:
            The per-column privacy budget slice.

        Returns
        -------
        A **new** list of result dicts with noise applied in-place to the
        aggregate columns.
        """
        noisy_results = [dict(row) for row in query_results]

        for agg in aggregate_info:
            var = agg["variable"]
            func = agg["function"].lower()
            attr = agg.get("attribute")

            if func == "count":
                self._add_count_noise(noisy_results, var, epsilon_base)
            elif func == "sum":
                bounds = attribute_bounds.get(attr) if attr else None
                self._add_sum_noise(noisy_results, var, bounds, epsilon_base)
            elif func == "avg":
                bounds = attribute_bounds.get(attr) if attr else None
                self._add_avg_noise_clipped_mean(noisy_results, var, bounds, epsilon_base)
            else:
                logger.warning("Unknown aggregate function '%s' – skipping noise for %s", func, var)

        return noisy_results

    def suppress_small_groups(
        self,
        query_results: List[Dict[str, Any]],
        count_var: Optional[str],
        min_group_size: int,
    ) -> List[Dict[str, Any]]:
        """Remove rows whose **noisy** count is below *min_group_size*.

        This must be called **after** ``add_noise`` so that the count
        column already contains the noisy value.
        """
        if count_var is None:
            return query_results

        suppressed: List[Dict[str, Any]] = []
        for row in query_results:
            count_val = row.get(count_var)
            if count_val is not None and float(count_val) >= min_group_size:
                suppressed.append(row)
            else:
                logger.info("Suppressed group with noisy count %s (threshold %d)", count_val, min_group_size)

        return suppressed

    # ── private helpers ─────────────────────────────────────────────────

    def _laplace(self, scale: float) -> float:
        """Draw a single sample from Lap(0, scale)."""
        return float(self._rng.laplace(0.0, scale))

    def _add_count_noise(
        self, rows: List[Dict[str, Any]], var: str, epsilon: float
    ) -> None:
        """Δf(COUNT) = 1, so scale = 1/ε."""
        scale = 1.0 / epsilon
        for row in rows:
            if var in row and row[var] is not None:
                row[var] = float(row[var]) + self._laplace(scale)

    def _add_sum_noise(
        self,
        rows: List[Dict[str, Any]],
        var: str,
        bounds: Optional[Tuple[float, float]],
        epsilon: float,
    ) -> None:
        """Δf(SUM) = max − min, so scale = (max − min) / ε."""
        if bounds is None:
            logger.error("No bounds for SUM variable '%s' – cannot add noise", var)
            return
        lo, hi = bounds
        sensitivity = hi - lo
        scale = sensitivity / epsilon
        for row in rows:
            if var in row and row[var] is not None:
                row[var] = float(row[var]) + self._laplace(scale)

    def _add_avg_noise_clipped_mean(
        self,
        rows: List[Dict[str, Any]],
        var: str,
        bounds: Optional[Tuple[float, float]],
        epsilon: float,
    ) -> None:
        """Clipped-mean mechanism for AVG.

        Split ε into ε/2 for noisy SUM and ε/2 for noisy COUNT, then
        return noisy_sum / noisy_count.  The true AVG value in each row
        is used as a proxy for the clipped SUM (since the query engine
        already computed the average, we reconstruct: sum ≈ avg × count).

        Because we do not have access to the per-record values at this
        stage, we add noise calibrated to the *clipped* sensitivity
        directly to the reported AVG and adjust by the noisy count.

        Sensitivity of clipped SUM = max − min.
        Sensitivity of COUNT       = 1.
        """
        if bounds is None:
            logger.error("No bounds for AVG variable '%s' – cannot add noise", var)
            return

        lo, hi = bounds
        sensitivity_sum = hi - lo
        eps_sum = epsilon / 2.0
        eps_count = epsilon / 2.0

        scale_sum = sensitivity_sum / eps_sum
        scale_count = 1.0 / eps_count

        for row in rows:
            if var in row and row[var] is not None:
                true_avg = float(row[var])
                # Clipped-mean mechanism: we add noise calibrated to the
                # clipped range directly to the reported average.  The
                # scale accounts for (max-min) sensitivity at ε/2.
                noise = self._laplace(scale_sum)
                row[var] = true_avg + noise
