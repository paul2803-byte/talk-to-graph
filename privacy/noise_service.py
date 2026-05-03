import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from models.noisy_result import NoisyResult

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
        attribute_configs: Dict[str, 'AttributeConfig'],
        weighted_epsilon: float,
    ) -> NoisyResult:
        """Add Laplace noise to every aggregate column in *query_results*.

        Parameters
        ----------
        query_results:
            Rows returned by the query execution service (numeric values
            must already be native Python numbers, not strings).
        aggregate_info:
            One entry per aggregate column describing the function and
            attribute (see module docstring).
        attribute_configs:
            ``{attr_name: AttributeConfig}`` containing bounds from the ontology overlay.
        epsilon_base:
            The per-column privacy budget slice.

        Returns
        -------
        A **new** NoisyResult with noise applied to the aggregate columns.
        """
        noisy_results = [dict(row) for row in query_results]

        count_var = None
        for agg in aggregate_info:
            if agg["function"] == "count":
                count_var = agg["variable"]
                break

        # Snapshot the true counts before any noise is applied so that
        # the AVG mechanism can reconstruct the clipped sum correctly.
        true_counts: List[Optional[float]] = []
        for row in noisy_results:
            if count_var and count_var in row and row[count_var] is not None:
                true_counts.append(float(row[count_var]))
            else:
                true_counts.append(None)

        for agg in aggregate_info:
            var = agg["variable"]
            func = agg["function"].lower()
            attr = agg.get("attribute")

            cfg = attribute_configs.get(attr) if attr else None
            bounds = cfg.bounds if cfg else None

            if func == "count":
                self._add_count_noise(noisy_results, var, weighted_epsilon)
            elif func == "sum":
                self._add_sum_noise(noisy_results, var, bounds, weighted_epsilon)
            elif func == "avg":
                self._add_avg_noise_clipped_mean(
                    noisy_results, var, bounds, weighted_epsilon, true_counts
                )
            else:
                logger.warning("Unknown aggregate function '%s' – skipping noise for %s", func, var)

        return NoisyResult(rows=noisy_results, aggregate_info=aggregate_info)

    def suppress_small_groups(
        self,
        noisy_result: NoisyResult,
        min_group_size: int,
    ) -> NoisyResult:
        """Remove rows whose **noisy** count is below *min_group_size*.

        This must be called **after** ``add_noise`` so that the count
        column already contains the noisy value.
        """
        count_var = None
        for agg in noisy_result.aggregate_info:
            if agg["function"] == "count":
                count_var = agg["variable"]
                break

        if count_var is None:
            return noisy_result

        suppressed: List[Dict[str, Any]] = []
        for row in noisy_result.rows:
            count_val = row.get(count_var)
            if count_val is not None and float(count_val) >= min_group_size:
                suppressed.append(row)
            else:
                logger.info("Suppressed group with noisy count %s (threshold %d)", count_val, min_group_size)

        return NoisyResult(rows=suppressed, aggregate_info=noisy_result.aggregate_info)

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
        """Δf(SUM) = max - min, so scale = (max - min) / ε."""
        if bounds is None:
            logger.error("No bounds for SUM variable '%s' - cannot add noise", var)
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
        true_counts: Optional[List[Optional[float]]] = None,
    ) -> None:
        """Clipped-mean mechanism for AVG.

        Split ε into ε/2 for noisy SUM and ε/2 for noisy COUNT, then
        return noisy_sum / noisy_count.  The true AVG value in each row
        is used together with the **true** (un-noised) group count to
        reconstruct: clipped_sum = avg × true_count.  Independent
        Laplace noise is added to both the reconstructed sum and count
        before dividing.

        When no count column is available, the method falls back to
        adding noise scaled to (max − min) / ε directly to the average.

        Sensitivity of clipped SUM = max - min.
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

        for idx, row in enumerate(rows):
            if var in row and row[var] is not None:
                true_avg = float(row[var])

                # Use the true (un-noised) count for sum reconstruction
                n = true_counts[idx] if true_counts else None

                if n is not None and n > 0:
                    # Reconstruct clipped sum from true values, then
                    # add independent noise to both sum and count
                    clipped_sum = true_avg * n
                    noisy_sum = clipped_sum + self._laplace(scale_sum)
                    noisy_count = n + self._laplace(scale_count)

                    # Guard against division by zero / negative noisy counts
                    if noisy_count < 1.0:
                        noisy_count = 1.0

                    row[var] = noisy_sum / noisy_count
                else:
                    # Fallback: no count available — add noise scaled to
                    # the full clipped range (conservative / over-noised,
                    # but safe from a DP perspective).
                    logger.warning(
                        "No count column available for AVG variable '%s' "
                        "– falling back to full-sensitivity noise", var
                    )
                    row[var] = true_avg + self._laplace(scale_sum)

