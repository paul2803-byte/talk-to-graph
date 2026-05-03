import logging
from typing import Any, Dict, List, Optional, Tuple
from orchestrator.fetch_ontology_service import FetchOntologyService
from query_execution import QueryExecutionService
from query_evaluation import QueryEvaluationService
from query_generation import generate_sparql_query
from query_generation.response_generator import ResponseGenerator
from privacy import NoiseService, PrivacyBudgetService
from models.attribute_config import AttributeConfig
from models.privacy_config import PrivacyConfig
from models.noisy_result import NoisyResult
from session import SessionService

logger = logging.getLogger(__name__)


# ── Error codes & user-safe messages ────────────────────────────────────

_ERROR_MESSAGES = {
    "ONTOLOGY_FETCH_FAILED": "Unable to load the data schema. Please try again later.",
    "QUERY_GENERATION_FAILED": "Unable to process your question. Please rephrase and try again.",
    "QUERY_REJECTED": "Your question could not be answered due to privacy constraints.",
    "BUDGET_EXHAUSTED": "The privacy budget for this session has been exhausted.",
    "QUERY_EXECUTION_FAILED": "An error occurred while running the query. Please try again.",
}


class OrchestratorService:
    def __init__(self):
        self.fetch_service = FetchOntologyService()
        self.execution_service = QueryExecutionService()
        self.evaluation_service = QueryEvaluationService()

        config = PrivacyConfig.from_env()
        # Global budget (shared across all sessions) — see plan review item 3.2
        self.budget_service = PrivacyBudgetService(config)
        self.noise_service = NoiseService()
        self.response_generator = ResponseGenerator()
        self.session_service = SessionService()
        self._config = config

    # ── helper: build error response ────────────────────────────────────

    def _error_response(
        self,
        error_code: str,
        session_id: str,
        conversation_history: list,
    ) -> dict:
        """Return a standardised error dict.  The user-safe message is also
        appended to the conversation history as an assistant turn."""
        message = _ERROR_MESSAGES.get(error_code, "An unexpected error occurred.")

        # Append the error message to conversation history
        self.session_service.add_to_history(session_id, "assistant", message)
        session = self.session_service.get_session(session_id)
        history = session.conversation_history if session else conversation_history

        return {
            "response": message,
            "sessionId": session_id,
            "remainingPrivacyBudget": self.budget_service.get_remaining(),
            "sessionEpsilonSpent": session.epsilon_spent if session else 0.0,
            "status": "error",
            "errorCode": error_code,
            "data": None,
            "conversationHistory": list(history),
        }

    # ── main pipeline ───────────────────────────────────────────────────

    def talk_to_data(
        self,
        question: str,
        data: Any,
        ontology_url: str,
        session_id: Optional[str] = None,
        epsilon: Optional[float] = None,
    ) -> dict:
        """
        Orchestration method to coordinate calls between different services.

        Pipeline:
            fetch ontology → generate SPARQL → static eval (R1-R6)
            → budget check → deduct budget → execute → add noise
            → suppress small groups → generate NL response → return
        """
        # ── 0. Session management ──────────────────────────────────────
        session = self.session_service.get_or_create_session(session_id)
        sid = session.session_id

        # Record user question
        logger.info("Received question for session %s: %s", sid, question)
        self.session_service.add_to_history(sid, "user", question)

        # ── 1. Fetch ontology ──────────────────────────────────────────
        try:
            ontology_obj = self.fetch_service.fetch_ontology(ontology_url)
            logger.info("Fetched ontology successfully.")
        except Exception as e:
            logger.error("Ontology fetch failed: %s", e)
            return self._error_response(
                "ONTOLOGY_FETCH_FAILED", sid, session.conversation_history
            )

        # ── 2. Generate SPARQL query ───────────────────────────────────
        try:
            sparql_query = generate_sparql_query(ontology_obj, question)
            # test queries for saving tokens during development
            sparql_query_test = """
                PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo2/>
                SELECT (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?count)
                WHERE {
                    ?s a oyd:Object1 ;
                    oyd:gehalt ?gehalt .
                }
            """
            sparql_query_test2 = """
                PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo2/>
                SELECT ?salary WHERE { 
                    ?s a oyd:Object1 ;
                    oyd:gehalt ?salary .
                }
            """
            logger.debug("Generated SPARQL Query:\n%s", sparql_query)
            logger.info("Generated SPARQL query successfully.")
        except Exception as e:
            logger.error("SPARQL generation failed: %s", e)
            return self._error_response(
                "QUERY_GENERATION_FAILED", sid, session.conversation_history
            )

        # ── 3. Build sensitivity config and bounds from ontology ───────
        attribute_configs: Dict[str, AttributeConfig] = {}

        def _register_attr(attr):
            """Register an attribute and recursively its children."""
            if attr.name not in attribute_configs:
                bounds = None
                if attr.min_value is not None and attr.max_value is not None:
                    bounds = (attr.min_value, attr.max_value)
                attribute_configs[attr.name] = AttributeConfig(
                    sensitivity_level=attr.sensitivity_level,
                    bounds=bounds,
                    number_buckets=attr.number_buckets,
                    date_granularity=attr.date_granularity,
                )
            for child in attr.children:
                _register_attr(child)

        for obj in ontology_obj.objects:
            for attr in obj.attributes:
                _register_attr(attr)

        # ── 4. Static evaluation (R1-R9) ──────────────────────────────
        is_valid, eval_message, aggregate_info = self.evaluation_service.evaluate_query(
            sparql_query, attribute_configs,
            max_semi_sensitive_group_by=self._config.max_semi_sensitive_group_by,
        )

        if not is_valid:
            logger.warning("Query rejected: %s", eval_message)
            return self._error_response(
                "QUERY_REJECTED", sid, session.conversation_history
            )

        # ── 5. Budget check ────────────────────────────────────────────
        # Use user-supplied epsilon if provided, otherwise fall back to
        # the configured default (epsilon_base).
        epsilon_query = epsilon if epsilon is not None else self._config.epsilon_base
        weighted_epsilon = self.budget_service.calculate_adjusted_epsilon(
            len(aggregate_info),
            epsilon_query,
        )

        if not self.budget_service.check_budget(epsilon_query):
            return self._error_response(
                "BUDGET_EXHAUSTED", sid, session.conversation_history
            )

        # ── 6. Deduct budget BEFORE executing ───
        self.budget_service.deduct_budget(epsilon_query)
        self.session_service.add_epsilon_spent(sid, epsilon_query)
        logger.info("Privacy budget check passed. Deducted %s.", epsilon_query)

        # ── 7. Execute query ───────────────────────────────────────────
        try:
            query_results = self.execution_service.execute_sparql_query(sparql_query, data)
            logger.info("Query executed successfully. Returned %s rows.", len(query_results))
        except Exception as e:
            logger.error("Query execution failed: %s", e)
            return self._error_response(
                "QUERY_EXECUTION_FAILED", sid, session.conversation_history
            )

        # ── 8. Add Laplace noise ───────────────────────────────────────
        noisy_result = self.noise_service.add_noise(
            query_results,
            aggregate_info,
            attribute_configs,
            weighted_epsilon,
        )

        # ── 9. Suppress small groups (uses noisy counts) ──────────────
        noisy_result = self.noise_service.suppress_small_groups(
            noisy_result, self._config.min_group_size
        )
        logger.info("Applied Laplace noise and suppressed small groups.")

        # ── 9.5. Humanize bucket labels ────────────────────────────────
        noisy_result = self._humanize_bucket_labels(
            noisy_result, attribute_configs
        )

        # ── 10. Build and return response ─────────────────────────────
        # Generate natural language response
        response_text = self.response_generator.generate_response(
            question, noisy_result
        )
        logger.info("Generated natural language response successfully.")

        # Append assistant response to conversation history
        self.session_service.add_to_history(sid, "assistant", response_text)

        # Return response
        return {
            "response": response_text,
            "sessionId": sid,
            "remainingPrivacyBudget": self.budget_service.get_remaining(),
            "sessionEpsilonSpent": session.epsilon_spent,
            "epsilonUsed": weighted_epsilon,
            "status": "success",
            "data": {
                "query_results": noisy_result.rows,
                "sparql_query": sparql_query,
            },
            "conversationHistory": list(session.conversation_history),
        }

    # ── bucket label humanization ────────────────────────────────────────

    @staticmethod
    def _humanize_bucket_labels(
        noisy_result: NoisyResult,
        attribute_configs: Dict[str, AttributeConfig],
    ) -> NoisyResult:
        """Replace raw bucket numbers with human-readable range labels.

        Detects columns whose name contains ``bucket_`` or ``_bucket`` and
        maps the raw FLOOR-division result back to a range string.

        For numeric attributes:  ``9``  →  ``"144 – 160"``
        For date decades:        ``198``  →  ``"1980 – 1990"``
        """
        if not noisy_result.rows:
            return noisy_result

        # Identify bucket columns from the first row's keys
        sample_keys = list(noisy_result.rows[0].keys())
        bucket_columns: List[Tuple[str, str]] = []  # (col_name, attr_name)

        for col in sample_keys:
            col_lower = col.lower()
            # Extract attribute name from column like "bucket_gewicht" or "gewicht_bucket"
            attr_name = None
            if col_lower.startswith("bucket_"):
                attr_name = col_lower[len("bucket_"):]
            elif col_lower.endswith("_bucket"):
                attr_name = col_lower[:-len("_bucket")]
            elif "bucket" in col_lower:
                # Try removing "bucket" and underscores
                candidate = col_lower.replace("bucket", "").strip("_")
                if candidate:
                    attr_name = candidate

            if attr_name:
                bucket_columns.append((col, attr_name))

        if not bucket_columns:
            return noisy_result

        # Build lookup for each bucket column
        humanized_rows = [dict(row) for row in noisy_result.rows]

        for col_name, attr_name in bucket_columns:
            cfg = attribute_configs.get(attr_name) or attribute_configs.get(attr_name.lower())
            
            # Check numeric bucketing
            num_buckets = cfg.number_buckets if cfg else None
            bounds = cfg.bounds if cfg else None

            if num_buckets and bounds:
                lo, hi = bounds
                bucket_size = (hi - lo) / num_buckets
                for row in humanized_rows:
                    val = row.get(col_name)
                    if val is not None:
                        try:
                            bucket_num = int(float(val))
                            range_lo = round(bucket_num * bucket_size, 1)
                            range_hi = round((bucket_num + 1) * bucket_size, 1)
                            # Use integers if bucket_size produces whole numbers
                            if bucket_size == int(bucket_size):
                                range_lo, range_hi = int(range_lo), int(range_hi)
                            row[col_name] = f"{range_lo} – {range_hi}"
                        except (ValueError, TypeError):
                            pass
                continue

            # Check date bucketing (DECADE)
            date_gran = cfg.date_granularity if cfg else None
            if date_gran == "DECADE":
                for row in humanized_rows:
                    val = row.get(col_name)
                    if val is not None:
                        try:
                            decade_num = int(float(val))
                            year_start = decade_num * 10
                            row[col_name] = f"{year_start} – {year_start + 10}"
                        except (ValueError, TypeError):
                            pass
            elif date_gran == "YEAR":
                for row in humanized_rows:
                    val = row.get(col_name)
                    if val is not None:
                        try:
                            row[col_name] = str(int(float(val)))
                        except (ValueError, TypeError):
                            pass

        return NoisyResult(rows=humanized_rows, aggregate_info=noisy_result.aggregate_info)
