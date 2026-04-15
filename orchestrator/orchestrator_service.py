import logging
from typing import Any, Optional
from orchestrator.fetch_ontology_service import FetchOntologyService
from query_execution import QueryExecutionService
from query_evaluation import QueryEvaluationService
from query_generation import generate_sparql_query
from query_generation.response_generator import ResponseGenerator
from privacy import NoiseService, PrivacyBudgetService
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
        self.session_service.add_to_history(sid, "user", question)

        # ── 1. Fetch ontology ──────────────────────────────────────────
        try:
            ontology_obj = self.fetch_service.fetch_ontology(ontology_url)
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
            logger.info("Generated SPARQL Query:\n%s", sparql_query)
        except Exception as e:
            logger.error("SPARQL generation failed: %s", e)
            return self._error_response(
                "QUERY_GENERATION_FAILED", sid, session.conversation_history
            )

        # ── 3. Build sensitivity config and bounds from ontology ───────
        sensitivity_config = {}
        sensitivity_bounds = {}
        for obj in ontology_obj.objects:
            for attr in obj.attributes:
                sensitivity_config[attr.name] = attr.sensitivity_level
                if attr.min_value is not None and attr.max_value is not None:
                    sensitivity_bounds[attr.name] = (attr.min_value, attr.max_value)

        # ── 4. Static evaluation (R1-R6) ──────────────────────────────
        is_valid, eval_message, aggregate_info = self.evaluation_service.evaluate_query(
            sparql_query, sensitivity_config, sensitivity_bounds,
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
        epsilon_per_column = epsilon if epsilon is not None else self._config.epsilon_base

        num_agg_columns = len(aggregate_info)
        epsilon_query = self.budget_service.calculate_query_cost(
            num_agg_columns, epsilon_override=epsilon_per_column
        )

        if not self.budget_service.check_budget(epsilon_query):
            return self._error_response(
                "BUDGET_EXHAUSTED", sid, session.conversation_history
            )

        # ── 6. Deduct budget BEFORE executing ───
        self.budget_service.deduct_budget(epsilon_query)
        self.session_service.add_epsilon_spent(sid, epsilon_query)

        # ── 7. Execute query ───────────────────────────────────────────
        try:
            query_results = self.execution_service.execute_sparql_query(sparql_query, data)
        except Exception as e:
            logger.error("Query execution failed: %s", e)
            return self._error_response(
                "QUERY_EXECUTION_FAILED", sid, session.conversation_history
            )

        # ── 8. Add Laplace noise ───────────────────────────────────────
        noisy_result = self.noise_service.add_noise(
            query_results,
            aggregate_info,
            sensitivity_bounds,
            epsilon_per_column,
        )

        # ── 9. Suppress small groups (uses noisy counts) ──────────────
        noisy_result = self.noise_service.suppress_small_groups(
            noisy_result, self._config.min_group_size
        )

        # ── 10. Build and return response ─────────────────────────────
        # Generate natural language response
        response_text = self.response_generator.generate_response(
            question, noisy_result
        )

        # Append assistant response to conversation history
        self.session_service.add_to_history(sid, "assistant", response_text)

        # Return response
        return {
            "response": response_text,
            "sessionId": sid,
            "remainingPrivacyBudget": self.budget_service.get_remaining(),
            "sessionEpsilonSpent": session.epsilon_spent,
            "epsilonUsed": epsilon_per_column,
            "status": "success",
            "data": {
                "query_results": noisy_result.rows,
                "sparql_query": sparql_query,
            },
            "conversationHistory": list(session.conversation_history),
        }
