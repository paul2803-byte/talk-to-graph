from typing import Any
from orchestrator.fetch_ontology_service import FetchOntologyService
from query_execution import QueryExecutionService
from query_evaluation import QueryEvaluationService
from privacy import NoiseService, PrivacyBudgetService
from models.privacy_config import PrivacyConfig


class OrchestratorService:
    def __init__(self):
        self.fetch_service = FetchOntologyService()
        self.execution_service = QueryExecutionService()
        self.evaluation_service = QueryEvaluationService()

        config = PrivacyConfig.from_env()
        self.budget_service = PrivacyBudgetService(config)
        self.noise_service = NoiseService()
        self._config = config

    def talk_to_data(self, question: str, data: Any, ontology_url: str) -> dict:
        """
        Orchestration method to coordinate calls between different services.

        Pipeline:
            fetch ontology → generate SPARQL → static eval (R1-R6)
            → budget check → execute → add noise → suppress small groups
            → deduct budget → return
        """
        # ── 1. Fetch ontology ──────────────────────────────────────────
        try:
            ontology_obj = self.fetch_service.fetch_ontology(ontology_url)
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to fetch ontology: {str(e)}"
            }

        # ── 2. Generate SPARQL query ───────────────────────────────────
        try:
            # sparql_query = generate_sparql_query(ontology_obj, question)
            sparql_query = """
                PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo2/>
                SELECT (AVG(?gehalt) AS ?averageSalary)
                WHERE {
                    ?s a oyd:Object1 ;
                    oyd:gehalt ?gehalt .
                }
            """
            print(f"Generated SPARQL Query:\n{sparql_query}")
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to generate SPARQL query: {str(e)}"
            }

        # ── 3. Build sensitivity config and bounds from ontology ───────
        sensitivity_config = {}
        sensitivity_bounds = {}
        attribute_bounds = {}
        for obj in ontology_obj.objects:
            for attr in obj.attributes:
                sensitivity_config[attr.name] = attr.sensitivity_level
                if attr.min_value is not None and attr.max_value is not None:
                    sensitivity_bounds[attr.name] = (attr.min_value, attr.max_value)
                    attribute_bounds[attr.name] = (attr.min_value, attr.max_value)

        # ── 4. Static evaluation (R1-R6) ──────────────────────────────
        is_valid, eval_message, aggregate_info = self.evaluation_service.evaluate_query(
            sparql_query, sensitivity_config, sensitivity_bounds,
            max_semi_sensitive_group_by=self._config.max_semi_sensitive_group_by,
        )
        if not is_valid:
            return {
                "status": "error",
                "message": f"Query rejected by static evaluation: {eval_message}"
            }

        # ── 5. Budget check ────────────────────────────────────────────
        num_agg_columns = len(aggregate_info)
        epsilon_query = self.budget_service.calculate_query_cost(num_agg_columns)

        if not self.budget_service.check_budget(epsilon_query):
            return {
                "status": "error",
                "message": (
                    f"Privacy budget exhausted. Remaining: "
                    f"{self.budget_service.get_remaining():.4f}, "
                    f"required: {epsilon_query:.4f}"
                ),
                "remaining_budget": self.budget_service.get_remaining(),
            }

        # ── 6. Execute query ───────────────────────────────────────────
        try:
            query_results = self.execution_service.execute_sparql_query(sparql_query, data)
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to execute SPARQL query: {str(e)}"
            }

        # ── 7. Add Laplace noise ───────────────────────────────────────
        noisy_results = self.noise_service.add_noise(
            query_results,
            aggregate_info,
            attribute_bounds,
            self._config.epsilon_base,
        )

        # ── 8. Suppress small groups (uses noisy counts) ──────────────
        # Find the count variable if there is one
        count_var = None
        for agg in aggregate_info:
            if agg["function"] == "count":
                count_var = agg["variable"]
                break

        noisy_results = self.noise_service.suppress_small_groups(
            noisy_results, count_var, self._config.min_group_size
        )

        # ── 9. Deduct budget ───────────────────────────────────────────
        self.budget_service.deduct_budget(epsilon_query)

        # ── 10. Return result ──────────────────────────────────────────
        return {
            "status": "success",
            "message": "Orchestrator processed the query and executed it on data",
            "remaining_budget": self.budget_service.get_remaining(),
            "details": {
                "question": question,
                "sparql_query": sparql_query,
                "query_results": noisy_results,
            }
        }
