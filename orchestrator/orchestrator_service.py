import json
from typing import Any
from orchestrator.fetch_ontology_service import FetchOntologyService
from query_generation import generate_sparql_query
from query_execution import QueryExecutionService

class OrchestratorService:
    def __init__(self):
        self.fetch_service = FetchOntologyService()
        self.execution_service = QueryExecutionService()

    def talk_to_data(self, question: str, data: Any, ontology_url: str) -> dict:
        """
        Orchestration method to coordinate calls between different services.
        
        Args:
            question (str): The user's natural language question.
            data (Any): Data to be processed (empty for now).
            ontology_url (str): URL to the ontology for query creation.
            
        Returns:
            dict: The result of the orchestration process.
        """
        # fetch ontology from ontology_url
        try:
            ontology_obj = self.fetch_service.fetch_ontology(ontology_url)
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to fetch ontology: {str(e)}"
            }

        # generate sparql query from question and ontology
        try:
            sparql_query = generate_sparql_query(ontology_obj, question)
            print(f"Generated SPARQL Query:\n{sparql_query}")
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to generate SPARQL query: {str(e)}"
            }

        # TODO #3 validate generalization on sensitve attributes
        
        # execute sparql query on data
        try:
            query_results = self.execution_service.execute_sparql_query(sparql_query, data)
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to execute SPARQL query: {str(e)}"
            }

        # TODO #5 add salt for k-anonymity
        # TODO #6 create natural language result
        # For now, this returns the query results along with the generated query.
        return {
            "status": "success",
            "message": "Orchestrator processed the query and executed it on data",
            "details": {
                "question": question,
                "sparql_query": sparql_query,
                "query_results": query_results
            }
        }
