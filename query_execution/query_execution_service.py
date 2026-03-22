import json
import logging
from typing import Any, Dict, List, Union
from rdflib import Graph

logger = logging.getLogger(__name__)

class QueryExecutionService:
    """
    Service for executing SPARQL queries on Linked Data (JSON-LD).
    """

    def execute_sparql_query(self, query: str, data: Union[str, Dict, List]) -> List[Dict[str, Any]]:
        """
        Executes a SPARQL query on the provided JSON-LD data.

        Args:
            query (str): The SPARQL query string.
            data (Union[str, Dict, List]): Data in JSON-LD form (string or dictionary/list).

        Returns:
            List[Dict[str, Any]]: The results of the query execution.
        """
        try:
            # Initialize the RDF graph
            g = Graph()

            # Prepare the data for parsing
            if isinstance(data, (dict, list)):
                data_str = json.dumps(data)
            else:
                data_str = data

            # Parse the JSON-LD data into the graph
            # Note: format="json-ld" is supported if rdflib >= 6.0.0
            g.parse(data=data_str, format="json-ld")

            logger.info(f"Executing SPARQL query on graph with {len(g)} triples.")

            # Execute the query
            query_results = g.query(query)

            # Process and format results
            results = []
            for row in query_results:
                result_item = {}
                # row is a result binding, iterate through variable names
                for var in query_results.vars:
                    val = row[var]
                    # Preserve native Python types (int, float, datetime, etc.)
                    # so that downstream services like NoiseService can work
                    # with numeric values directly.
                    if val is not None:
                        try:
                            result_item[str(var)] = val.toPython()
                        except AttributeError:
                            result_item[str(var)] = str(val)
                    else:
                        result_item[str(var)] = None
                results.append(result_item)

            return results

        except Exception as e:
            logger.error(f"Error executing SPARQL query: {str(e)}", exc_info=True)
            raise e
