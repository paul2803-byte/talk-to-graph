import logging
from typing import Dict, Tuple, Set

from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.algebra import CompValue
from rdflib.term import Variable, URIRef

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueryEvaluationService:
    """
    Service for evaluating SPARQL queries against k-anonymity constraints.

    Performs static analysis of a generated SPARQL query to ensure it does not
    reveal sensitive information or create quasi-identifiers that could be used
    to re-identify individuals.
    """

    # Aggregate function names in the SPARQL algebra
    AGGREGATE_FUNCTIONS = {"Aggregate_Count", "Aggregate_Sum", "Aggregate_Avg",
                           "Aggregate_Min", "Aggregate_Max", "Aggregate_Sample",
                           "Aggregate_GroupConcat"}

    def evaluate_query(self, query: str, sensitivity_config: Dict[str, str]) -> Tuple[bool, str]:
        """
        Evaluates a SPARQL query and returns whether it is valid along with a result message.

        Args:
            query (str): The SPARQL query string to evaluate.
            sensitivity_config (Dict[str, str]): A dictionary mapping attribute names to their
                sensitivity level. Each value must be one of:
                - "sensitive"
                - "semi-sensitive"
                - "not-sensitive"

        Returns:
            Tuple[bool, str]: A tuple containing:
                - bool: Whether the query is valid/successful.
                - str: A result message describing the evaluation outcome.
        """
        # Classify attributes by sensitivity
        sensitive_attrs, semi_sensitive_attrs, not_sensitive_attrs = self._classify_attributes(
            sensitivity_config
        )

        # Parse and translate SPARQL to algebra tree
        try:
            parsed = parseQuery(query)
            algebra = translateQuery(parsed)
        except Exception as e:
            return False, f"Failed to parse SPARQL query: {str(e)}"

        root = algebra.algebra

        # Step 1: Map SPARQL variables to sensitivity levels via triple patterns
        var_sensitivity = self._map_variables_to_sensitivity(
            root, sensitive_attrs, semi_sensitive_attrs, not_sensitive_attrs
        )

        # Step 2: Collect variables used in FILTER expressions
        filter_vars = self._collect_filter_variables(root)

        # Step 3: Collect SELECT projection info (variable, is_aggregated)
        select_vars, aggregated_vars = self._collect_select_variables(root)

        # Step 4: Collect GROUP BY variables
        group_by_vars = self._collect_group_by_variables(root)

        # ── Rule R1: Sensitive attributes must not appear anywhere ──
        for var, level in var_sensitivity.items():
            if level == "sensitive":
                return False, (
                    f"Query rejected: sensitive attribute variable '?{var}' "
                    f"must not appear in the query."
                )

        # ── Rule R2: Semi-sensitive attributes must not appear in FILTER ──
        for var in filter_vars:
            var_name = str(var)
            if var_sensitivity.get(var_name) == "semi-sensitive":
                return False, (
                    f"Query rejected: semi-sensitive attribute variable '?{var_name}' "
                    f"must not appear in a FILTER expression."
                )

        # ── Rule R3: Semi-sensitive attributes in SELECT must be aggregated ──
        for var in select_vars:
            var_name = str(var)
            if var_sensitivity.get(var_name) == "semi-sensitive":
                if var not in aggregated_vars:
                    return False, (
                        f"Query rejected: semi-sensitive attribute variable '?{var_name}' "
                        f"in SELECT must be inside an aggregate function (COUNT, AVG, etc.)."
                    )

        # ── Rule R4: At most one semi-sensitive attribute in GROUP BY ──
        semi_sensitive_in_group = [
            var for var in group_by_vars
            if var_sensitivity.get(str(var)) == "semi-sensitive"
        ]
        if len(semi_sensitive_in_group) > 1:
            names = ", ".join(f"?{v}" for v in semi_sensitive_in_group)
            return False, (
                f"Query rejected: multiple semi-sensitive attributes ({names}) "
                f"in GROUP BY creates quasi-identifier risk."
            )

        logger.info("Query passed k-anonymity static analysis.")
        return True, "Query is k-anonymity compliant."

    # ──────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _classify_attributes(
        config: Dict[str, str]
    ) -> Tuple[Set[str], Set[str], Set[str]]:
        """Partition attribute names into sensitive / semi-sensitive / not-sensitive sets (lower-cased)."""
        sensitive: Set[str] = set()
        semi_sensitive: Set[str] = set()
        not_sensitive: Set[str] = set()
        for attr, level in config.items():
            name = attr.lower()
            if level == "sensitive":
                sensitive.add(name)
            elif level == "semi-sensitive":
                semi_sensitive.add(name)
            else:
                not_sensitive.add(name)
        return sensitive, semi_sensitive, not_sensitive

    @staticmethod
    def _local_name(uri: URIRef) -> str:
        """Extract the local name (fragment or last path segment) from a URI."""
        s = str(uri)
        if "#" in s:
            return s.rsplit("#", 1)[1]
        return s.rsplit("/", 1)[-1]

    def _map_variables_to_sensitivity(
        self,
        node,
        sensitive: Set[str],
        semi_sensitive: Set[str],
        not_sensitive: Set[str],
    ) -> Dict[str, str]:
        """
        Walk the algebra tree, find triple patterns of the form
            ?s <predicate> ?var
        and map ?var's name to a sensitivity level based on the predicate's local name.
        """
        mapping: Dict[str, str] = {}
        self._walk_triples(node, mapping, sensitive, semi_sensitive, not_sensitive)
        return mapping

    def _walk_triples(self, node, mapping, sensitive, semi_sensitive, not_sensitive):
        """Recursively walk the algebra tree looking for triple patterns."""
        if isinstance(node, CompValue):
            if node.name == "BGP":
                triples = node.get("triples", [])
                for triple in triples:
                    s, p, o = triple
                    if isinstance(p, URIRef):
                        local = self._local_name(p).lower()
                        if isinstance(o, Variable):
                            var_name = str(o)
                            if local in sensitive:
                                mapping[var_name] = "sensitive"
                            elif local in semi_sensitive:
                                mapping[var_name] = "semi-sensitive"
                            elif local in not_sensitive:
                                mapping[var_name] = "not-sensitive"
            # Recurse into all child CompValues
            for key in node.keys():
                child = node[key]
                if isinstance(child, CompValue):
                    self._walk_triples(child, mapping, sensitive, semi_sensitive, not_sensitive)
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, CompValue):
                            self._walk_triples(item, mapping, sensitive, semi_sensitive, not_sensitive)

    def _collect_filter_variables(self, node) -> Set[Variable]:
        """Collect all variables referenced in FILTER expressions."""
        variables: Set[Variable] = set()
        self._walk_filters(node, variables)
        return variables

    def _walk_filters(self, node, variables: Set[Variable]):
        """Recursively walk the tree to find Filter nodes and extract their variables."""
        if isinstance(node, CompValue):
            if node.name == "Filter":
                expr = node.get("expr")
                if expr is not None:
                    self._extract_variables_from_expr(expr, variables)
            # Continue recursion into all children
            for key in node.keys():
                child = node[key]
                if isinstance(child, CompValue):
                    self._walk_filters(child, variables)
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, CompValue):
                            self._walk_filters(item, variables)

    def _extract_variables_from_expr(self, expr, variables: Set[Variable]):
        """Extract all Variable references from an expression subtree."""
        if isinstance(expr, Variable):
            variables.add(expr)
        elif isinstance(expr, CompValue):
            for key in expr.keys():
                child = expr[key]
                self._extract_variables_from_expr(child, variables)
        elif isinstance(expr, list):
            for item in expr:
                self._extract_variables_from_expr(item, variables)

    def _collect_select_variables(self, node) -> Tuple[Set[Variable], Set[Variable]]:
        """
        Collect variables from the SELECT projection.

        Returns:
            Tuple of (all_select_vars, aggregated_vars).
            aggregated_vars are variables that appear inside an aggregate function.
        """
        all_vars: Set[Variable] = set()
        aggregated_vars: Set[Variable] = set()

        if isinstance(node, CompValue) and node.name == "Project":
            pv = node.get("PV", [])
            for v in pv:
                if isinstance(v, Variable):
                    all_vars.add(v)

            # Check for Extend nodes (aliases from aggregates like (AVG(?x) AS ?avg))
            inner = node.get("p")
            self._check_aggregation_in_extend(inner, all_vars, aggregated_vars)

        return all_vars, aggregated_vars

    def _check_aggregation_in_extend(self, node, all_vars, aggregated_vars):
        """Walk Extend/AggregateJoin nodes to find which variables are aggregated."""
        if not isinstance(node, CompValue):
            return

        if node.name == "Extend":
            var = node.get("var")
            expr = node.get("expr")
            if var is not None and expr is not None:
                if isinstance(var, Variable):
                    all_vars.add(var)
                # Check if the expression is an aggregate
                if self._is_aggregate_expr(expr):
                    # The alias variable is aggregated
                    if isinstance(var, Variable):
                        aggregated_vars.add(var)
                    # Also mark the inner variable of the aggregate as aggregated
                    inner_vars: Set[Variable] = set()
                    self._extract_variables_from_expr(expr, inner_vars)
                    aggregated_vars.update(inner_vars)
            # Recurse into inner node
            inner = node.get("p")
            self._check_aggregation_in_extend(inner, all_vars, aggregated_vars)

        elif node.name == "AggregateJoin":
            aggs = node.get("A", [])
            for agg in aggs:
                if isinstance(agg, CompValue) and agg.name in self.AGGREGATE_FUNCTIONS:
                    inner_vars_agg: Set[Variable] = set()
                    self._extract_variables_from_expr(agg, inner_vars_agg)
                    aggregated_vars.update(inner_vars_agg)
            inner = node.get("p")
            if inner:
                self._check_aggregation_in_extend(inner, all_vars, aggregated_vars)

        elif node.name == "Group":
            # Process inner parts of Group
            inner = node.get("p")
            if inner:
                self._check_aggregation_in_extend(inner, all_vars, aggregated_vars)
        else:
            # Generic recursion
            for key in node.keys():
                child = node[key]
                if isinstance(child, CompValue):
                    self._check_aggregation_in_extend(child, all_vars, aggregated_vars)

    def _is_aggregate_expr(self, expr) -> bool:
        """Check if an expression node is an aggregate function."""
        if isinstance(expr, CompValue):
            if expr.name in self.AGGREGATE_FUNCTIONS:
                return True
            # Check children
            for key in expr.keys():
                if self._is_aggregate_expr(expr[key]):
                    return True
        return False

    def _collect_group_by_variables(self, node) -> Set[Variable]:
        """Collect variables used in GROUP BY clauses."""
        group_vars: Set[Variable] = set()
        self._walk_group_by(node, group_vars)
        return group_vars

    def _walk_group_by(self, node, group_vars: Set[Variable]):
        """Recursively walk the tree to find Group nodes and extract their variables."""
        if isinstance(node, CompValue):
            if node.name == "Group":
                expr = node.get("expr") or []
                for item in expr:
                    if isinstance(item, Variable):
                        group_vars.add(item)
                    elif isinstance(item, CompValue):
                        inner_vars: Set[Variable] = set()
                        self._extract_variables_from_expr(item, inner_vars)
                        group_vars.update(inner_vars)
            for key in node.keys():
                child = node[key]
                if isinstance(child, CompValue):
                    self._walk_group_by(child, group_vars)
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, CompValue):
                            self._walk_group_by(item, group_vars)
