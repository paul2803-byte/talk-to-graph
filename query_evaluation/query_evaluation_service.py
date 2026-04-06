import logging
from typing import Any, Dict, List, Optional, Tuple, Set

from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.algebra import CompValue
from rdflib.term import Variable, URIRef, Literal

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

    # Aggregates that are blocked unconditionally on semi-sensitive attributes
    _BLOCKED_AGGREGATES = {"Aggregate_Min", "Aggregate_Max", "Aggregate_Sample", "Aggregate_GroupConcat"}

    # Aggregates that require ontology bounds (SUM, AVG) — COUNT is exempt
    _BOUNDS_REQUIRED_AGGREGATES = {"Aggregate_Sum", "Aggregate_Avg"}

    # Mapping from algebra names to the short names used in aggregate_info
    _AGG_SHORT_NAMES = {
        "Aggregate_Count": "count",
        "Aggregate_Sum": "sum",
        "Aggregate_Avg": "avg",
    }

    def evaluate_query(
        self,
        query: str,
        sensitivity_config: Dict[str, str],
        sensitivity_bounds: Optional[Dict[str, Tuple[Optional[float], Optional[float]]]] = None,
        max_semi_sensitive_group_by: int = 1,
    ) -> Tuple[bool, str, List[Dict[str, Any]]]:
        """
        Evaluates a SPARQL query and returns whether it is valid along with
        a result message and aggregate metadata.

        Args:
            query: The SPARQL query string to evaluate.
            sensitivity_config: ``{attribute_name: sensitivity_level}``.
                Each value must be one of "sensitive", "semi-sensitive",
                or "not-sensitive".
            sensitivity_bounds: ``{attribute_name: (min_value, max_value)}``.
                Required by Rule R6 for SUM/AVG on semi-sensitive attributes.

        Returns:
            Tuple of (is_valid, message, aggregate_info).
            aggregate_info is a list of dicts with keys:
                - "variable": projected variable name
                - "function": one of "count", "sum", "avg"
                - "attribute": ontology attribute name (or None for COUNT(*))
        """
        if sensitivity_bounds is None:
            sensitivity_bounds = {}

        # Classify attributes by sensitivity
        sensitive_attrs, semi_sensitive_attrs, not_sensitive_attrs = self._classify_attributes(
            sensitivity_config
        )

        # Parse and translate SPARQL to algebra tree
        try:
            parsed = parseQuery(query)
            algebra = translateQuery(parsed)
        except Exception as e:
            return False, f"Failed to parse SPARQL query: {str(e)}", []

        root = algebra.algebra

        # Step 1: Map SPARQL variables to sensitivity levels via triple patterns
        var_sensitivity, var_to_attr = self._map_variables_to_sensitivity(
            root, sensitive_attrs, semi_sensitive_attrs, not_sensitive_attrs
        )

        # Step 2: Collect variables used in FILTER expressions
        filter_vars = self._collect_filter_variables(root)

        # Step 3: Collect SELECT projection info (variable, is_aggregated)
        select_vars, aggregated_vars = self._collect_select_variables(root)

        # Step 4: Collect GROUP BY variables
        group_by_vars = self._collect_group_by_variables(root)

        # Step 5: Collect detailed aggregate info (function + inner attribute)
        aggregate_details = self._collect_aggregate_details(root)

        # ── Rule R7: Block concrete-subject access to sensitive/semi-sensitive ──
        concrete_violations = self._detect_concrete_subject_access(
            root, sensitive_attrs, semi_sensitive_attrs
        )
        if concrete_violations:
            pred_name, level = concrete_violations[0]
            return False, (
                f"Query rejected: a specific individual is targeted via a concrete "
                f"subject URI accessing {level} predicate '{pred_name}' (Rule R7)."
            ), []

        # ── Rule R1: Sensitive attributes must not appear anywhere ──
        for var, level in var_sensitivity.items():
            if level == "sensitive":
                return False, (
                    f"Query rejected: sensitive attribute variable '?{var}' "
                    f"must not appear in the query."
                ), []

        # ── Rule R2: Semi-sensitive attributes must not appear in FILTER ──
        for var in filter_vars:
            var_name = str(var)
            if var_sensitivity.get(var_name) == "semi-sensitive":
                return False, (
                    f"Query rejected: semi-sensitive attribute variable '?{var_name}' "
                    f"must not appear in a FILTER expression."
                ), []

        # ── Rule R2b: Semi-sensitive predicates must not be constrained
        #    by concrete literal values in WHERE patterns ──
        literal_violations = self._detect_literal_constraints_on_semi_sensitive(
            root, semi_sensitive_attrs
        )
        if literal_violations:
            pred_name = literal_violations[0]
            return False, (
                f"Query rejected: semi-sensitive predicate '{pred_name}' is "
                f"constrained by a concrete value in the WHERE clause. "
                f"This is equivalent to a FILTER on semi-sensitive data (Rule R2)."
            ), []

        # ── Rule R3: Semi-sensitive attributes in SELECT must be aggregated ──
        for var in select_vars:
            var_name = str(var)
            if var_sensitivity.get(var_name) == "semi-sensitive":
                if var not in aggregated_vars:
                    return False, (
                        f"Query rejected: semi-sensitive attribute variable '?{var_name}' "
                        f"in SELECT must be inside an aggregate function (COUNT, AVG, etc.)."
                    ), []

        # ── Rule R4: At most one semi-sensitive attribute in GROUP BY ──
        semi_sensitive_in_group = [
            var for var in group_by_vars
            if var_sensitivity.get(str(var)) == "semi-sensitive"
        ]
        if len(semi_sensitive_in_group) > max_semi_sensitive_group_by:
            names = ", ".join(f"?{v}" for v in semi_sensitive_in_group)
            return False, (
                f"Query rejected: too many semi-sensitive attributes ({names}) "
                f"in GROUP BY (max {max_semi_sensitive_group_by}) creates quasi-identifier risk."
            ), []

        # ── Rule R5: Block MIN/MAX/Sample/GroupConcat on semi-sensitive ──
        for detail in aggregate_details:
            inner_attr = detail.get("inner_attribute")
            agg_func = detail["agg_function"]
            if inner_attr and var_sensitivity.get(inner_attr) == "semi-sensitive":
                if agg_func in self._BLOCKED_AGGREGATES:
                    return False, (
                        f"Query rejected: {agg_func} on semi-sensitive attribute "
                        f"'?{inner_attr}' is not allowed (Rule R5)."
                    ), []

        # ── Rule R6: SUM/AVG on semi-sensitive attrs require ontology bounds ──
        for detail in aggregate_details:
            inner_attr = detail.get("inner_attribute")
            agg_func = detail["agg_function"]
            if inner_attr and var_sensitivity.get(inner_attr) == "semi-sensitive":
                if agg_func in self._BOUNDS_REQUIRED_AGGREGATES:
                    # Resolve SPARQL variable to ontology attribute name
                    onto_attr = var_to_attr.get(inner_attr, inner_attr)
                    onto_attr_lower = onto_attr.lower()
                    bounds = sensitivity_bounds.get(onto_attr) or sensitivity_bounds.get(onto_attr_lower)
                    if bounds is None or bounds[0] is None or bounds[1] is None:
                        return False, (
                            f"Query rejected: {agg_func} on semi-sensitive attribute "
                            f"'?{inner_attr}' requires min/max bounds in the ontology (Rule R6)."
                        ), []

        # ── Rule R8: AVG/SUM on semi-sensitive requires COUNT in projection ──
        has_semi_sensitive_agg = False
        has_count = False
        for detail in aggregate_details:
            inner_attr = detail.get("inner_attribute")
            agg_func = detail["agg_function"]
            if agg_func == "Aggregate_Count":
                has_count = True
            if inner_attr and var_sensitivity.get(inner_attr) == "semi-sensitive":
                if agg_func in self._BOUNDS_REQUIRED_AGGREGATES:
                    has_semi_sensitive_agg = True
        if has_semi_sensitive_agg and not has_count:
            return False, (
                "Query rejected: queries using AVG or SUM on semi-sensitive "
                "attributes must also include a COUNT aggregate to enable "
                "small-group suppression (Rule R8)."
            ), []

        # Build aggregate_info list for downstream NoiseService
        aggregate_info: List[Dict[str, Any]] = []
        for detail in aggregate_details:
            short_name = self._AGG_SHORT_NAMES.get(detail["agg_function"])
            if short_name is not None:
                # Resolve SPARQL variable name → ontology attribute name
                # so downstream services (e.g. NoiseService) can look up
                # bounds by the canonical attribute name, not an LLM-chosen
                # abbreviation like ?g for gehalt.
                raw_attr = detail.get("inner_attribute")
                resolved_attr = var_to_attr.get(raw_attr, raw_attr) if raw_attr else None
                aggregate_info.append({
                    "variable": detail["alias_variable"],
                    "function": short_name,
                    "attribute": resolved_attr,
                })

        logger.info("Query passed static analysis (R1-R8).")
        return True, "Query is compliant.", aggregate_info

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
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        """
        Walk the algebra tree, find triple patterns of the form
            ?s <predicate> ?var
        and map ?var's name to a sensitivity level based on the predicate's local name.

        Returns:
            Tuple of (var_sensitivity, var_to_attr_name).
            var_to_attr_name maps SPARQL variable names to ontology attribute names.
        """
        mapping: Dict[str, str] = {}
        var_to_attr: Dict[str, str] = {}
        self._walk_triples(node, mapping, var_to_attr, sensitive, semi_sensitive, not_sensitive)
        return mapping, var_to_attr

    def _walk_triples(self, node, mapping, var_to_attr, sensitive, semi_sensitive, not_sensitive):
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
                            var_to_attr[var_name] = self._local_name(p)
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
                    self._walk_triples(child, mapping, var_to_attr, sensitive, semi_sensitive, not_sensitive)
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, CompValue):
                            self._walk_triples(item, mapping, var_to_attr, sensitive, semi_sensitive, not_sensitive)

    def _detect_concrete_subject_access(
        self, node, sensitive: Set[str], semi_sensitive: Set[str]
    ) -> List[Tuple[str, str]]:
        """Detect triple patterns where a concrete subject URI accesses a
        sensitive or semi-sensitive predicate (Rule R7).

        Returns a list of (predicate_local_name, sensitivity_level) tuples.
        """
        violations: List[Tuple[str, str]] = []
        self._walk_concrete_subjects(node, sensitive, semi_sensitive, violations)
        return violations

    def _walk_concrete_subjects(self, node, sensitive, semi_sensitive, violations):
        """Walk BGP triples looking for concrete (URIRef) subjects paired with
        sensitive/semi-sensitive predicates."""
        if isinstance(node, CompValue):
            if node.name == "BGP":
                for triple in node.get("triples", []):
                    s, p, o = triple
                    if isinstance(s, URIRef) and isinstance(p, URIRef):
                        local = self._local_name(p).lower()
                        if local in sensitive:
                            violations.append((local, "sensitive"))
                        elif local in semi_sensitive:
                            violations.append((local, "semi-sensitive"))
            for key in node.keys():
                child = node[key]
                if isinstance(child, CompValue):
                    self._walk_concrete_subjects(child, sensitive, semi_sensitive, violations)
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, CompValue):
                            self._walk_concrete_subjects(child, sensitive, semi_sensitive, violations)

    def _detect_literal_constraints_on_semi_sensitive(
        self, node, semi_sensitive: Set[str]
    ) -> List[str]:
        """Detect triple patterns where a semi-sensitive predicate has a concrete
        literal or URI value as the object (Rule R2 enhancement).

        A pattern like ``?s oyd:alter 30`` constrains semi-sensitive data the
        same way as ``FILTER(?alter = 30)`` and must be blocked.

        Returns a list of predicate local names that are violated.
        """
        violations: List[str] = []
        self._walk_literal_constraints(node, semi_sensitive, violations)
        return violations

    def _walk_literal_constraints(self, node, semi_sensitive, violations):
        """Walk BGP triples looking for concrete (Literal/URIRef) objects on
        semi-sensitive predicates."""
        if isinstance(node, CompValue):
            if node.name == "BGP":
                for triple in node.get("triples", []):
                    s, p, o = triple
                    if isinstance(p, URIRef):
                        local = self._local_name(p).lower()
                        if local in semi_sensitive:
                            # Object is a concrete value (not a variable)
                            if not isinstance(o, Variable):
                                violations.append(local)
            for key in node.keys():
                child = node[key]
                if isinstance(child, CompValue):
                    self._walk_literal_constraints(child, semi_sensitive, violations)
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, CompValue):
                            self._walk_literal_constraints(item, semi_sensitive, violations)

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

        Recursively walks the algebra tree to find the Project node,
        since the root may be a wrapper like SelectQuery.

        Returns:
            Tuple of (all_select_vars, aggregated_vars).
            aggregated_vars are variables that appear inside an aggregate function.
        """
        all_vars: Set[Variable] = set()
        aggregated_vars: Set[Variable] = set()
        self._walk_select_variables(node, all_vars, aggregated_vars)
        return all_vars, aggregated_vars

    def _walk_select_variables(self, node, all_vars: Set[Variable], aggregated_vars: Set[Variable]):
        """Recursively walk the tree to find the Project node and extract projected variables."""
        if not isinstance(node, CompValue):
            return

        if node.name == "Project":
            pv = node.get("PV", [])
            for v in pv:
                if isinstance(v, Variable):
                    all_vars.add(v)

            # Check for Extend nodes (aliases from aggregates like (AVG(?x) AS ?avg))
            inner = node.get("p")
            self._check_aggregation_in_extend(inner, all_vars, aggregated_vars)
            return

        # Recurse into children to find the Project node
        for key in node.keys():
            child = node[key]
            if isinstance(child, CompValue):
                self._walk_select_variables(child, all_vars, aggregated_vars)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, CompValue):
                        self._walk_select_variables(item, all_vars, aggregated_vars)

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

    # ──────────────────────────────────────────────────────────────────────
    # Aggregate detail extraction (for Rules R5/R6 + downstream metadata)
    # ──────────────────────────────────────────────────────────────────────

    def _collect_aggregate_details(self, root) -> List[Dict[str, Any]]:
        """Return a list of dicts describing each aggregate in the query.

        Each dict has:
            - ``agg_function``: algebra name (e.g. ``"Aggregate_Avg"``)
            - ``alias_variable``: the projected alias variable name (str)
            - ``inner_attribute``: the SPARQL variable inside the aggregate (str or None)

        The rdflib algebra structure is:
            Extend(var=?alias, expr=Variable('__agg_N__'))
              └─ AggregateJoin(A=[Aggregate_X(vars=?attr, res=?__agg_N__)])

        So we first collect aggregate info from AggregateJoin keyed by the
        internal ``res`` variable, then walk Extend nodes to map each
        user-facing alias to the corresponding aggregate.
        """
        # Pass 1: collect aggregates from AggregateJoin → {res_var: info}
        agg_map: Dict[str, Dict[str, Any]] = {}
        self._collect_agg_join_info(root, agg_map)

        # Pass 2: walk Extend nodes to link alias variables
        details: List[Dict[str, Any]] = []
        self._link_extend_aliases(root, agg_map, details)

        # If there are unlinked aggregates (no Extend above them), add them directly
        linked_res_vars = {d.get("_res") for d in details}
        for res_var, info in agg_map.items():
            if res_var not in linked_res_vars:
                details.append(info)

        # Remove internal _res key from output
        for d in details:
            d.pop("_res", None)

        return details

    def _collect_agg_join_info(self, node, agg_map: Dict[str, Dict[str, Any]]):
        """Walk the tree to find AggregateJoin nodes and collect aggregate info."""
        if not isinstance(node, CompValue):
            return

        if node.name == "AggregateJoin":
            aggs = node.get("A", [])
            for agg in aggs:
                if isinstance(agg, CompValue) and agg.name in self.AGGREGATE_FUNCTIONS:
                    # Get the inner variable (the attribute being aggregated)
                    vars_val = agg.get("vars")
                    inner_attr = str(vars_val) if isinstance(vars_val, Variable) else None

                    # Get the result variable (internal __agg_N__ reference)
                    res_val = agg.get("res")
                    res_var = str(res_val) if isinstance(res_val, Variable) else None

                    info = {
                        "agg_function": agg.name,
                        "alias_variable": inner_attr,  # default, overwritten by Extend
                        "inner_attribute": inner_attr,
                    }
                    if res_var:
                        agg_map[res_var] = info

        for key in node.keys():
            child = node[key]
            if isinstance(child, CompValue):
                self._collect_agg_join_info(child, agg_map)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, CompValue):
                        self._collect_agg_join_info(item, agg_map)

    def _link_extend_aliases(self, node, agg_map, details):
        """Walk Extend nodes and link user-facing aliases to aggregate info."""
        if not isinstance(node, CompValue):
            return

        if node.name == "Extend":
            alias_var = node.get("var")
            expr = node.get("expr")
            alias_name = str(alias_var) if isinstance(alias_var, Variable) else None

            # The expr is often just Variable('__agg_N__')
            if isinstance(expr, Variable):
                ref_name = str(expr)
                if ref_name in agg_map:
                    entry = dict(agg_map[ref_name])
                    entry["alias_variable"] = alias_name or entry["alias_variable"]
                    entry["_res"] = ref_name
                    details.append(entry)

        # Recurse into all children
        for key in node.keys():
            child = node[key]
            if isinstance(child, CompValue):
                self._link_extend_aliases(child, agg_map, details)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, CompValue):
                        self._link_extend_aliases(item, agg_map, details)


