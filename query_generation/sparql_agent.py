"""
SPARQL Agent Configuration

This module contains the system prompt and configuration for the SPARQL query
generation agent.
"""

SPARQL_AGENT_SYSTEM_PROMPT = """
1. Role & Objective
You are an expert Semantic Web Engineer. Your sole task is to translate natural language questions into valid, optimized SPARQL 1.1 queries based on a provided ontology.
- OUTPUT: Return ONLY the raw SPARQL query. 
- FORMATTING: Do NOT wrap the query in markdown code blocks like ```sparql ... ```. Do NOT include any explanations, preambles, or post-scripts.
- Constraint: Do NOT execute the query.
- Constraint: Do NOT provide the answer to the question.

2. Knowledge Context (The Ontology)
Strictly adhere to the provided ontology (usually in JSON-LD):
- Prefixes: 
    - Always look for the `@base` and `@context` in the JSON-LD. Use the `@base` URI for the default namespace (e.g., `PREFIX oyd: <...>`).
    - Use standard prefixes (rdf:, rdfs:, owl:, xsd:).
- Classes & Properties: 
    - Use ONLY the URIs and IDs defined in the `@graph`.
    - Pay close attention to `domain` and `range`. If a property is defined for `Object1`, do not use it with `Object2`.
    - If Object1 is a subclass of something, respect that hierarchy.
- Closed World Assumption: If a class or property is not in the ontology, do NOT invent it. If you cannot answer the question with the given ontology, state exactly what is missing.

3. Structural Requirements
- Prefix Declarations: Include all necessary namespaces.
- Query Clauses: SELECT, WHERE, etc., must be syntactically correct for SPARQL 1.1.
- Filters: Use FILTER for constraints.
- Aggregation: When using AVG(...), always include a COUNT of the same grouping (e.g. COUNT(?x) AS ?count) in the SELECT clause. This is required for differential privacy noise calibration.
- DP Grouping (Bucketing): When grouping by a metric semi-sensitive attribute, you MUST apply bucketing unless the user explicitly requests a custom size. 
    1. Numeric attributes: calculate the exact bucket size as `(max_value - min_value) / number_buckets` based on the provided ontology constraints. Use standard syntax: `BIND(FLOOR(?attribute / bucket_size) AS ?bucket_attribute)` and group by that alias.
    2. Date attributes: use native SPARQL time grouping based on the provided `date_granularity`. E.g., for YEAR use `BIND(YEAR(?date) AS ?bucket_date)`. For DECADE use `BIND(FLOOR(YEAR(?date) / 10) AS ?bucket_date)`.

4. Example
If Ontology base is <https://example.org/> and includes Object1 with property gehalt:
Query:
PREFIX oyd: <https://example.org/>
SELECT (AVG(?gehalt) AS ?avg) (COUNT(?s) AS ?count) WHERE { ?s a oyd:Object1 ; oyd:gehalt ?gehalt . }
""".strip()


def get_sparql_agent_prompt() -> str:
    """
    Returns the system prompt for the SPARQL generation agent.
    
    Returns:
        str: The system prompt string.
    """
    return SPARQL_AGENT_SYSTEM_PROMPT


from models.ontology import Ontology

def format_user_message(ontology: Ontology, question: str) -> str:
    """
    Formats the user message containing the structured ontology and question.
    
    Args:
        ontology: The structured Ontology object.
        question: The user's natural language question.
    
    Returns:
        str: The formatted user message for the LLM.
    """
    
    # Build the "Hardened" Markdown representation
    ontology_md = f"### Namespace\n- **Prefix**: {ontology.prefix}\n- **Base URI**: {ontology.base_uri}\n\n### Knowledge Graph Schema\n"
    
    for obj in ontology.objects:
        ontology_md += f"#### Class: {ontology.prefix}:{obj.name}\n"
        
        datatype_attrs = []
        
        for attr in obj.attributes:
            extra_details = f"Sensitivity: {attr.sensitivity_level}"
            if attr.min_value is not None and attr.max_value is not None:
                extra_details += f", min_value: {attr.min_value}, max_value: {attr.max_value}"
            if attr.number_buckets is not None:
                extra_details += f", number_buckets: {attr.number_buckets}"
            if attr.date_granularity is not None:
                extra_details += f", date_granularity: {attr.date_granularity}"

            if attr.is_composite and attr.children:
                datatype_attrs.append(
                    f"  - {ontology.prefix}:{attr.name} "
                    f"(Type: {attr.attr_type}, "
                    f"Anonymization: {attr.anonymization_type}, "
                    f"{extra_details})"
                )
                for child in attr.children:
                    datatype_attrs.append(
                        f"    - {ontology.prefix}:{child.name} "
                        f"(Sensitivity: {child.sensitivity_level}, "
                        f"inherited from {attr.name})"
                    )
            else:
                datatype_attrs.append(
                    f"  - {ontology.prefix}:{attr.name} "
                    f"(Anonymization: {attr.anonymization_type}, "
                    f"{extra_details})"
                )
        
        if datatype_attrs:
            ontology_md += "- **Attributes (Datatype Properties)**:\n" + "\n".join(datatype_attrs) + "\n"
        
        ontology_md += "\n"

    return f"""## Ontology (Simplified Schema)

{ontology_md}

## Question

{question}

Please generate a SPARQL 1.1 query to answer the above question based on the provided ontology. 
Use the prefix '{ontology.prefix}:' for all classes and properties defined in the schema.
"""
