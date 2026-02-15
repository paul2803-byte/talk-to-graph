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

4. Example
If Ontology base is <https://example.org/> and includes Object1 with property gehalt:
Query:
PREFIX oyd: <https://example.org/>
SELECT (AVG(?g) AS ?avg) WHERE { ?s a oyd:Object1 ; oyd:gehalt ?g . }
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
        
        datatybe_attrs = []
        object_attrs = []
        
        for attr in obj.attributes:
            # Simple heuristic for type mapping
            if attr.type.lower() in ['string', 'integer', 'date', 'number', 'boolean']:
                xsd_type = f"xsd:{attr.type.lower()}"
                if attr.type.lower() == 'number': xsd_type = "xsd:decimal"
                datatybe_attrs.append(f"  - {ontology.prefix}:{attr.name} (Type: {xsd_type})")
            else:
                object_attrs.append(f"  - {ontology.prefix}:{attr.name} (Target: {ontology.prefix}:{attr.type})")
        
        if datatybe_attrs:
            ontology_md += "- **Attributes (Datatype Properties)**:\n" + "\n".join(datatybe_attrs) + "\n"
        if object_attrs:
            ontology_md += "- **Relationships (Object Properties)**:\n" + "\n".join(object_attrs) + "\n"
        
        ontology_md += "\n"

    return f"""## Ontology (Simplified Schema)

{ontology_md}

## Question

{question}

Please generate a SPARQL 1.1 query to answer the above question based on the provided ontology. 
Use the prefix '{ontology.prefix}:' for all classes and properties defined in the schema.
"""
