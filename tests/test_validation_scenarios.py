"""
Validation scenario tests for the AnonymisationDemo ontology.

Builds the sensitivity config and bounds from the YAML definition inline,
then runs each SPARQL query through QueryEvaluationService.evaluate_query()
and asserts whether it should be allowed or rejected.

Run:
    python -m pytest tests/test_validation_scenarios.py -v --tb=short
"""
import sys
import textwrap
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pytest

from models.attribute_config import AttributeConfig
from query_evaluation.query_evaluation_service import QueryEvaluationService


# ── Ontology-derived configuration ──────────────────────────────────────
# Based on the AnonymisationDemo YAML with manually set min/max bounds.

NAMESPACE = "https://soya.ownyourdata.eu/AnonymisationDemo/"

ATTRIBUTE_CONFIGS: Dict[str, AttributeConfig] = {
    "name": AttributeConfig("sensitive"),
    "adresse": AttributeConfig("semi-sensitive"),
    "geburtsdatum": AttributeConfig("semi-sensitive", date_granularity="DECADE"),
    "gehalt": AttributeConfig("semi-sensitive", bounds=(0.0, 200000.0), number_buckets=20),
    "latitude": AttributeConfig("semi-sensitive", bounds=(-90.0, 90.0)),
    "longitude": AttributeConfig("semi-sensitive", bounds=(-180.0, 180.0)),
    "gewicht": AttributeConfig("semi-sensitive", bounds=(40.0, 150.0), number_buckets=5),
    "koerpergroesse": AttributeConfig("semi-sensitive", bounds=(140.0, 220.0), number_buckets=5),
    "start_pv": AttributeConfig("semi-sensitive"),
    "detail": AttributeConfig("semi-sensitive"),
    "city": AttributeConfig("semi-sensitive"),
    "zip": AttributeConfig("semi-sensitive"),
    "state": AttributeConfig("semi-sensitive"),
    "country": AttributeConfig("semi-sensitive"),
}


# ── Helper ──────────────────────────────────────────────────────────────

@dataclass
class ValidationScenario:
    """One validation test case."""
    id: str
    natural_language: str
    sparql: str
    expected_allowed: bool
    note: str = ""


def run_validation(
    scenarios: List[ValidationScenario],
    attribute_configs: Dict[str, AttributeConfig] = ATTRIBUTE_CONFIGS,
) -> List[dict]:
    """Run each scenario through QueryEvaluationService and return results."""
    service = QueryEvaluationService()
    results = []
    for sc in scenarios:
        is_valid, message, agg_info = service.evaluate_query(
            sc.sparql,
            attribute_configs,
            max_semi_sensitive_group_by=1,
        )
        results.append({
            "id": sc.id,
            "question": sc.natural_language,
            "sparql": textwrap.dedent(sc.sparql).strip(),
            "expected_allowed": sc.expected_allowed,
            "actual_allowed": is_valid,
            "pass": is_valid == sc.expected_allowed,
            "message": message,
            "aggregate_info": agg_info,
            "note": sc.note,
        })
    return results


def print_report(results: List[dict]) -> None:
    """Print a human-readable validation report."""
    import io, sys
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("\n" + "=" * 90)
    print("  QUERY VALIDATION REPORT -- AnonymisationDemo Ontology")
    print("=" * 90)
    passed = sum(1 for r in results if r["pass"])
    total = len(results)
    for r in results:
        status = "[PASS]" if r["pass"] else "[FAIL]"
        expected = "ALLOWED" if r["expected_allowed"] else "REJECTED"
        actual   = "ALLOWED" if r["actual_allowed"]   else "REJECTED"
        print(f"\n-- {r['id']} -- {status}")
        print(f"  Question : {r['question']}")
        print(f"  Expected : {expected}")
        print(f"  Actual   : {actual}")
        print(f"  Message  : {r['message']}")
        if r["note"]:
            print(f"  Note     : {r['note']}")
        print(f"  SPARQL   :")
        for line in r["sparql"].splitlines():
            print(f"    {line}")
    print("\n" + "-" * 90)
    print(f"  SUMMARY: {passed}/{total} scenarios matched expectations")
    print("-" * 90 + "\n")


# ── Scenarios ───────────────────────────────────────────────────────────

# --- User-provided scenarios ---

USER_SCENARIOS = [
    ValidationScenario(
        id="U1",
        natural_language="What is the average gehalt?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt .
            }}
        """,
        expected_allowed=True,
        note="Simple aggregate with COUNT → allowed.",
    ),
    ValidationScenario(
        id="U2",
        natural_language="Who has the highest gehalt?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?name ?gehalt
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:name ?name ;
                   oyd:gehalt ?gehalt .
            }}
            ORDER BY DESC(?gehalt)
            LIMIT 1
        """,
        expected_allowed=False,
        note="Accesses sensitive attribute 'name' → blocked by R1.",
    ),
    ValidationScenario(
        id="U3",
        natural_language="What is the average gehalt per geburtsdatum (in decade steps)?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            SELECT ?decade (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt ;
                   oyd:geburtsdatum ?geburtsdatum .
                BIND(FLOOR(YEAR(?geburtsdatum) / 10) AS ?decade)
            }}
            GROUP BY ?decade
        """,
        expected_allowed=True,
        note=(
            "GROUP BY on ?decade derived from ?geburtsdatum natively with count. "
            "Satisfies Rule R9 for date_granularity DECADE. "
            "Now ALLOWED — GROUP BY keys are controlled by R4/R9."
        ),
    ),
    ValidationScenario(
        id="U4",
        natural_language="What are the gehalt values?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?gehalt
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt .
            }}
        """,
        expected_allowed=False,
        note="Semi-sensitive attribute in SELECT without aggregation → R3.",
    ),
    ValidationScenario(
        id="U5",
        natural_language="What is the average gehalt per country?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?country (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt ;
                   oyd:adresse ?addr .
                ?addr oyd:country ?country .
            }}
            GROUP BY ?country
        """,
        expected_allowed=True,
        note=(
            "GROUP BY country (semi-sensitive, inherited from adresse) + AVG on gehalt "
            "(semi-sensitive) with COUNT. 1 semi-sensitive in GROUP BY is within R4 limit → allowed."
        ),
    ),
    ValidationScenario(
        id="U6",
        natural_language="What is the most common name?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?name (COUNT(?name) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:name ?name .
            }}
            GROUP BY ?name
            ORDER BY DESC(?cnt)
            LIMIT 1
        """,
        expected_allowed=False,
        note="Accesses sensitive attribute 'name' → blocked by R1.",
    ),
    ValidationScenario(
        id="U7",
        natural_language="What are the street values?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?detail
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:adresse ?addr .
                ?addr oyd:detail ?detail .
            }}
        """,
        expected_allowed=False,
        note="'detail' (street) inherits semi-sensitive from 'adresse' → R3 (raw SELECT without aggregation).",
    ),
]

# --- Additional generated scenarios ---

ADDITIONAL_SCENARIOS = [
    # ── Should be ALLOWED ──
    ValidationScenario(
        id="A1",
        natural_language="How many records are there in total?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT (COUNT(?s) AS ?total)
            WHERE {{
                ?s a oyd:AnonymisationDemo .
            }}
        """,
        expected_allowed=True,
        note="Simple count, no sensitive access.",
    ),
    ValidationScenario(
        id="A2",
        natural_language="What is the average gewicht?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT (AVG(?gewicht) AS ?avg_gewicht) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gewicht ?gewicht .
            }}
        """,
        expected_allowed=True,
        note="AVG on semi-sensitive with bounds + COUNT → allowed.",
    ),
    ValidationScenario(
        id="A3",
        natural_language="What is the average koerpergroesse per city?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?city (AVG(?kg) AS ?avg_kg) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:koerpergroesse ?kg ;
                   oyd:adresse ?addr .
                ?addr oyd:city ?city .
            }}
            GROUP BY ?city
        """,
        expected_allowed=True,
        note="GROUP BY city (semi-sensitive, inherited from adresse) + AVG on semi-sensitive with COUNT. 1 semi-sensitive in GROUP BY → allowed.",
    ),
    ValidationScenario(
        id="A4",
        natural_language="How many people live in each country?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?country (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:adresse ?addr .
                ?addr oyd:country ?country .
            }}
            GROUP BY ?country
        """,
        expected_allowed=True,
        note="COUNT grouped by country (semi-sensitive, inherited from adresse). 1 semi-sensitive in GROUP BY → allowed.",
    ),
    ValidationScenario(
        id="A5",
        natural_language="What is the total gehalt per state?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?state (SUM(?gehalt) AS ?total_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt ;
                   oyd:adresse ?addr .
                ?addr oyd:state ?state .
            }}
            GROUP BY ?state
        """,
        expected_allowed=True,
        note="SUM on semi-sensitive with bounds + COUNT, GROUP BY state (semi-sensitive, inherited from adresse). 1 semi-sensitive in GROUP BY → allowed.",
    ),
    ValidationScenario(
        id="A6",
        natural_language="What is the total gehalt per 10k bucket?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?bucket_gehalt (SUM(?gehalt) AS ?total_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt .
                BIND(FLOOR(?gehalt / 10000.0) AS ?bucket_gehalt)
            }}
            GROUP BY ?bucket_gehalt
        """,
        expected_allowed=True,
        note="Valid DP bucketing for numeric attribute gehalt (max=200000, min=0, 20 buckets) -> size 10000. Rule R9 accepts.",
    ),

    # ── Should be REJECTED ──
    ValidationScenario(
        id="R1",
        natural_language="List all names and their salaries.",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?name ?gehalt
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:name ?name ;
                   oyd:gehalt ?gehalt .
            }}
        """,
        expected_allowed=False,
        note="Sensitive attribute 'name' in SELECT → blocked by R1.",
    ),
    ValidationScenario(
        id="R2",
        natural_language="What is the average gehalt for people born on 1990-01-01?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            SELECT (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt ;
                   oyd:geburtsdatum "1990-01-01"^^xsd:date .
            }}
        """,
        expected_allowed=False,
        note=(
            "Concrete literal on semi-sensitive predicate 'geburtsdatum' → R2b."
        ),
    ),
    ValidationScenario(
        id="R3",
        natural_language="Show everyone's gewicht.",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?gewicht
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gewicht ?gewicht .
            }}
        """,
        expected_allowed=False,
        note="Semi-sensitive in SELECT without aggregation → R3.",
    ),
    ValidationScenario(
        id="R4",
        natural_language="What is the average gehalt grouped by geburtsdatum AND koerpergroesse?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?geburtsdatum ?koerpergroesse (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt ;
                   oyd:geburtsdatum ?geburtsdatum ;
                   oyd:koerpergroesse ?koerpergroesse .
            }}
            GROUP BY ?geburtsdatum ?koerpergroesse
        """,
        expected_allowed=False,
        note=(
            "Two semi-sensitive attrs in GROUP BY creates quasi-identifier risk → R4. "
            "Also blocked by R3 (raw semi-sensitive in SELECT)."
        ),
    ),
    ValidationScenario(
        id="R5",
        natural_language="What is the maximum gehalt?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT (MAX(?gehalt) AS ?max_gehalt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt .
            }}
        """,
        expected_allowed=False,
        note="MAX on semi-sensitive → R5.",
    ),
    ValidationScenario(
        id="R6",
        natural_language="What is the average gehalt for people with gehalt above 50000?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt .
                FILTER(?gehalt > 50000)
            }}
        """,
        expected_allowed=False,
        note="FILTER on semi-sensitive attribute 'gehalt' → R2.",
    ),
    ValidationScenario(
        id="R7",
        natural_language="What is person1's gehalt?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?gehalt
            WHERE {{
                oyd:person1 oyd:gehalt ?gehalt .
            }}
        """,
        expected_allowed=False,
        note="Concrete subject URI accessing semi-sensitive predicate → R7.",
    ),
    ValidationScenario(
        id="R8",
        natural_language="What is the average koerpergroesse? (no COUNT)",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT (AVG(?kg) AS ?avg_kg)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:koerpergroesse ?kg .
            }}
        """,
        expected_allowed=False,
        note="AVG on semi-sensitive without COUNT → R8.",
    ),
    ValidationScenario(
        id="R9",
        natural_language="What is the total gehalt per 500 bucket?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?bucket_gehalt (SUM(?gehalt) AS ?total_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt .
                BIND(FLOOR(?gehalt / 500) AS ?bucket_gehalt)
            }}
            GROUP BY ?bucket_gehalt
        """,
        expected_allowed=False,
        note="Invalid DP bucketing size for gehalt (expected 10000). Blocked by Rule R9.",
    ),
    ValidationScenario(
        id="R10",
        natural_language="What is the total gehalt grouped exactly by gehalt?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?gehalt (SUM(?gehalt) AS ?total_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt .
            }}
            GROUP BY ?gehalt
        """,
        expected_allowed=False,
        note="Raw grouping of metric variable that requires bucketing. Blocked by Rule R9.",
    ),
    ValidationScenario(
        id="R11",
        natural_language="What is the average koerpergroesse per gewicht?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT (AVG(?koerpergroesse) AS ?avg_koerpergroesse) (COUNT(?s) AS ?count) ?gewicht
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:koerpergroesse ?koerpergroesse ;
                   oyd:gewicht ?gewicht .
            }}
            GROUP BY ?gewicht
            ORDER BY ?gewicht
        """,
        expected_allowed=False,
        note=(
            "Raw GROUP BY on semi-sensitive metric attribute 'gewicht' without bucketing. "
            "Must use BIND(FLOOR(?gewicht / bucket_size) AS ?bucket_gewicht). Blocked by R9."
        ),
    ),

    # ── Composite sensitivity inheritance scenarios ──
    ValidationScenario(
        id="C1",
        natural_language="What is the average gehalt grouped by country AND city?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?country ?city (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt ;
                   oyd:adresse ?addr .
                ?addr oyd:country ?country ;
                      oyd:city ?city .
            }}
            GROUP BY ?country ?city
        """,
        expected_allowed=False,
        note=(
            "Two semi-sensitive attrs (country, city — both inherited from adresse) "
            "in GROUP BY → R4 quasi-identifier risk. "
            "Also blocked by R3 (raw semi-sensitive in SELECT)."
        ),
    ),
    ValidationScenario(
        id="C2",
        natural_language="What is the average gehalt per zip code?",
        sparql=f"""
            PREFIX oyd: <{NAMESPACE}>
            SELECT ?zip (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
            WHERE {{
                ?s a oyd:AnonymisationDemo ;
                   oyd:gehalt ?gehalt ;
                   oyd:adresse ?addr .
                ?addr oyd:zip ?zip .
            }}
            GROUP BY ?zip
        """,
        expected_allowed=True,
        note=(
            "GROUP BY zip (semi-sensitive, inherited from adresse) + AVG on gehalt "
            "(semi-sensitive) with COUNT. 1 semi-sensitive in GROUP BY → allowed."
        ),
    ),
]

ALL_SCENARIOS = USER_SCENARIOS + ADDITIONAL_SCENARIOS


# ── Pytest parametrised tests ───────────────────────────────────────────

@pytest.fixture
def service():
    return QueryEvaluationService()


@pytest.mark.parametrize(
    "scenario",
    ALL_SCENARIOS,
    ids=[s.id for s in ALL_SCENARIOS],
)
def test_validation_scenario(service, scenario: ValidationScenario):
    """Assert that each scenario matches its expected validation outcome."""
    is_valid, message, _ = service.evaluate_query(
        scenario.sparql,
        ATTRIBUTE_CONFIGS,
        max_semi_sensitive_group_by=1,
    )
    assert is_valid == scenario.expected_allowed, (
        f"[{scenario.id}] {scenario.natural_language}\n"
        f"  Expected {'ALLOWED' if scenario.expected_allowed else 'REJECTED'}, "
        f"got {'ALLOWED' if is_valid else 'REJECTED'}.\n"
        f"  Message: {message}\n"
        f"  Note: {scenario.note}"
    )


# ── Stand-alone entry point for the report ──────────────────────────────

if __name__ == "__main__":
    results = run_validation(ALL_SCENARIOS)
    print_report(results)
    # Exit with non-zero if any scenario failed
    if not all(r["pass"] for r in results):
        sys.exit(1)
