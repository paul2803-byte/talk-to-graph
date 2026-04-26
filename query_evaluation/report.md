# Query Validation Report — AnonymisationDemo Ontology

**Result: 22/22 scenarios matched expectations**

> [!IMPORTANT]
> **Sensitivity inheritance enabled:** Address sub-attributes (`detail`, `city`, `zip`, `state`, `country`) now inherit `semi-sensitive` from their parent attribute `adresse`. This changes the behavior of several queries compared to the initial report.

> [!NOTE]
> **R3 refinement:** Rule R3 now exempts GROUP BY variables — a semi-sensitive attribute in GROUP BY + SELECT is a grouping key (risk managed by R4), not raw data exposure.

## How to run

```bash
# As pytest (recommended)
python -m pytest tests/test_validation_scenarios.py -v --tb=short

# As standalone report
set PYTHONPATH=.
python tests/test_validation_scenarios.py
```

---

## User-provided scenarios (U1–U7)

### U1 — What is the average gehalt? → ALLOWED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gehalt ?gehalt .
}
```
Simple aggregate with COUNT — compliant.

---

### U2 — Who has the highest gehalt? → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?name ?gehalt
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:name ?name ;
       oyd:gehalt ?gehalt .
}
ORDER BY DESC(?gehalt)
LIMIT 1
```
**Rule R1**: Sensitive attribute `name` appears in query.

---

### U3 — Average gehalt per geburtsdatum (in decade steps)? → ALLOWED ✓ // TODO -> in the query the decade is not used. 

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?geburtsdatum (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gehalt ?gehalt ;
       oyd:geburtsdatum ?geburtsdatum .
}
GROUP BY ?geburtsdatum
```
`?geburtsdatum` (semi-sensitive) appears in SELECT as a GROUP BY key — exempt from R3. Only 1 semi-sensitive in GROUP BY (R4 allows max 1). Now **allowed**.

> [!NOTE]
> **Changed from initial report:** Previously rejected by R3. With the R3 refinement (GROUP BY keys are exempt), this query is now allowed. GROUP BY exposure is controlled by R4.

---

### U4 — What are the gehalt values? → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?gehalt
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gehalt ?gehalt .
}
```
**Rule R3**: Semi-sensitive attribute in SELECT without aggregation.

---

### U5 — What is the average gehalt per country? → ALLOWED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?country (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gehalt ?gehalt ;
       oyd:adresse ?addr .
    ?addr oyd:country ?country .
}
GROUP BY ?country
```
GROUP BY `country` (semi-sensitive, inherited from `adresse`) + AVG on `gehalt` (semi-sensitive) with COUNT. 1 semi-sensitive in GROUP BY is within R4 limit — allowed.

---

### U6 — What is the most common name? → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?name (COUNT(?name) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:name ?name .
}
GROUP BY ?name
ORDER BY DESC(?cnt)
LIMIT 1
```
**Rule R1**: Sensitive attribute `name` appears in query.

---

### U7 — What are the street values? → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?detail
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:adresse ?addr .
    ?addr oyd:detail ?detail .
}
```
**Rule R3**: `detail` (street) inherits `semi-sensitive` from `adresse` — raw SELECT without aggregation is blocked.

> [!NOTE]
> **Changed from initial report:** Previously allowed (detail was not-sensitive). With sensitivity inheritance, detail is now semi-sensitive.

---

## Additional generated scenarios — Allowed (A1–A5)

### A1 — How many records are there in total? → ALLOWED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT (COUNT(?s) AS ?total)
WHERE {
    ?s a oyd:AnonymisationDemo .
}
```
Simple count, no sensitive access.

---

### A2 — What is the average gewicht? → ALLOWED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT (AVG(?gewicht) AS ?avg_gewicht) (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gewicht ?gewicht .
}
```
AVG on semi-sensitive with bounds + COUNT — compliant.

---

### A3 — What is the average koerpergroesse per city? → ALLOWED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?city (AVG(?kg) AS ?avg_kg) (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:koerpergroesse ?kg ;
       oyd:adresse ?addr .
    ?addr oyd:city ?city .
}
GROUP BY ?city
```
GROUP BY `city` (semi-sensitive, inherited from `adresse`) + AVG on semi-sensitive with COUNT. 1 semi-sensitive in GROUP BY — allowed.

---

### A4 — How many people live in each country? → ALLOWED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?country (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:adresse ?addr .
    ?addr oyd:country ?country .
}
GROUP BY ?country
```
COUNT grouped by `country` (semi-sensitive, inherited from `adresse`). 1 semi-sensitive in GROUP BY — allowed.

---

### A5 — What is the total gehalt per state? → ALLOWED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?state (SUM(?gehalt) AS ?total_gehalt) (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gehalt ?gehalt ;
       oyd:adresse ?addr .
    ?addr oyd:state ?state .
}
GROUP BY ?state
```
SUM on semi-sensitive with bounds + COUNT, GROUP BY `state` (semi-sensitive, inherited from `adresse`). 1 semi-sensitive in GROUP BY — allowed.

---

## Additional generated scenarios — Rejected (R1–R8)

### R1 — List all names and their salaries → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?name ?gehalt
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:name ?name ;
       oyd:gehalt ?gehalt .
}
```
**Rule R1**: Sensitive attribute `name` in SELECT.

---

### R2 — Average gehalt for people born on 1990-01-01? → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gehalt ?gehalt ;
       oyd:geburtsdatum "1990-01-01"^^xsd:date .
}
```
**Rule R2b**: Concrete literal on semi-sensitive predicate `geburtsdatum`.

---

### R3 — Show everyone's gewicht → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?gewicht
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gewicht ?gewicht .
}
```
**Rule R3**: Semi-sensitive in SELECT without aggregation.

---

### R4 — Average gehalt grouped by geburtsdatum AND koerpergroesse? → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?geburtsdatum ?koerpergroesse (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gehalt ?gehalt ;
       oyd:geburtsdatum ?geburtsdatum ;
       oyd:koerpergroesse ?koerpergroesse .
}
GROUP BY ?geburtsdatum ?koerpergroesse
```
**Rule R4**: Two semi-sensitive attributes in GROUP BY creates quasi-identifier risk.

---

### R5 — What is the maximum gehalt? → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT (MAX(?gehalt) AS ?max_gehalt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gehalt ?gehalt .
}
```
**Rule R5**: MAX on semi-sensitive is blocked unconditionally.

---

### R6 — Average gehalt for people with gehalt above 50000? → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gehalt ?gehalt .
    FILTER(?gehalt > 50000)
}
```
**Rule R2**: FILTER on semi-sensitive attribute `gehalt`.

---

### R7 — What is person1's gehalt? → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?gehalt
WHERE {
    oyd:person1 oyd:gehalt ?gehalt .
}
```
**Rule R7**: Concrete subject URI accessing semi-sensitive predicate.

---

### R8 — Average koerpergroesse (no COUNT) → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT (AVG(?kg) AS ?avg_kg)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:koerpergroesse ?kg .
}
```
**Rule R8**: AVG on semi-sensitive without COUNT for small-group suppression.

---

## Composite sensitivity inheritance scenarios (C1–C2)

### C1 — Average gehalt grouped by country AND city → REJECTED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?country ?city (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gehalt ?gehalt ;
       oyd:adresse ?addr .
    ?addr oyd:country ?country ;
          oyd:city ?city .
}
GROUP BY ?country ?city
```
**Rule R4**: Two semi-sensitive attrs (`country`, `city` — both inherited from `adresse`) in GROUP BY creates quasi-identifier risk.

---

### C2 — Average gehalt per zip code → ALLOWED ✓

```sparql
PREFIX oyd: <https://soya.ownyourdata.eu/AnonymisationDemo/>
SELECT ?zip (AVG(?gehalt) AS ?avg_gehalt) (COUNT(?s) AS ?cnt)
WHERE {
    ?s a oyd:AnonymisationDemo ;
       oyd:gehalt ?gehalt ;
       oyd:adresse ?addr .
    ?addr oyd:zip ?zip .
}
GROUP BY ?zip
```
GROUP BY `zip` (semi-sensitive, inherited from `adresse`) + AVG on `gehalt` (semi-sensitive) with COUNT. 1 semi-sensitive in GROUP BY — allowed.

---

## Ontology configuration used

The sensitivity config and bounds were derived from the AnonymisationDemo YAML overlay:

| Attribute | Sensitivity | Inherited from | Bounds (min, max) |
|-----------|-------------|----------------|-------------------|
| name | **sensitive** | — | — |
| adresse | semi-sensitive | — | — |
| ↳ detail | semi-sensitive | adresse | — |
| ↳ city | semi-sensitive | adresse | — |
| ↳ zip | semi-sensitive | adresse | — |
| ↳ state | semi-sensitive | adresse | — |
| ↳ country | semi-sensitive | adresse | — |
| geburtsdatum | semi-sensitive | — | — |
| gehalt | semi-sensitive | — | 0 – 200,000 |
| latitude | semi-sensitive | — | -90 – 90 |
| longitude | semi-sensitive | — | -180 – 180 |
| gewicht | semi-sensitive | — | 30 – 300 |
| koerpergroesse | semi-sensitive | — | 50 – 250 |
| start_pv | semi-sensitive | — | — |

> [!IMPORTANT]
> The bounds for `gehalt`, `latitude`, `longitude`, `gewicht`, and `koerpergroesse` were **manually set**. The YAML overlay doesn't include min/max values natively — adjust these in the test file if needed.
