# Talk-to-Data Service

> A privacy-preserving natural language interface for querying Linked Data (JSON-LD). Users ask questions in plain language; the service translates them into SPARQL, enforces differential privacy guarantees, and returns noisy, human-readable answers.

---

## Table of Contents

1. [Overview](#overview)
2. [Service Workflow](#service-workflow)
3. [Technical Component Reference](#technical-component-reference)
4. [Data Models](#data-models)
5. [Environment Configuration](#environment-configuration)
6. [API Endpoints](#api-endpoints)
7. [Privacy Rules (R1–R6)](#privacy-rules-r1r6)
8. [Current Limitations & Open Tasks](#current-limitations--open-tasks)

---

## Overview

The **Talk-to-Data Service** allows users to ask natural language questions against a Linked Data dataset while enforcing **differential privacy** (DP). The service:

1. Fetches an **ontology** (schema definition with sensitivity annotations) from a remote SOYA endpoint.
2. Translates a user question into a **SPARQL query** via an LLM.
3. **Evaluates** the query against six static privacy rules (R1–R6) to prevent sensitive data leakage.
4. Checks and deducts from a global **privacy budget** (ε-based).
5. **Executes** the SPARQL query on the provided JSON-LD data.
6. Applies **Laplace noise** to aggregate results and **suppresses small groups**.
7. Generates a **natural language response** using an LLM with numeric output validation.
8. Returns the response together with session metadata and remaining budget.

```
 User Question + JSON-LD Data + Ontology URL
                │
                ▼
 ┌──────────────────────────────────────────────────┐
 │               OrchestratorService                │
 │                                                  │
 │  1. Fetch Ontology                               │
 │  2. Generate SPARQL          (LLM)               │
 │  3. Evaluate Query           (Rules R1–R6)       │
 │  4. Check Privacy Budget                         │
 │  5. Deduct Budget                                │
 │  6. Execute SPARQL           (rdflib)            │
 │  7. Add Laplace Noise                            │
 │  8. Suppress Small Groups                        │
 │  9. Generate NL Response     (LLM + validation)  │
 │ 10. Return Response                              │
 └──────────────────────────────────────────────────┘
                │
                ▼
 JSON response: { response, sessionId, remainingPrivacyBudget, data, ... }
```

---

## Service Workflow

### Step-by-Step Pipeline

The orchestrator (`OrchestratorService.talk_to_data`) executes the following pipeline for every incoming request:

| Step | Action | Component | Failure Behaviour |
|------|--------|-----------|-------------------|
| 0 | **Session management** — get or create a session; record the user question in conversation history | `SessionService` | — |
| 1 | **Fetch ontology** — download the YAML ontology from `{ontology_url}/yaml` and parse it into an `Ontology` object | `FetchOntologyService` | Return `ONTOLOGY_FETCH_FAILED` |
| 2 | **Generate SPARQL** — send ontology + question to an LLM to produce a SPARQL query | `QueryGenerator` _(currently hardcoded)_ | Return `QUERY_GENERATION_FAILED` |
| 3 | **Build sensitivity config** — extract per-attribute sensitivity levels and bounds (min/max) from the ontology | Inline in orchestrator | — |
| 4 | **Static evaluation (R1–R6)** — parse the SPARQL algebra tree and check six privacy rules | `QueryEvaluationService` | Return `QUERY_REJECTED` |
| 5 | **Budget check** — compute ε cost (`epsilon_base × num_aggregate_columns`) and verify there is enough remaining budget | `PrivacyBudgetService` | Return `BUDGET_EXHAUSTED` |
| 6 | **Deduct budget** — subtract the query cost from the global budget and add it to the session's `epsilon_spent` | `PrivacyBudgetService` / `SessionService` | — |
| 7 | **Execute query** — run the SPARQL query against the JSON-LD data using `rdflib` | `QueryExecutionService` | Return `QUERY_EXECUTION_FAILED` |
| 8 | **Add Laplace noise** — inject calibrated noise into each aggregate column (COUNT, SUM, AVG) | `NoiseService` | — |
| 9 | **Suppress small groups** — remove rows whose noisy COUNT falls below `min_group_size` | `NoiseService` | — |
| 10 | **Wrap in NoisyResult** — package the noisy rows and aggregate metadata into an immutable `NoisyResult` | `NoisyResult` model | — |
| 11 | **Generate NL response** — send the question + noisy results to an LLM; validate that numeric values were not rounded/altered; fall back to a deterministic template on failure | `ResponseGenerator` | Deterministic fallback |
| 12 | **Record & return** — append the assistant message to conversation history and return the final JSON | Orchestrator | — |

---

## Technical Component Reference

### Directory Structure

```
talk-to-data-service/
├── server.py                        # Flask app – HTTP endpoints
├── orchestrator/
│   ├── orchestrator_service.py      # Central pipeline coordinator
│   └── fetch_ontology_service.py    # Ontology fetching & YAML parsing
├── query_generation/
│   ├── query_generation_service.py  # SPARQL generation via LLM
│   ├── llm_client.py               # Provider-agnostic LLM wrapper
│   ├── sparql_agent.py              # System prompt & user message formatting
│   └── response_generator.py       # NL response generation + validation
├── query_evaluation/
│   └── query_evaluation_service.py  # Static privacy rule analysis (R1–R6)
├── query_execution/
│   └── query_execution_service.py   # SPARQL execution on JSON-LD via rdflib
├── privacy/
│   ├── noise_service.py             # Laplace noise injection + suppression
│   └── privacy_budget_service.py    # Global ε budget tracking
├── models/
│   ├── ontology.py                  # Ontology / OntologyObject / Attribute
│   ├── noisy_result.py              # NoisyResult (immutable result wrapper)
│   ├── privacy_config.py            # PrivacyConfig (env-based DP settings)
│   └── session_service.py           # Session / SessionService
├── tests/
│   ├── test_noise_service.py
│   ├── test_privacy_budget_service.py
│   ├── test_query_evaluation_r5_r6.py
│   ├── test_response_generator.py
│   └── test_session_service.py
├── requirements.txt
├── .env.example
└── .env                             # Local overrides (git-ignored)
```

---

### `server.py` — Flask HTTP Layer

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/ping` | GET | Health check; returns `"Server running"` |
| `/api/talk-to-data` | POST | Main endpoint — accepts a question, data, ontology URL, and optional session ID |
| `/api/privacy-budget` | GET | Returns the current global privacy budget status |
| `/api/privacy-budget/reset` | POST | Resets the privacy budget (for testing/demo only) |

**Request body** for `/api/talk-to-data`:

```json
{
  "question": "What is the average salary?",
  "data": { },
  "ontology_url": "https://soya.ownyourdata.eu/AnonymisationDemo2",
  "sessionId": "optional-uuid"
}
```

**Response body**:

```json
{
  "response": "The average salary is approximately 45321.78.",
  "sessionId": "uuid-v4",
  "remainingPrivacyBudget": 0.8,
  "sessionEpsilonSpent": 0.1,
  "status": "success",
  "data": {
    "query_results": [ { "averageSalary": 45321.78 } ],
    "sparql_query": "PREFIX oyd: <...> SELECT ..."
  },
  "conversationHistory": [
    { "role": "user", "content": "What is the average salary?" },
    { "role": "assistant", "content": "The average salary is approximately 45321.78." }
  ]
}
```

---

### `OrchestratorService` — Pipeline Coordinator

**Location**: `orchestrator/orchestrator_service.py`

Central class that wires all sub-services together. Owns the global `PrivacyBudgetService` and `SessionService`. Implements the 13-step pipeline described in the workflow section.

Key methods:

| Method | Purpose |
|--------|---------|
| `talk_to_data(question, data, ontology_url, session_id)` | Runs the full pipeline and returns the response dict |
| `_error_response(error_code, session_id, conversation_history)` | Builds a standardised error response with a user-safe message |

---

### `FetchOntologyService` — Ontology Fetching

**Location**: `orchestrator/fetch_ontology_service.py`

Fetches an ontology from `{url}/yaml`, parses the YAML, and constructs an `Ontology` object. Reads sensitivity levels and min/max bounds from the `OverlayClassification` overlay.

---

### `QueryGenerator` — SPARQL Generation

**Location**: `query_generation/query_generation_service.py`

Uses `LLMClient` to translate a natural language question + ontology into a SPARQL query. Strips markdown code fences from LLM output.

> **Note**: SPARQL generation is currently **hardcoded** in the orchestrator (see [Limitations](#current-limitations--open-tasks)). The `QueryGenerator` class is implemented but not yet wired up.

---

### `LLMClient` — Provider-Agnostic LLM Wrapper

**Location**: `query_generation/llm_client.py`

Supports four providers via a unified `call(system_prompt, user_message) → str` API:

| Provider | Underlying SDK |
|----------|---------------|
| `openai` | `openai.OpenAI` |
| `anthropic` | `anthropic.Anthropic` |
| `azure` | `openai.AzureOpenAI` |
| `google` | `google.generativeai` |

Configuration is read from `LLM_*` environment variables.

---

### `ResponseGenerator` — Natural Language Responses

**Location**: `query_generation/response_generator.py`

Generates a human-readable answer from question + `NoisyResult`. Includes:

- **Input sanitisation** — strips control characters, truncates question to 500 chars.
- **Prompt isolation** — user question is wrapped in `<user_question>` tags.
- **Output validation** — every number in the LLM response is checked against the actual noisy values. If the LLM rounded or hallucinated a number, the response is rejected and a deterministic fallback template is used.

---

### `QueryEvaluationService` — Static Privacy Rules

**Location**: `query_evaluation/query_evaluation_service.py`

Parses SPARQL queries into an algebra tree (via `rdflib`) and enforces six privacy rules. Returns `(is_valid, message, aggregate_info)`.

Aggregate metadata (`aggregate_info`) is passed downstream to `NoiseService` so noise can be calibrated per-column.

---

### `QueryExecutionService` — SPARQL Execution

**Location**: `query_execution/query_execution_service.py`

Loads JSON-LD data into an `rdflib.Graph`, runs the SPARQL query, and converts RDF terms to native Python types (preserving `int`/`float`/`datetime`).

---

### `NoiseService` — Laplace Mechanism

**Location**: `privacy/noise_service.py`

| Method | Sensitivity (Δf) | Scale |
|--------|------------------|-------|
| `_add_count_noise` | 1 | `1 / ε` |
| `_add_sum_noise` | `max − min` | `(max − min) / ε` |
| `_add_avg_noise_clipped_mean` | `max − min` (clipped mean) | `(max − min) / (ε / 2)` |
| `suppress_small_groups` | — | Removes rows with noisy count < `min_group_size` |

Uses `numpy` for random number generation with an optional seed for reproducible tests.

---

### `PrivacyBudgetService` — Budget Tracking

**Location**: `privacy/privacy_budget_service.py`

Tracks a **global** ε budget (shared across all sessions). Key operations:

- `calculate_query_cost(num_aggregate_columns)` → `epsilon_base × num_columns`
- `check_budget(epsilon_query)` → `True` if enough budget remains
- `deduct_budget(epsilon_query)` → subtracts from remaining budget
- `reset()` → resets spent budget (for testing only)

---

## Data Models

### `Ontology` / `OntologyObject` / `Attribute`

**Location**: `models/ontology.py`

```
Ontology
├── prefix: str                  (e.g. "oyd")
├── base_uri: str                (e.g. "https://soya.ownyourdata.eu/AnonymisationDemo2/")
└── objects: List[OntologyObject]
    └── OntologyObject
        ├── name: str
        └── attributes: List[Attribute]
            └── Attribute
                ├── name: str
                ├── anonymization_type: str
                ├── sensitivity_level: str   ("sensitive" | "semi-sensitive" | "not-sensitive")
                ├── min_value: Optional[float]
                └── max_value: Optional[float]
```

### `NoisyResult`

**Location**: `models/noisy_result.py`

Immutable (`frozen=True`) dataclass wrapping post-noise query results. Enforces at the type level that only noised data reaches the response layer.

### `PrivacyConfig`

**Location**: `models/privacy_config.py`

Loaded from environment variables. Fields: `epsilon_total`, `epsilon_base`, `min_group_size`, `max_semi_sensitive_group_by`.

### `Session` / `SessionService`

**Location**: `models/session_service.py`

In-memory session store keyed by UUIDv4. Each session tracks conversation history and per-session `epsilon_spent`.

---

## Environment Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_PROVIDER` | `openai` | LLM provider (`openai`, `anthropic`, `azure`, `google`) |
| `LLM_API_KEY` | — | API key for the LLM provider |
| `LLM_MODEL` | `gpt-4o` | Model identifier |
| `RESPONSE_LLM_PROVIDER` | _(falls back to `LLM_PROVIDER`)_ | Override provider for NL response generation |
| `RESPONSE_LLM_MODEL` | _(falls back to `LLM_MODEL`)_ | Override model for NL response generation |
| `RESPONSE_LLM_API_KEY` | _(falls back to `LLM_API_KEY`)_ | Override API key for NL response generation |
| `EPSILON_TOTAL` | `1.0` | Total global privacy budget |
| `EPSILON_BASE` | `0.1` | ε cost per aggregate column in a query |
| `MIN_GROUP_SIZE` | `5` | Minimum noisy count to keep a group (suppression threshold) |
| `MAX_SEMI_SENSITIVE_GROUP_BY` | `1` | Max semi-sensitive attributes allowed in GROUP BY |
| `AZURE_OPENAI_ENDPOINT` | — | Required when `LLM_PROVIDER=azure` |
| `AZURE_OPENAI_API_VERSION` | `2024-02-15-preview` | API version for Azure OpenAI |

---

## API Endpoints

### `POST /api/talk-to-data`

The main endpoint. Accepts `question`, `data` (JSON-LD), `ontology_url`, and optional `sessionId`. Returns the noisy answer, session metadata, and remaining budget.

### `GET /api/privacy-budget`

Returns `remaining_budget`, `epsilon_total`, and `epsilon_base`.

### `POST /api/privacy-budget/reset`

Resets the global budget to `epsilon_total`. **Intended for testing/demo only.**

### `GET /api/ping`

Health check.

---

## Privacy Rules (R1–R6)

These rules are enforced by `QueryEvaluationService` through static analysis of the SPARQL algebra tree **before** execution:

| Rule | Description |
|------|-------------|
| **R1** | **Sensitive attributes** must not appear anywhere in the query (SELECT, WHERE, FILTER). |
| **R2** | **Semi-sensitive attributes** must not appear in `FILTER` expressions. |
| **R3** | **Semi-sensitive attributes** in `SELECT` must be wrapped in an aggregate function (COUNT, AVG, SUM, etc.). |
| **R4** | At most `MAX_SEMI_SENSITIVE_GROUP_BY` semi-sensitive attributes may appear in `GROUP BY` (prevents quasi-identifier creation). |
| **R5** | `MIN`, `MAX`, `SAMPLE`, and `GROUP_CONCAT` are **blocked** on semi-sensitive attributes (these aggregates can reveal individual values). |
| **R6** | `SUM` and `AVG` on semi-sensitive attributes **require** min/max bounds in the ontology overlay (needed to calibrate sensitivity for the Laplace mechanism). |

---

## Current Limitations & Open Tasks

### Known Limitations

- **In-memory state**: The privacy budget and session store are held in memory. A server restart resets both. There is no persistence layer.
- **Hardcoded SPARQL query**: The SPARQL generation step is currently **hardcoded** in the orchestrator (`orchestrator_service.py` step 2). The `QueryGenerator` class exists but is not wired into the pipeline.
- **Single-provider LLM**: Only one LLM provider is active at a time. There is no retry/fallback across providers.
- **Global budget only**: The privacy budget is global (shared across all sessions). There is no per-user or per-dataset budget isolation.
- **No authentication/authorisation**: The API has no auth layer. Anyone with network access can query the service and consume the privacy budget.
- **No rate limiting**: There is no throttle on API requests.
- **YAML-only ontology format**: The ontology must be served in the specific SOYA YAML format. No JSON-LD or Turtle ontology input is supported.

### Open Tasks

- [ ] **Wire up `QueryGenerator`** — replace the hardcoded SPARQL query in the orchestrator with actual LLM-based generation.
- [ ] **Persistent budget storage** — use a database or file-backed store so the budget survives server restarts.
- [ ] **Per-user / per-dataset budget isolation** — allow scoped budgets instead of a single global one.
- [ ] **Authentication & authorisation** — add API key or OAuth-based access control.
- [ ] **Rate limiting** — prevent budget exhaustion through rapid-fire requests.
- [ ] **Multi-turn SPARQL generation** — leverage conversation history to refine queries across turns.
- [ ] **Support additional ontology formats** — accept JSON-LD or Turtle ontology definitions directly.
- [ ] **Improve AVG noise mechanism** — the clipped-mean implementation currently adds noise directly to the average without a true noisy-sum / noisy-count division (the count information is not available at noise injection time).
- [ ] **Frontend / chatbot UI** — build a web interface for the chatbot (the response shape already supports it).
- [ ] **Logging & monitoring** — add structured logging, metrics export, and request tracing.
- [ ] **Containerisation** — provide a `Dockerfile` and `docker-compose.yml` for deployment.
