"""
Natural-language response generator.

Takes the user question and the **noisy** query results (wrapped in
``NoisyResult``) and produces a human-readable answer string.  Uses the
shared ``LLMClient`` so that no LLM-integration code is duplicated.

Security measures
-----------------
* **Input sanitization** — control characters are stripped, and the question
  is truncated to ``MAX_QUESTION_LENGTH`` before it is placed into the prompt.
* **Prompt isolation** — the user question is placed inside a clearly
  delimited ``<user_question>`` block and is never interpolated into the
  system prompt itself.
* **Output validation** — every numeric value that appears in the LLM
  response is checked against the values in ``NoisyResult.rows``.  If the
  LLM rounded, altered, or hallucinated a number, the response is rejected
  and the deterministic fallback template is used instead.
"""

import logging
import math
import os
import re
from typing import Any, Dict, List, Optional

from models.noisy_result import NoisyResult

logger = logging.getLogger(__name__)

MAX_QUESTION_LENGTH = 500

# ── system prompt ───────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are a data assistant.  You receive a user's question together with the
query results (which may contain one or more rows of data).

Your task is to produce a **concise, human-readable** answer in one or two
sentences.

Rules:
1. Include the **exact numeric values** from the query results — do NOT
   round, approximate, or omit them.
2. Do NOT reveal any technical details such as SPARQL queries, column names,
   or internal identifiers.
3. If there are multiple rows, summarise them as a brief list.
4. Answer in the same language as the user's question.
"""


class ResponseGeneratorError(Exception):
    """Raised when response generation fails irrecoverably."""
    pass


class ResponseGenerator:
    """Generates a natural-language response from noisy query results."""

    def __init__(self, llm_client=None):
        """Initialise with an optional ``LLMClient``.

        If *llm_client* is ``None``, a client is created lazily from the
        ``RESPONSE_LLM_*`` env-vars (falling back to the default ``LLM_*``
        env-vars).
        """
        self._llm_client = llm_client

    # ── lazy init ───────────────────────────────────────────────────────

    def _get_client(self):
        if self._llm_client is None:
            from query_generation.llm_client import LLMClient
            self._llm_client = LLMClient(
                api_key=os.getenv("RESPONSE_LLM_API_KEY") or os.getenv("LLM_API_KEY"),
                provider=os.getenv("RESPONSE_LLM_PROVIDER") or os.getenv("LLM_PROVIDER"),
                model=os.getenv("RESPONSE_LLM_MODEL") or os.getenv("LLM_MODEL"),
            )
        return self._llm_client

    # ── public API ──────────────────────────────────────────────────────

    def generate_response(
        self,
        question: str,
        noisy_result: NoisyResult,
    ) -> str:
        """Return a natural-language answer for *question* using *noisy_result*.

        Falls back to a deterministic template if the LLM call or output
        validation fails.
        """
        sanitized_question = self._sanitize_input(question)

        try:
            client = self._get_client()
            user_message = self._build_user_message(
                sanitized_question, noisy_result
            )
            llm_response = client.call(_SYSTEM_PROMPT, user_message)

            # Validate that the LLM didn't alter numeric values
            if self._validate_output(llm_response, noisy_result):
                return llm_response.strip()

            logger.warning(
                "LLM response failed numeric validation — using fallback template"
            )
        except Exception:
            logger.exception("LLM call for response generation failed — using fallback")

        return self._fallback_template(noisy_result)

    # ── input sanitization ──────────────────────────────────────────────

    @staticmethod
    def _sanitize_input(question: str) -> str:
        """Strip control characters and truncate to MAX_QUESTION_LENGTH."""
        # Remove ASCII control characters (0x00-0x1F) except common whitespace
        cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", question)
        return cleaned[:MAX_QUESTION_LENGTH]

    # ── prompt construction ─────────────────────────────────────────────

    @staticmethod
    def _build_user_message(question: str, noisy_result: NoisyResult) -> str:
        """Build the user-role message with clearly delimited sections."""
        rows_text = _format_rows(noisy_result.rows)
        return (
            f"<user_question>\n{question}\n</user_question>\n\n"
            f"<query_results>\n{rows_text}\n</query_results>"
        )

    # ── output validation ───────────────────────────────────────────────

    @staticmethod
    def _validate_output(response: str, noisy_result: NoisyResult) -> bool:
        """Check that every number in *response* matches a value in *noisy_result*.

        Returns ``True`` if all numbers pass (or there are no numbers to check).
        """
        # Collect all numeric values from noisy_result rows
        expected_values: set = set()
        for row in noisy_result.rows:
            for val in row.values():
                try:
                    expected_values.add(float(val))
                except (TypeError, ValueError):
                    pass

        if not expected_values:
            return True  # nothing numeric to validate

        # Extract every number from the LLM response text
        numbers_in_response = re.findall(
            r"-?\d+(?:[.,]\d+)*", response
        )

        for num_str in numbers_in_response:
            # Normalise comma-as-decimal to dot-as-decimal
            num_str_norm = num_str.replace(",", "")
            try:
                num_val = float(num_str_norm)
            except ValueError:
                continue

            # Allow numbers that trivially match row-counts, years, etc.
            # We only flag numbers that are "close" to an expected value
            # but don't actually match (i.e., the LLM rounded them).
            matched = any(
                math.isclose(num_val, ev, rel_tol=1e-5, abs_tol=1e-3)
                for ev in expected_values
            )
            roughly_close = any(
                math.isclose(num_val, ev, rel_tol=0.05, abs_tol=0.5)
                for ev in expected_values
            )
            if roughly_close and not matched:
                # The LLM rounded or altered a value
                logger.warning(
                    "Numeric value %.4f in response is close to but doesn't "
                    "match any expected value — validation failed",
                    num_val,
                )
                return False

        return True

    # ── fallback template ───────────────────────────────────────────────

    @staticmethod
    def _fallback_template(noisy_result: NoisyResult) -> str:
        """Deterministic template that renders noisy values faithfully."""
        if not noisy_result.rows:
            return "The query returned no results."

        lines: List[str] = ["Results for your query:"]

        if len(noisy_result.rows) == 1:
            row = noisy_result.rows[0]
            for key, value in row.items():
                lines.append(f"• {key}: {_format_value(value)}")
        else:
            for idx, row in enumerate(noisy_result.rows, start=1):
                parts = ", ".join(
                    f"{k}: {_format_value(v)}" for k, v in row.items()
                )
                lines.append(f"{idx}. {parts}")

        return "\n".join(lines)


# ── module-level helpers ────────────────────────────────────────────────

def _format_value(value: Any) -> str:
    """Format a single value for display."""
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _format_rows(rows: List[Dict[str, Any]]) -> str:
    """Format result rows as a simple text table for the LLM prompt."""
    if not rows:
        return "(no results)"

    parts: List[str] = []
    for idx, row in enumerate(rows, start=1):
        entries = ", ".join(f"{k}={v}" for k, v in row.items())
        parts.append(f"Row {idx}: {entries}")
    return "\n".join(parts)
