"""Tests for ResponseGenerator — LLM response, fallback, sanitization, validation."""

import pytest
from unittest.mock import MagicMock

from models.noisy_result import NoisyResult
from query_generation.response_generator import ResponseGenerator


# ── Fixtures ────────────────────────────────────────────────────────────

@pytest.fixture
def mock_llm():
    """Return a mock LLMClient whose .call() can be configured per test."""
    return MagicMock()


@pytest.fixture
def generator(mock_llm):
    return ResponseGenerator(llm_client=mock_llm)


@pytest.fixture
def single_row_result():
    return NoisyResult(
        rows=[{"averageSalary": 4523.17}],
        aggregate_info=[{"variable": "averageSalary", "function": "avg", "attribute": "gehalt"}],
    )


@pytest.fixture
def multi_row_result():
    return NoisyResult(
        rows=[
            {"city": "Vienna", "avgSalary": 5100.50},
            {"city": "Graz", "avgSalary": 4200.30},
        ],
        aggregate_info=[{"variable": "avgSalary", "function": "avg", "attribute": "gehalt"}],
    )


@pytest.fixture
def empty_result():
    return NoisyResult(rows=[], aggregate_info=[])


# ── LLM-based generation ────────────────────────────────────────────────

class TestGenerateResponse:
    def test_returns_llm_response_when_valid(self, generator, mock_llm, single_row_result):
        mock_llm.call.return_value = "The average salary is 4523.17 EUR."
        result = generator.generate_response("What is the average salary?", single_row_result)
        assert result == "The average salary is 4523.17 EUR."

    def test_uses_fallback_when_llm_fails(self, generator, mock_llm, single_row_result):
        mock_llm.call.side_effect = Exception("API error")
        result = generator.generate_response("What is the average salary?", single_row_result)
        assert "averageSalary" in result
        assert "4523.17" in result

    def test_uses_fallback_when_llm_rounds_values(self, generator, mock_llm, single_row_result):
        # LLM rounded 4523.17 → 4523 — validation should catch this
        mock_llm.call.return_value = "The average salary is approximately 4523 EUR."
        result = generator.generate_response("What is the average salary?", single_row_result)
        # Should fall back to template (contains the exact value)
        assert "4523.17" in result


# ── Input sanitization ──────────────────────────────────────────────────

class TestSanitizeInput:
    def test_strips_control_characters(self):
        dirty = "Hello\x00\x01\x02World"
        clean = ResponseGenerator._sanitize_input(dirty)
        assert "\x00" not in clean
        assert "\x01" not in clean
        assert "HelloWorld" in clean

    def test_preserves_normal_whitespace(self):
        text = "Hello World\nNew line\tTab"
        clean = ResponseGenerator._sanitize_input(text)
        assert clean == text

    def test_truncates_long_input(self):
        long_text = "x" * 1000
        clean = ResponseGenerator._sanitize_input(long_text)
        assert len(clean) == 500


# ── Output validation ───────────────────────────────────────────────────

class TestValidateOutput:
    def test_exact_match_passes(self, single_row_result):
        response = "The average salary is 4523.17 per month."
        assert ResponseGenerator._validate_output(response, single_row_result) is True

    def test_rounded_value_fails(self, single_row_result):
        response = "The average salary is approximately 4523 per month."
        assert ResponseGenerator._validate_output(response, single_row_result) is False

    def test_no_numbers_in_response_passes(self, single_row_result):
        response = "The average salary is quite reasonable."
        assert ResponseGenerator._validate_output(response, single_row_result) is True

    def test_empty_result_always_passes(self, empty_result):
        response = "There are 42 entries in the database."
        assert ResponseGenerator._validate_output(response, empty_result) is True


# ── Fallback template ───────────────────────────────────────────────────

class TestFallbackTemplate:
    def test_single_row_format(self, single_row_result):
        result = ResponseGenerator._fallback_template(single_row_result)
        assert "Results for your query:" in result
        assert "averageSalary" in result
        assert "4523.17" in result

    def test_multi_row_format(self, multi_row_result):
        result = ResponseGenerator._fallback_template(multi_row_result)
        assert "1." in result
        assert "2." in result
        assert "Vienna" in result
        assert "Graz" in result

    def test_empty_result_message(self, empty_result):
        result = ResponseGenerator._fallback_template(empty_result)
        assert result == "The query returned no results."
