"""Tests for SessionService — session creation, history, and budget isolation."""

import pytest
from session.session_service import SessionService, Session


@pytest.fixture
def svc():
    return SessionService()


class TestCreateSession:
    def test_creates_valid_uuid(self, svc):
        session = svc.create_session()
        assert len(session.session_id) == 36  # UUID4 format
        assert "-" in session.session_id

    def test_each_session_has_unique_id(self, svc):
        s1 = svc.create_session()
        s2 = svc.create_session()
        assert s1.session_id != s2.session_id


class TestGetSession:
    def test_returns_existing_session(self, svc):
        session = svc.create_session()
        found = svc.get_session(session.session_id)
        assert found is session

    def test_returns_none_for_unknown_id(self, svc):
        assert svc.get_session("non-existent-id") is None


class TestGetOrCreateSession:
    def test_none_id_creates_new_session(self, svc):
        session = svc.get_or_create_session(None)
        assert isinstance(session, Session)

    def test_empty_string_creates_new_session(self, svc):
        session = svc.get_or_create_session("")
        assert isinstance(session, Session)

    def test_known_id_returns_existing(self, svc):
        original = svc.create_session()
        returned = svc.get_or_create_session(original.session_id)
        assert returned is original

    def test_unknown_id_creates_new_session(self, svc):
        session = svc.get_or_create_session("unknown-uuid-1234")
        assert isinstance(session, Session)
        assert session.session_id != "unknown-uuid-1234"


class TestConversationHistory:
    def test_add_to_history(self, svc):
        session = svc.create_session()
        svc.add_to_history(session.session_id, "user", "Hello")
        svc.add_to_history(session.session_id, "assistant", "Hi there!")

        assert len(session.conversation_history) == 2
        assert session.conversation_history[0] == {
            "role": "user", "content": "Hello"
        }
        assert session.conversation_history[1] == {
            "role": "assistant", "content": "Hi there!"
        }

    def test_add_to_unknown_session_is_noop(self, svc):
        # Should not raise
        svc.add_to_history("non-existent", "user", "test")
