"""
Session management for the chatbot interface.

Sessions track conversation history and are identified by UUIDv4.
Privacy budget is kept **global** (shared across all sessions) to preserve
the differential-privacy guarantee — see plan review item 3.2.
"""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class Session:
    """An individual chat session."""

    session_id: str
    conversation_history: List[Dict[str, str]] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    epsilon_spent: float = 0.0


class SessionService:
    """Manages chat sessions (in-memory).

    The privacy budget is **not** per-session — it is global and managed by
    a single ``PrivacyBudgetService`` instance owned by the orchestrator.
    """

    def __init__(self) -> None:
        self._sessions: Dict[str, Session] = {}

    # ── public API ──────────────────────────────────────────────────────

    def create_session(self) -> Session:
        """Create a new session with a fresh UUIDv4 identifier."""
        session_id = str(uuid.uuid4())
        session = Session(session_id=session_id)
        self._sessions[session_id] = session
        logger.info("Created session %s", session_id)
        return session

    def get_session(self, session_id: str) -> Optional[Session]:
        """Return the session for *session_id*, or ``None`` if not found."""
        return self._sessions.get(session_id)

    def get_or_create_session(self, session_id: Optional[str] = None) -> Session:
        """Return an existing session or create a new one.

        - If *session_id* is ``None`` or empty, a **new** session is created.
        - If *session_id* is provided but unknown, a **new** session is
          created (the caller receives a different ID from the one they sent,
          signalling that the old session was not found).
        """
        if session_id:
            session = self.get_session(session_id)
            if session is not None:
                return session
            logger.warning(
                "Session %s not found — creating a new session", session_id
            )
        return self.create_session()

    def add_to_history(
        self, session_id: str, role: str, content: str
    ) -> None:
        """Append a message to the session's conversation history."""
        session = self.get_session(session_id)
        if session is None:
            logger.error("Cannot add to history — session %s not found", session_id)
            return
        session.conversation_history.append({"role": role, "content": content})

    def add_epsilon_spent(self, session_id: str, epsilon: float) -> None:
        """Add to the session's total epsilon spent."""
        session = self.get_session(session_id)
        if session is None:
            logger.error("Cannot add epsilon spent — session %s not found", session_id)
            return
        session.epsilon_spent += epsilon
