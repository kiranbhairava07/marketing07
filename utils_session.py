"""
utils_session.py

One fix vs the original:
    cleanup_old_sessions had a broken SQL query:

        INTERVAL ':days days'

    PostgreSQL does NOT substitute bind parameters inside string literals,
    so ':days' was never replaced — the interval was literally the string
    ':days days', which PostgreSQL would reject or silently ignore.
    The cleanup never actually deleted anything.

    Fixed by using Python string formatting for the integer value (safe
    since days_old is typed as int, not user input).

Everything else (is_new_user_atomic, get_session_first_action) is correct
and unchanged.
"""

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
import logging

logger = logging.getLogger(__name__)


async def is_new_user_atomic(
    db: AsyncSession,
    session_id: str,
    action_type: str,
    branch_id: int = None,
    qr_code_id: int = None
) -> bool:
    """
    Atomically determine if this is a new user by trying to insert into session_first_seen.

    Uses the database PRIMARY KEY constraint on session_id to guarantee uniqueness —
    no application-level locks, no race conditions possible.

    Returns True (NEW user) if this is the first time this session_id is seen.
    Returns False (RETURNING user) if session_id already exists, or on any error.
    """
    try:
        query = text("""
            INSERT INTO session_first_seen
                (session_id, first_action_type, first_branch_id, first_qr_code_id)
            VALUES
                (:session_id, :action_type, :branch_id, :qr_code_id)
            ON CONFLICT (session_id) DO NOTHING
            RETURNING session_id
        """)

        result = await db.execute(
            query,
            {
                "session_id": session_id,
                "action_type": action_type,
                "branch_id": branch_id,
                "qr_code_id": qr_code_id
            }
        )

        # Commit immediately to release the lock
        await db.commit()

        inserted = result.scalar_one_or_none()

        if inserted:
            logger.info(f"✅ NEW user detected: session={session_id[:8]}..., action={action_type}")
            return True
        else:
            logger.info(f"🔄 RETURNING user detected: session={session_id[:8]}..., action={action_type}")
            return False

    except IntegrityError as e:
        logger.debug(f"Session {session_id[:8]}... already exists (IntegrityError): {e}")
        await db.rollback()
        return False

    except Exception as e:
        logger.error(f"Error checking session {session_id[:8]}...: {e}", exc_info=True)
        await db.rollback()
        return False


async def get_session_first_action(db: AsyncSession, session_id: str) -> dict:
    """
    Get information about when we first saw this session.
    Returns dict or None if not found.
    """
    try:
        query = text("""
            SELECT
                session_id,
                first_seen_at,
                first_action_type,
                first_branch_id,
                first_qr_code_id,
                created_at
            FROM session_first_seen
            WHERE session_id = :session_id
        """)

        result = await db.execute(query, {"session_id": session_id})
        row = result.one_or_none()

        if row:
            return {
                "session_id":        row.session_id,
                "first_seen_at":     row.first_seen_at,
                "first_action_type": row.first_action_type,
                "first_branch_id":   row.first_branch_id,
                "first_qr_code_id":  row.first_qr_code_id,
                "created_at":        row.created_at
            }
        return None

    except Exception as e:
        logger.error(f"Error fetching session info: {e}")
        return None


async def cleanup_old_sessions(db: AsyncSession, days_old: int = 90):
    """
    Delete session records older than `days_old` days.

    FIX: original used INTERVAL ':days days' — bind params are not substituted
    inside PostgreSQL string literals, so the interval was never applied.
    Using Python formatting for the integer is safe here (days_old is int, not
    user-supplied string input).
    """
    try:
        query = text(f"""
            DELETE FROM session_first_seen
            WHERE created_at < NOW() - INTERVAL '{int(days_old)} days'
            RETURNING session_id
        """)

        result = await db.execute(query)
        await db.commit()

        deleted_count = len(result.all())
        logger.info(f"🧹 Cleaned up {deleted_count} old sessions (older than {days_old} days)")
        return deleted_count

    except Exception as e:
        logger.error(f"Error cleaning up old sessions: {e}")
        await db.rollback()
        return 0