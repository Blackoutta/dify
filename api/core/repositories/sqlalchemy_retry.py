import logging
import random
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import TypeVar

from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

T = TypeVar("T")

SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS = 4
SQLALCHEMY_PERSISTENCE_RETRY_BASE_DELAY_SECONDS = 2
SQLALCHEMY_PERSISTENCE_RETRY_MAX_DELAY_SECONDS = 8
SQLALCHEMY_PERSISTENCE_RETRY_JITTER_SECONDS = 1


def is_retryable_db_error(exc: SQLAlchemyError) -> bool:
    return isinstance(exc, OperationalError) or (
        isinstance(exc, DBAPIError) and bool(getattr(exc, "connection_invalidated", False))
    )


def execute_with_db_retry(
    *,
    session_factory: Callable[[], AbstractContextManager[Session]],
    operation: Callable[[Session], T],
    logger: logging.Logger,
    operation_name: str,
    context: str,
) -> T:
    for attempt in range(1, SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS + 1):
        with session_factory() as session:
            try:
                return operation(session)
            except SQLAlchemyError as exc:
                logger.exception(
                    "%s persistence error for %s (attempt %s/%s)",
                    operation_name,
                    context,
                    attempt,
                    SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS,
                )
                if not is_retryable_db_error(exc) or attempt >= SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS:
                    raise

                try:
                    session.rollback()
                except Exception:
                    logger.debug("Failed to rollback %s persistence retry session", operation_name, exc_info=True)

                next_attempt = attempt + 1
                logger.info(
                    "Retrying %s persistence for %s (attempt %s/%s)",
                    operation_name,
                    context,
                    next_attempt,
                    SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS,
                )

        _sleep_before_retry(attempt)

    raise RuntimeError("unreachable workflow persistence retry state")


def _sleep_before_retry(attempt: int) -> None:
    delay = min(
        SQLALCHEMY_PERSISTENCE_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
        SQLALCHEMY_PERSISTENCE_RETRY_MAX_DELAY_SECONDS,
    )
    delay += random.uniform(0, SQLALCHEMY_PERSISTENCE_RETRY_JITTER_SECONDS)  # noqa: S311
    time.sleep(delay)
