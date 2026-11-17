"""Typed application errors mapped to HTTP responses in main.py."""
from __future__ import annotations


class QueryServiceError(Exception):
    """Base class for errors that carry a stable machine-readable code."""

    code = "query_service_error"
    http_status = 500

    def __init__(self, message: str = "", extra: dict | None = None) -> None:
        super().__init__(message)
        self.extra = extra or {}


class InvalidSqlError(QueryServiceError):
    code = "invalid_sql"
    http_status = 400


class InvalidDeadlineError(QueryServiceError):
    code = "invalid_deadline"
    http_status = 422


class PoolAcquireTimeout(QueryServiceError):
    """Raised when no connection became available within the total deadline."""

    code = "acquire_timeout"
    http_status = 503


class PoolAcquireCancelled(QueryServiceError):
    """Raised when a still-queued request is revoked (e.g. client gone).

    Only requests that have not yet started executing can be cancelled;
    running SQL is never interrupted.
    """

    code = "acquire_cancelled"
    # 499 mirrors the conventional "client closed request" status and keeps
    # the receipt distinct from 503 (deadline) and 504 (execution timeout).
    http_status = 499


class QueryTimeout(QueryServiceError):
    """Raised when the statement did not finish before the total deadline."""

    code = "query_timeout"
    http_status = 504
