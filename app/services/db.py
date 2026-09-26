"""
Shared Postgres connection pool.

Every service was previously doing psycopg2.connect(...) / conn.close() on
every single call — fine at low volume, but each connection is a fresh
TCP+auth handshake, and if any single request doesn't close cleanly (a
client aborting mid-request, an unhandled exception skipping the finally
block, etc.) connections can leak until the DB or the app runs out of
headroom. That's the most likely explanation for the "works after a fresh
restart, then hangs after a while" pattern.

This module opens a small, bounded pool once at import time and hands
connections out via a context manager that always returns them to the
pool (even on error) rather than closing them outright. Every service
(main.py, route_risk_api.py, agent.py, ingestion scripts) should import
get_conn from here instead of calling psycopg2.connect directly.

connect_timeout=5 means a genuinely unreachable DB fails in 5 seconds with
a clear ConnectionError instead of hanging a request (and a thread pool
slot) forever — this is what turned one bad connection into "the whole
API stops responding" before.
"""

import os
from contextlib import contextmanager

import psycopg2
import psycopg2.pool
import psycopg2.extras

DB_DSN = os.environ["DATABASE_URL"]

# minconn=1: keep at least one warm connection so the very first request
# after startup isn't paying the connect cost.
# maxconn=10: generous for a small API + a couple of ingestion scripts
# running concurrently; raise if you see "connection pool exhausted"
# errors under real load, but don't set it far beyond what your Postgres
# plan's max_connections allows across all your services combined.
_pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=DB_DSN,
    connect_timeout=5,
    cursor_factory=psycopg2.extras.RealDictCursor,
)


@contextmanager
def get_conn():
    """
    Usage:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
                conn.commit()

    On any exception, the connection is rolled back before being returned
    to the pool -- a failed request can't leave a half-committed
    transaction sitting on a connection that gets handed to the next caller.
    """
    conn = _pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def closeall():
    """Call on app shutdown if you want a clean exit; not required for
    normal operation since the pool is process-scoped."""
    _pool.closeall()