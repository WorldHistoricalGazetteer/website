"""A QUERY-rate limiter for the reconciliation API (place#268).

WHG had no rate limiter of any kind — no `DEFAULT_THROTTLE_CLASSES`, no
`throttle_classes` anywhere in `api/`, no `limit_req` in prod nginx. The only
control was the per-account daily allowance, which counts **requests**.

🛑 The central design constraint, and the reason a stock DRF throttle is the
wrong tool: **a request-rate limiter throttles exactly the behaviour we ask for
and leaves the wasteful shape untouched.**

    client shape   requests/min   queries/request   gateway queries/min
    unbatched            20              1                  20
    well-batched          2             50                 100

The well-behaved client — the one following our published guidance — imposes
five times the load at a tenth of the request rate. So this charges **queries**,
not requests.

That matches what is actually scarce. The gateway saturates under sustained load
and recovers instantly when idle: bursts of `requests.Timeout` against a 10 s
budget, zero connection errors, successes interleaved with failures throughout —
a queue, not an outage. The resource being exhausted is gateway concurrency, and
it is consumed by *queries in flight*. Each `/reconcile` request fans out across
up to `RECON_FANOUT` concurrent gateway workers, so one 50-query batch can
occupy the whole fan-out for its duration.

⚠️ The daily allowance is deliberately left alone. It rewards batching correctly
— at 5,000 requests/day a client batching at 50 can reconcile 250,000 names
against 5,000 unbatched — and that part works.

## Fixed window, and why that is honest rather than lazy

One counter per user per 60-second window, incremented atomically. A fixed window
admits up to **2× the limit** across a boundary (the tail of one window plus the
head of the next), so the effective burst ceiling is `2 × RECON_QUERY_RATE`. That
is a known property, not a defect, and it is the burst allowance rather than a
surprise: at the default 600/min the burst ceiling is 1200.

A token bucket would smooth it, at the cost of a Lua script or a read-modify-write
that is not atomic under `gthread` workers. Given measured traffic — 325 POSTs
over nine days, max batch 25 — smoothness buys nothing and atomicity buys
correctness, so the simple mechanism wins on its merits.

## Why Redis and not the default cache

`CACHES['default']` is a **file-based** cache with `TIMEOUT: None`. File-based
caches have no atomic increment, so under concurrency two threads read the same
value and one write is lost — a limiter that silently undercounts, which is worse
than none because it reports itself as working. `django_redis` supports atomic
`incr`, and Redis is already in the stack for Celery.

🛑 **If Redis is unreachable this FAILS OPEN**, and that is deliberate. A limiter
that cannot count must not become an outage: the failure mode of failing closed
here is the entire reconciliation API returning 429 because a cache is down,
which is a far worse incident than the overload this protects against. The
degradation is logged so it is visible rather than silent.
"""

import logging
import time

from django.conf import settings
from django.core.cache import caches
from django.core.cache.backends.base import InvalidCacheBackendError

logger = logging.getLogger('reconciliation')

# Queries per minute per user. Override in settings. A falsy value means
# UNLIMITED, matching the convention `UserAPIProfile.daily_limit` already
# established (`if profile.daily_limit and ...`) — see place#213's lockout.
DEFAULT_QUERY_RATE = 600

WINDOW_SECONDS = 60

# Cache alias to count in. Falls back to 'default' only if absent, which is
# logged loudly because 'default' is file-based and cannot count atomically.
THROTTLE_CACHE_ALIAS = 'throttle'


def _rate() -> int:
    return int(getattr(settings, 'RECON_QUERY_RATE', DEFAULT_QUERY_RATE) or 0)


def _cache():
    try:
        return caches[THROTTLE_CACHE_ALIAS]
    except InvalidCacheBackendError:
        logger.warning(
            "throttle: cache alias '%s' is not configured; falling back to "
            "'default', which is file-based and CANNOT count atomically — the "
            "limiter will undercount under concurrency (place#268)",
            THROTTLE_CACHE_ALIAS)
        return caches['default']


def _key(user) -> str:
    """Per-identity counter key.

    A token-authenticated caller has a pk. A session caller arrives as
    `CSRFUser` (an `AnonymousUser` subclass whose `is_authenticated` is True),
    which has **no pk** — so all of them would otherwise share one bucket and
    throttle each other. Those are browser calls from Map your Data, whose
    volume is bounded by a human, so they are keyed separately and collectively.
    """
    pk = getattr(user, 'pk', None)
    if pk:
        return f"rq:u{pk}"
    return "rq:session"


def consume_query_budget(user, n_queries: int) -> tuple[bool, int]:
    """Charge `n_queries` against `user`'s per-minute budget.

    Returns `(allowed, retry_after_seconds)`. `retry_after_seconds` is 0 when
    allowed.

    ⚠️ The charge is applied even when it takes the caller over the limit, so a
    client that ignores 429 and hammers cannot reset its own window by retrying.
    """
    rate = _rate()
    if not rate:
        return True, 0

    n = max(1, int(n_queries))
    now = time.time()
    window = int(now // WINDOW_SECONDS)
    key = f"{_key(user)}:{window}"

    cache = _cache()
    try:
        # `add` then `incr`: `incr` raises if the key is absent, and `add` is a
        # no-op when another thread got there first, so the pair is safe under
        # concurrency without a lock.
        cache.add(key, 0, WINDOW_SECONDS * 2)
        used = cache.incr(key, n)
    except Exception as exc:
        # FAIL OPEN — see the module docstring. A limiter that cannot count must
        # not take the API down with it.
        logger.warning("throttle: cache unavailable, allowing request unthrottled: %s", exc)
        return True, 0

    # 🛑 `incr` can return None instead of raising, and this is the path that
    # matters in production rather than an edge case:
    #
    #   * `django_redis` configured with IGNORE_EXCEPTIONS (as the `throttle`
    #     alias is) SWALLOWS a Redis outage and returns None — so the `except`
    #     above never fires;
    #   * a backend that does not implement atomic incr can also return None.
    #
    # Without this guard `None > rate` raises TypeError, which means a Redis blip
    # would 500 every reconcile POST — strictly worse than both failing open and
    # failing closed. Found by running this suite alongside the others, where the
    # `throttle` alias was absent and the fallback backend returned None; the
    # mocked-exception tests all passed while this path was wide open.
    if not isinstance(used, int):
        logger.warning("throttle: backend returned %r from incr (no atomic counter, or a "
                       "swallowed cache error); allowing request unthrottled", used)
        return True, 0

    if used > rate:
        retry_after = max(1, int((window + 1) * WINDOW_SECONDS - now))
        logger.info("throttle: %s over budget — %d/%d queries this window, retry in %ds",
                    _key(user), used, rate, retry_after)
        return False, retry_after

    return True, 0


def throttle_message(retry_after: int) -> str:
    """The 429 body text.

    Says what the limit counts, because a client told only "too many requests"
    will reduce its batch size — the opposite of what we want. Our published
    client guidance already tells integrators to retry on 429, and until now we
    never emitted one, so a client that implemented correct backoff had nothing
    to back off against.
    """
    return (
        f"Query rate limit exceeded: at most {_rate()} queries per minute. "
        f"Retry after {retry_after}s. NOTE: the limit counts QUERIES, not "
        f"requests — keep batching (up to 50 per request) and slow the rate of "
        f"requests rather than shrinking the batches."
    )
