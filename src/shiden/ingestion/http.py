"""Shared HTTP fetch helper for all ingesters.

Replaces the three identical ``_fetch_with_retry`` copies that lived in the
OPCOM, BNR and ECB ingesters (and gives the weather ingester retries, which
it previously lacked).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping

import httpx

logger = logging.getLogger(__name__)


def get_text_with_retry(
    url: str,
    *,
    params: Mapping[str, str | float] | None = None,
    timeout: float = 30.0,
    max_retries: int = 3,
    source: str = "HTTP",
) -> str:
    """GET ``url`` and return the response body as text.

    Retries on 5xx responses and timeouts with exponential backoff
    (1 s, 2 s, 4 s …).  4xx responses raise immediately — they will not be
    fixed by retrying.  ``source`` labels log lines (e.g. "OPCOM", "BNR").
    """
    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                response = client.get(url, params=params)
                if response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"Server error {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return response.text
        except (httpx.TimeoutException, httpx.HTTPStatusError) as exc:
            if (
                isinstance(exc, httpx.HTTPStatusError)
                and exc.response.status_code < 500
            ):
                raise  # 4xx — retrying won't help
            last_exc = exc
            if attempt < max_retries - 1:
                logger.warning(
                    "%s fetch attempt %d/%d failed (%s), retrying in %.1fs",
                    source,
                    attempt + 1,
                    max_retries,
                    exc,
                    delay,
                )
                time.sleep(delay)
                delay *= 2
    raise RuntimeError(
        f"{source} fetch failed after {max_retries} attempts: {last_exc}"
    ) from last_exc
