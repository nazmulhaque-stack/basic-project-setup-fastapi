"""FastAPI dependency providers: Session -> Cache -> Crypto -> Elasticsearch -> Service.

Clean Architecture boundary: Endpoints and Controllers only ever see domain Services.

Split into:
- `factory.py`: the `provide()` mechanism (session/cache/crypto/ES injection). Import
  `provide` from here to build a new entity's `get_x_service`/`XServiceDep` pair —
  defined directly in that entity's own `app/api/v1/<entity>.py` module, next to the
  router that uses it, rather than in a shared cross-entity registry.
- `support.py`: the two dependencies that don't fit that shape (log search, temp state).
"""

from app.api.deps.factory import SessionDep, get_crypto, get_redis_client, provide
from app.api.deps.support import (
    LogSearchServiceDep,
    TempStateDep,
    get_log_search_service,
    get_temp_state,
)

__all__ = [
    "provide",
    "SessionDep",
    "get_crypto",
    "get_redis_client",
    "get_log_search_service",
    "get_temp_state",
    "LogSearchServiceDep",
    "TempStateDep",
]
