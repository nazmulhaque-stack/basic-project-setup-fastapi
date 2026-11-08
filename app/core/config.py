"""Core configuration module utilizing Pydantic Settings v2.

Loads environment variables once and caches via @lru_cache.
Handles database pooling params, Redis cluster topology, rate limiter rules,
cache TTLs and per-operation flags, encryption keys, and reverse proxy validation.
"""

from functools import lru_cache
import ipaddress
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _sync_db_url(
    url_str: str,
    user: Optional[str],
    password: Optional[str],
    host: Optional[str],
    port: Optional[int],
    db_name: Optional[str],
    *,
    override_host_port: bool,
) -> str:
    """Overlay individual DB_* credential overrides onto a DATABASE_URL-shaped
    connection string, without any SQLAlchemy URL object. Preserves a real
    configured host/port over an explicit localhost/127.0.0.1 override (so a
    `.env` default of `DB_HOST=localhost` never clobbers a deployment's real
    `DATABASE_URL`).
    """
    parts = urlsplit(url_str)
    new_user = user if user is not None else parts.username
    new_password = password if password is not None else parts.password

    if override_host_port:
        existing_host = parts.hostname
        if host is not None:
            if existing_host and existing_host not in ("127.0.0.1", "localhost") and host in ("127.0.0.1", "localhost"):
                new_host = existing_host
            else:
                new_host = host
        else:
            new_host = existing_host

        if port is not None:
            if existing_host and existing_host not in ("127.0.0.1", "localhost") and host in ("127.0.0.1", "localhost"):
                new_port = parts.port
            else:
                new_port = port
        else:
            new_port = parts.port
    else:
        new_host = parts.hostname
        new_port = parts.port

    new_db = db_name if db_name is not None else (parts.path.lstrip("/") or None)

    netloc = ""
    if new_user:
        netloc += quote(str(new_user), safe="")
        if new_password:
            netloc += f":{quote(str(new_password), safe='')}"
        netloc += "@"
    if new_host:
        netloc += str(new_host)
    if new_port:
        netloc += f":{new_port}"

    new_path = f"/{new_db}" if new_db else parts.path
    return urlunsplit((parts.scheme, netloc, new_path, parts.query, parts.fragment))


class CacheOperationFlags(BaseModel):
    """Granular per-operation feature flags for dynamic caching behavior.

    Allows enabling or disabling cache at each individual CRUD juncture without redeployment.
    """
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    enable_get_item: bool = Field(default=True, alias="CACHE_ENABLE_GET_ITEM")
    enable_set_item: bool = Field(default=True, alias="CACHE_ENABLE_SET_ITEM")
    enable_delete_item: bool = Field(default=True, alias="CACHE_ENABLE_DELETE_ITEM")
    enable_get_list: bool = Field(default=True, alias="CACHE_ENABLE_GET_LIST")
    enable_set_list: bool = Field(default=True, alias="CACHE_ENABLE_SET_LIST")
    enable_invalidate_list: bool = Field(default=True, alias="CACHE_ENABLE_INVALIDATE_LIST")
    enable_negative_cache: bool = Field(default=True, alias="CACHE_ENABLE_NEGATIVE_CACHE")
    enable_stampede_lock: bool = Field(default=True, alias="CACHE_ENABLE_STAMPEDE_LOCK")


class Settings(BaseSettings):
    """Central application settings parsed from environment variables."""

    # General App Settings
    APP_NAME: str = "HighPerfStatelessService"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

    @field_validator("DEBUG", mode="before")
    @classmethod
    def parse_debug(cls, v: Any) -> bool:
        """Tolerates string values like 'release', 'production', 'dev', etc."""
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            v_clean = v.strip().lower()
            if v_clean in ("true", "1", "yes", "debug", "dev", "development"):
                return True
            if v_clean in ("false", "0", "no", "release", "prod", "production"):
                return False
        return False

    # PostgreSQL / PgBouncer Connection & Pooling
    # Database Credentials & Individual Connection Parameters
    DB_USER: Optional[str] = Field(default=None, description="Database username")
    DB_PASSWORD: Optional[str] = Field(default=None, description="Database password")
    DB_HOST: Optional[str] = Field(default=None, description="Database host")
    DB_PORT: Optional[int] = Field(default=None, description="Database port")
    DB_NAME: Optional[str] = Field(default=None, description="Database name")

    POSTGRES_USER: Optional[str] = Field(default=None, description="PostgreSQL username alias")
    POSTGRES_PASSWORD: Optional[str] = Field(default=None, description="PostgreSQL password alias")
    POSTGRES_HOST: Optional[str] = Field(default=None, description="PostgreSQL host alias")
    POSTGRES_PORT: Optional[int] = Field(default=None, description="PostgreSQL port alias")
    POSTGRES_DB: Optional[str] = Field(default=None, description="PostgreSQL database name alias")

    DATABASE_URL: str = Field(
        default="postgresql://postgres:postgres@127.0.0.1:6432/app_db",
        description="Postgres connection string for asyncpg (a 'postgresql+asyncpg://' SQLAlchemy-style prefix is also accepted and normalized)",
    )
    MIGRATION_DATABASE_URL: Optional[str] = Field(
        default=None,
        description="Optional direct database URL bypassing PgBouncer, kept in sync with DATABASE_URL's credentials",
    )
    ENVIRONMENT: str = Field(
        default="development",
        description="Deployment environment name, e.g. development, testing, or production",
    )
    PGBOUNCER_ENABLED: bool = Field(
        default=True,
        description="When True, disables client statement caching and uses NullPool for transaction pooling",
    )
    DB_POOL_SIZE: int = Field(
        default=5,
        ge=1,
        description="Base connection pool size per worker process (kept small for PgBouncer)",
    )
    DB_MAX_OVERFLOW: int = Field(
        default=5,
        ge=0,
        description="Max overflow connections allowed per worker during traffic bursts",
    )
    DB_POOL_TIMEOUT: int = Field(
        default=30,
        description="Seconds to wait before throwing a timeout when obtaining a pool connection",
    )
    DB_POOL_RECYCLE: int = Field(
        default=1800,
        description="Recycle connections every 30 minutes to purge stale TCP sockets",
    )
    MAX_PAGE_SIZE: int = Field(
        default=100,
        description="Maximum allowed items per paginated page",
    )

    # Elasticsearch 8 Log & Search Engine
    ELASTICSEARCH_ENABLED: bool = Field(
        default=True,
        description="Enables Elasticsearch for fast text search and centralized log analysis",
    )
    ELASTICSEARCH_URL: str = Field(
        default="http://127.0.0.1:9200",
        description="Elasticsearch cluster HTTP/HTTPS endpoint",
    )
    ELASTICSEARCH_INDEX_PREFIX: str = Field(
        default="demo",
        description="Prefix for Elasticsearch index namespaces",
    )
    ELASTICSEARCH_USERNAME: Optional[str] = Field(
        default=None,
        description="Optional Elasticsearch HTTP Basic username",
    )
    ELASTICSEARCH_PASSWORD: Optional[str] = Field(
        default=None,
        description="Optional Elasticsearch HTTP Basic password",
    )
    ELASTICSEARCH_API_KEY: Optional[str] = Field(
        default=None,
        description="Optional Elasticsearch base64 encoded API key",
    )
    ELASTICSEARCH_VERIFY_CERTS: bool = Field(
        default=False,
        description="Enforce TLS certificate verification for Elasticsearch cluster",
    )
    ELASTICSEARCH_CA_CERTS: Optional[str] = Field(
        default=None,
        description="Path to CA bundle for Elasticsearch TLS verification",
    )
    ELASTICSEARCH_TIMEOUT: float = Field(
        default=5.0,
        description="Elasticsearch request timeout in seconds",
    )
    ELASTICSEARCH_MAX_RETRIES: int = Field(
        default=3,
        description="Maximum retry attempts on connection or network failures",
    )
    ELASTICSEARCH_LOG_INDEX: str = Field(
        default="app-logs",
        description="Index template / data stream target for centralized application logs",
    )
    ELASTICSEARCH_LOG_RETENTION_DAYS: int = Field(
        default=30,
        description="Retention horizon for application logs in days",
    )
    ELASTICSEARCH_LOG_BUFFER_SIZE: int = Field(
        default=1000,
        description="Maximum bounded capacity of the asynchronous in-memory log queue",
    )
    ELASTICSEARCH_LOG_FLUSH_INTERVAL: float = Field(
        default=2.0,
        description="Periodic flush interval in seconds for asynchronous log buffer",
    )
    ELASTICSEARCH_LOG_BATCH_SIZE: int = Field(
        default=100,
        description="Number of log records batched per bulk indexing request",
    )

    # Elasticsearch Host/Port/Scheme Individual Parameters
    ELASTICSEARCH_HOST: Optional[str] = Field(default=None, description="Elasticsearch hostname")
    ELASTICSEARCH_PORT: Optional[int] = Field(default=None, description="Elasticsearch port")
    ELASTICSEARCH_SCHEME: Optional[str] = Field(default=None, description="Elasticsearch protocol (http or https)")

    # Redis Individual Parameters
    REDIS_HOST: Optional[str] = Field(default=None, description="Redis standalone host")
    REDIS_PORT: Optional[int] = Field(default=None, description="Redis standalone port")
    REDIS_USER: Optional[str] = Field(default=None, description="Redis username (Redis 6+ ACL)")
    REDIS_DB: Optional[int] = Field(default=None, description="Redis DB index (0-15)")

    # Redis (Cluster 7 and Standalone)
    REDIS_URL: Optional[str] = Field(
        default=None,
        description="Optional Redis standalone connection URL (e.g. redis://127.0.0.1:6379/0 or rediss://). If provided, standalone client is preferred.",
    )
    REDIS_CLUSTER_NODES: str = Field(
        default="127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002",
        description="Comma-separated list of host:port for Redis Cluster startup nodes",
    )
    REDIS_PASSWORD: Optional[str] = Field(default=None, description="Redis authentication password")
    REDIS_SOCKET_TIMEOUT: float = Field(default=2.0, description="Redis socket read/write timeout in seconds")
    REDIS_CONNECT_TIMEOUT: float = Field(default=3.0, description="Redis initial connection timeout in seconds")
    REDIS_MAX_CONNECTIONS: int = Field(default=50, description="Max pool connection limit per worker process")
    REDIS_SSL: bool = Field(default=False, description="Enable TLS for Redis connections")

    # Rate Limiting Defaults
    RATE_LIMIT_DEFAULT: int = 10
    RATE_LIMIT_WINDOW: int = 60
    RATE_LIMIT_FAIL_OPEN: bool = True

    # Cache TTLs (in seconds, lock in ms)
    CACHE_ITEM_TTL: int = 300
    CACHE_LIST_TTL: int = 120
    CACHE_NEGATIVE_TTL: int = 30
    CACHE_LOCK_TTL_MS: int = 5000

    # Category-Specific TTLs (in seconds)
    CACHE_TTL_USER: int = Field(default=300, description="TTL for user profile cache items")

    # Temporary State TTLs (in seconds)
    IDEMPOTENCY_KEY_TTL: int = Field(default=86400, description="TTL for idempotency tokens (24 hours)")
    CHALLENGE_TOKEN_TTL: int = Field(default=300, description="TTL for verification challenges (5 minutes)")

    # Dynamic Cache Flags
    CACHE_ENABLE_GET_ITEM: bool = True
    CACHE_ENABLE_SET_ITEM: bool = True
    CACHE_ENABLE_DELETE_ITEM: bool = True
    CACHE_ENABLE_GET_LIST: bool = True
    CACHE_ENABLE_SET_LIST: bool = True
    CACHE_ENABLE_INVALIDATE_LIST: bool = True
    CACHE_ENABLE_NEGATIVE_CACHE: bool = True
    CACHE_ENABLE_STAMPEDE_LOCK: bool = True

    # Transactional Outbox & Background Elasticsearch Synchronization
    OUTBOX_ENABLED: bool = Field(default=True, description="Enable transactional outbox processing")
    OUTBOX_POLL_INTERVAL: float = Field(default=1.0, description="Interval in seconds between outbox worker polls")
    OUTBOX_BATCH_SIZE: int = Field(default=50, description="Max outbox records processed per batch")
    OUTBOX_MAX_RETRIES: int = Field(default=5, description="Max retry attempts for failed outbox sync events")

    # Encryption (AES-256-GCM + Blind Index)
    # Map of version_int -> 64-char hex string (32 bytes)
    ENCRYPTION_KEYS: str = Field(
        default='{"1": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}',
        description="JSON string or path to file containing version-to-key hex map",
    )
    ACTIVE_KEY_VERSION: int = Field(default=1, ge=1)
    BLIND_INDEX_KEY: str = Field(
        default="fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
        description="Hex string or path to file containing 32-byte key for HMAC-SHA256 blind indexing",
    )

    # Reverse Proxy Security
    TRUSTED_PROXIES: str = Field(
        default="127.0.0.1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16",
        description="Comma-separated CIDRs or IPs of trusted reverse proxies",
    )

    @model_validator(mode="after")
    def resolve_service_urls(self) -> "Settings":
        """Dynamically synchronize DATABASE_URL, REDIS_URL, and ELASTICSEARCH_URL with individual credentials."""
        # 1. Resolve Redis standalone URL if host is provided and REDIS_URL is not explicitly set
        if self.REDIS_HOST and not self.REDIS_URL:
            scheme = "rediss" if self.REDIS_SSL else "redis"
            auth = ""
            if self.REDIS_USER and self.REDIS_PASSWORD:
                auth = f"{self.REDIS_USER}:{self.REDIS_PASSWORD}@"
            elif self.REDIS_PASSWORD:
                auth = f":{self.REDIS_PASSWORD}@"
            port = self.REDIS_PORT or 6379
            db = self.REDIS_DB or 0
            self.REDIS_URL = f"{scheme}://{auth}{self.REDIS_HOST}:{port}/{db}"

        # 2. Resolve Elasticsearch URL if host is provided
        if self.ELASTICSEARCH_HOST:
            scheme = self.ELASTICSEARCH_SCHEME or ("https" if self.ELASTICSEARCH_VERIFY_CERTS else "http")
            port = self.ELASTICSEARCH_PORT or 9200
            self.ELASTICSEARCH_URL = f"{scheme}://{self.ELASTICSEARCH_HOST}:{port}"

        user = self.DB_USER or self.POSTGRES_USER
        password = self.DB_PASSWORD or self.POSTGRES_PASSWORD
        host = self.DB_HOST or self.POSTGRES_HOST
        port = self.DB_PORT or self.POSTGRES_PORT
        db_name = self.DB_NAME or self.POSTGRES_DB

        if user or password or host or port or db_name:
            try:
                self.DATABASE_URL = _sync_db_url(
                    self.DATABASE_URL, user, password, host, port, db_name, override_host_port=True
                )
                if self.MIGRATION_DATABASE_URL:
                    self.MIGRATION_DATABASE_URL = _sync_db_url(
                        self.MIGRATION_DATABASE_URL, user, password, None, None, None, override_host_port=False
                    )
            except Exception:
                pass
        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    @property
    def parsed_redis_nodes(self) -> List[Tuple[str, int]]:
        """Parses comma-separated host:port string into list of (host, port) tuples."""
        nodes: List[Tuple[str, int]] = []
        for raw_node in self.REDIS_CLUSTER_NODES.split(","):
            raw_node = raw_node.strip()
            if not raw_node:
                continue
            parts = raw_node.split(":")
            if len(parts) == 2:
                nodes.append((parts[0].strip(), int(parts[1].strip())))
            else:
                nodes.append((parts[0].strip(), 6379))
        if not nodes:
            nodes.append(("127.0.0.1", 6379))
        return nodes

    @property
    def parsed_trusted_proxies(self) -> List[ipaddress.IPv4Network | ipaddress.IPv6Network]:
        """Parses comma-separated trusted proxy strings into ip_network objects."""
        networks: List[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
        for raw_cidr in self.TRUSTED_PROXIES.split(","):
            raw_cidr = raw_cidr.strip()
            if not raw_cidr:
                continue
            try:
                # strict=False allows host addresses like 192.168.1.1/24
                networks.append(ipaddress.ip_network(raw_cidr, strict=False))
            except ValueError:
                # If pure IP without prefix, treat as /32 or /128
                try:
                    ip: ipaddress.IPv4Address | ipaddress.IPv6Address = ipaddress.ip_address(raw_cidr)
                    networks.append(ipaddress.ip_network(ip, strict=False))
                except ValueError:
                    pass
        return networks

    def get_encryption_keys(self) -> Dict[int, bytes]:
        """Resolves encryption keys from JSON string or file path into {version: 32_bytes}."""
        raw_val: str = self.ENCRYPTION_KEYS.strip()
        # Support file path reading with restricted permissions
        if os.path.exists(raw_val):
            raw_val = Path(raw_val).read_text().strip()

        try:
            parsed: Dict[str, str] = json.loads(raw_val)
        except Exception as e:
            raise ValueError(f"Invalid ENCRYPTION_KEYS JSON format: {e}") from e

        key_map: Dict[int, bytes] = {}
        for ver_str, hex_key in parsed.items():
            try:
                ver_int: int = int(ver_str)
                key_bytes: bytes = bytes.fromhex(hex_key.strip())
                if len(key_bytes) != 32:
                    raise ValueError(f"Key version {ver_int} must be exactly 32 bytes (64 hex characters), got {len(key_bytes)} bytes")
                key_map[ver_int] = key_bytes
            except Exception as e:
                raise ValueError(f"Failed parsing encryption key version '{ver_str}': {e}") from e

        if self.ACTIVE_KEY_VERSION not in key_map:
            raise ValueError(f"ACTIVE_KEY_VERSION {self.ACTIVE_KEY_VERSION} not found in ENCRYPTION_KEYS mapping")
        return key_map

    def get_blind_index_key(self) -> bytes:
        """Resolves HMAC blind index key from hex string or file path."""
        raw_val: str = self.BLIND_INDEX_KEY.strip()
        if os.path.exists(raw_val):
            raw_val = Path(raw_val).read_text().strip()
        try:
            key_bytes: bytes = bytes.fromhex(raw_val)
            if len(key_bytes) != 32:
                raise ValueError(f"BLIND_INDEX_KEY must be exactly 32 bytes (64 hex characters), got {len(key_bytes)} bytes")
            return key_bytes
        except Exception as e:
            raise ValueError(f"Invalid BLIND_INDEX_KEY: {e}") from e

    def get_cache_flags(self) -> CacheOperationFlags:
        """Returns the dynamic cache operation flags instance."""
        return CacheOperationFlags(
            enable_get_item=self.CACHE_ENABLE_GET_ITEM,
            enable_set_item=self.CACHE_ENABLE_SET_ITEM,
            enable_delete_item=self.CACHE_ENABLE_DELETE_ITEM,
            enable_get_list=self.CACHE_ENABLE_GET_LIST,
            enable_set_list=self.CACHE_ENABLE_SET_LIST,
            enable_invalidate_list=self.CACHE_ENABLE_INVALIDATE_LIST,
            enable_negative_cache=self.CACHE_ENABLE_NEGATIVE_CACHE,
            enable_stampede_lock=self.CACHE_ENABLE_STAMPEDE_LOCK,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Returns singleton instance of Settings across the worker process.

    Safe for Gunicorn multi-worker models because each forked process
    maintains its own isolated lru_cache instance.
    """
    return Settings()
