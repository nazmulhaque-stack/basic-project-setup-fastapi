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
from typing import Dict, List, Optional, Tuple

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CacheOperationFlags(BaseSettings):
    """Granular per-operation feature flags for dynamic caching behavior.

    Allows enabling or disabling cache at each individual CRUD juncture without redeployment.
    """
    model_config = SettingsConfigDict(populate_by_name=True, extra="ignore")

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
        default="postgresql+asyncpg://postgres:postgres@127.0.0.1:6432/app_db",
        description="Async database URL using asyncpg, asyncmy, or aiosqlite",
    )
    MIGRATION_DATABASE_URL: Optional[str] = Field(
        default=None,
        description="Optional direct database URL for Alembic migrations, bypassing PgBouncer",
    )
    ENVIRONMENT: str = Field(
        default="development",
        description="Deployment environment name, e.g. development, testing, or production",
    )
    AUTO_MIGRATE: bool = Field(
        default=False,
        description="Apply existing Alembic migrations during application startup",
    )
    AUTO_CREATE_TABLES: bool = Field(
        default=False,
        description="Create missing metadata tables for explicit development/testing bootstrap",
    )
    AUTO_SEED: bool = Field(
        default=False,
        description="Run registered idempotent database seeders during startup",
    )
    SEED_CATEGORIES: str = Field(
        default="[]",
        description="Optional JSON array of category names for the safe reference seeder",
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

    # Kafka event integration
    KAFKA_ENABLED: bool = Field(
        default=False,
        description="Enables the Kafka producer and transactional outbox dispatcher",
    )
    KAFKA_BOOTSTRAP_SERVERS: str = Field(
        default="127.0.0.1:9092",
        description="Comma-separated Kafka bootstrap broker addresses",
    )
    KAFKA_EVENTS_TOPIC: str = Field(
        default="domain.events",
        description="Topic for versioned domain event envelopes",
    )
    KAFKA_CONSUMER_GROUP: str = Field(
        default="highperf-fastapi",
        description="Default Kafka consumer group for service consumers",
    )
    KAFKA_INVENTORY_TOPIC: str = Field(
        default="inventory.events",
        description="Inbound topic containing inventory reservation requests",
    )
    KAFKA_INVENTORY_CONSUMER_GROUP: str = Field(
        default="highperf-inventory",
        description="Consumer group that owns inventory reservation processing",
    )
    KAFKA_INVENTORY_DLQ_TOPIC: str = Field(
        default="inventory.events.dlq",
        description="Dead-letter topic for invalid inventory reservation events",
    )
    INVENTORY_RESERVATION_SWEEP_INTERVAL: float = Field(default=30.0, gt=0)
    OUTBOX_POLL_INTERVAL: float = Field(default=1.0, gt=0)
    OUTBOX_BATCH_SIZE: int = Field(default=100, ge=1, le=1000)

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
    CACHE_TTL_PRODUCT: int = Field(default=600, description="TTL for catalog product cache items")
    CACHE_TTL_REFERENCE: int = Field(default=3600, description="TTL for static reference/lookup tables")
    CACHE_TTL_CONFIG: int = Field(default=1800, description="TTL for dynamic application configuration")

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
                from sqlalchemy.engine import make_url
                url = make_url(self.DATABASE_URL)
                new_user = user if user is not None else url.username
                new_password = password if password is not None else url.password
                if host is not None:
                    if url.host and url.host not in ("127.0.0.1", "localhost") and host in ("127.0.0.1", "localhost"):
                        new_host = url.host
                    else:
                        new_host = host
                else:
                    new_host = url.host

                if port is not None:
                    if url.host and url.host not in ("127.0.0.1", "localhost") and host in ("127.0.0.1", "localhost"):
                        new_port = url.port
                    else:
                        new_port = port
                else:
                    new_port = url.port
                new_database = db_name if db_name is not None else url.database

                updated_url = url.set(
                    username=new_user,
                    password=new_password,
                    host=new_host,
                    port=new_port,
                    database=new_database,
                )
                self.DATABASE_URL = updated_url.render_as_string(hide_password=False)

                if self.MIGRATION_DATABASE_URL:
                    mig_url = make_url(self.MIGRATION_DATABASE_URL)
                    mig_updated = mig_url.set(
                        username=user if user is not None else mig_url.username,
                        password=password if password is not None else mig_url.password,
                    )
                    self.MIGRATION_DATABASE_URL = mig_updated.render_as_string(hide_password=False)
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
