"""Bulk synchronizes users and profiles from PostgreSQL to Elasticsearch.

Uses raw asyncpg with Keyset pagination and Elasticsearch _bulk API with streaming NDJSON.
"""

import asyncio
from datetime import datetime, timezone
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional
import uuid

import asyncpg
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgrespassword@localhost:5432/app_db")
ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
INDEX_PREFIX = os.getenv("ELASTICSEARCH_INDEX_PREFIX", "demo")
CHUNK_SIZE = int(os.getenv("SYNC_CHUNK_SIZE", "5000"))
MAX_SYNC = int(os.getenv("SYNC_MAX_USERS", "0"))  # 0 means all users


def serialize_doc(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    return str(obj)


async def init_indices(es_client: httpx.AsyncClient, profile_index: str, user_index: str) -> None:
    """Creates Elasticsearch indices with optimized settings and explicit mappings if they don't exist."""
    profile_mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "1s",
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "name": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "email": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "role": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "department": {
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                        },
                        "code": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                        },
                        "description": {"type": "text"},
                    }
                },
                "role_info": {
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                        },
                        "description": {"type": "text"},
                        "permissions": {"type": "keyword"},
                    }
                },
                "accounts": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "keyword"},
                        "account_number": {"type": "keyword"},
                        "account_type": {"type": "keyword"},
                        "balance_cents": {"type": "long"},
                        "currency": {"type": "keyword"},
                        "status": {"type": "keyword"},
                        "created_at": {"type": "date"},
                    },
                },
                "total_balance_cents": {"type": "long"},
                "account_count": {"type": "integer"},
                "phone_masked": {"type": "keyword"},
                "created_at": {"type": "date"},
                "department_name": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "department_code": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "role_name": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "account_numbers": {"type": "keyword"},
                "account_types": {"type": "keyword"},
                "currencies": {"type": "keyword"},
            }
        },
    }

    user_mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "1s",
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "name": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "email": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "role": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "department": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "phone_masked": {"type": "keyword"},
                "account_count": {"type": "integer"},
                "total_balance_cents": {"type": "long"},
                "created_at": {"type": "date"},
            }
        },
    }

    for idx_name, mapping in [(profile_index, profile_mapping), (user_index, user_mapping)]:
        check_resp = await es_client.head(f"/{idx_name}")
        if check_resp.status_code == 404:
            logger.info("Creating index: %s", idx_name)
            create_resp = await es_client.put(f"/{idx_name}", json=mapping)
            if create_resp.status_code not in (200, 201):
                logger.warning("Failed to create index %s: %s", idx_name, create_resp.text)
        else:
            logger.info("Index already exists: %s", idx_name)


async def sync_users():
    logger.info("Connecting to PostgreSQL: %s", DB_URL)
    conn = await asyncpg.connect(DB_URL)

    logger.info("Connecting to Elasticsearch: %s", ES_URL)
    async with httpx.AsyncClient(base_url=ES_URL, timeout=60.0) as es_client:
        # Check ES health
        resp = await es_client.get("/_cluster/health")
        if resp.status_code != 200:
            logger.error("Elasticsearch not healthy: %s", resp.text)
            await conn.close()
            return

        user_index = f"{INDEX_PREFIX}_user"
        profile_index = f"{INDEX_PREFIX}_user_profile"
        await init_indices(es_client, profile_index, user_index)

        # Pre-fetch lookup caches for departments and roles
        dept_rows = await conn.fetch("SELECT id, name, code, description FROM departments")
        dept_lookup: Dict[str, Dict[str, Any]] = {}
        for d in dept_rows:
            d_obj = {
                "id": d["id"],
                "name": d["name"],
                "code": d["code"],
                "description": d["description"] or "",
            }
            dept_lookup[d["name"].lower()] = d_obj
            dept_lookup[d["code"].lower()] = d_obj

        role_rows = await conn.fetch("SELECT id, name, description, permissions FROM roles")
        role_lookup: Dict[str, Dict[str, Any]] = {}
        for r in role_rows:
            r_obj = {
                "id": r["id"],
                "name": r["name"],
                "description": r["description"] or "",
                "permissions": list(r["permissions"] or []),
            }
            role_lookup[r["name"].lower()] = r_obj

        total_users = await conn.fetchval("SELECT count(*) FROM users;")
        target_sync = MAX_SYNC if (MAX_SYNC > 0 and MAX_SYNC < total_users) else total_users
        logger.info("Total users in PostgreSQL: %d | Target sync: %d", total_users, target_sync)

        # Temporarily disable index refresh for maximum bulk ingest throughput
        await es_client.put(f"/{profile_index}/_settings", json={"index": {"refresh_interval": "-1"}})
        await es_client.put(f"/{user_index}/_settings", json={"index": {"refresh_interval": "-1"}})

        last_created_at: Optional[datetime] = None
        last_id: Optional[uuid.UUID] = None
        synced_count = 0

        keyset_query = """
            SELECT 
                u.id, u.name, u.email, u.role, u.department, u.created_at,
                COALESCE(
                    json_agg(
                        json_build_object(
                            'id', a.id,
                            'account_number', a.account_number,
                            'account_type', a.account_type,
                            'balance_cents', a.balance_cents,
                            'currency', a.currency,
                            'status', a.status,
                            'created_at', a.created_at
                        )
                    ) FILTER (WHERE a.id IS NOT NULL),
                    '[]'::json
                ) AS accounts_json,
                COALESCE(SUM(a.balance_cents), 0)::bigint AS total_balance_cents,
                COUNT(a.id)::int AS account_count
            FROM (
                SELECT id, name, email, role, department, created_at
                FROM users
                WHERE ($1::timestamptz IS NULL OR (created_at, id) < ($1, $2))
                ORDER BY created_at DESC, id DESC
                LIMIT $3
            ) u
            LEFT JOIN user_accounts a ON a.user_id = u.id
            GROUP BY u.id, u.name, u.email, u.role, u.department, u.created_at
            ORDER BY u.created_at DESC, u.id DESC
        """

        try:
            while synced_count < target_sync:
                batch_limit = min(CHUNK_SIZE, target_sync - synced_count)
                rows = await conn.fetch(keyset_query, last_created_at, last_id, batch_limit)
                if not rows:
                    break

                bulk_lines: List[str] = []
                for r in rows:
                    user_id = str(r["id"])
                    created_iso = r["created_at"].isoformat() if r["created_at"] else None
                    dept_key = (r["department"] or "").lower()
                    role_key = (r["role"] or "").lower()

                    dept_dict = dept_lookup.get(
                        dept_key,
                        {
                            "id": 0,
                            "name": r["department"] or "",
                            "code": r["department"] or "",
                            "description": "",
                        },
                    )

                    role_dict = role_lookup.get(
                        role_key,
                        {
                            "id": 0,
                            "name": r["role"] or "user",
                            "description": "",
                            "permissions": [],
                        },
                    )

                    raw_accs = r["accounts_json"]
                    if isinstance(raw_accs, str):
                        try:
                            raw_accs = json.loads(raw_accs)
                        except Exception:
                            raw_accs = []
                    elif not isinstance(raw_accs, list):
                        raw_accs = []

                    profile_doc = {
                        "id": user_id,
                        "name": r["name"],
                        "email": r["email"],
                        "role": r["role"],
                        "department": dept_dict,
                        "role_info": role_dict,
                        "accounts": raw_accs,
                        "total_balance_cents": int(r["total_balance_cents"]),
                        "account_count": int(r["account_count"]),
                        "phone_masked": "***",
                        "created_at": created_iso,
                        "department_name": dept_dict["name"],
                        "department_code": dept_dict["code"],
                        "role_name": role_dict["name"],
                        "account_numbers": [a["account_number"] for a in raw_accs if isinstance(a, dict) and "account_number" in a],
                        "account_types": [a.get("account_type") for a in raw_accs if isinstance(a, dict) and "account_type" in a],
                        "currencies": list(set(a.get("currency") for a in raw_accs if isinstance(a, dict) and "currency" in a)),
                    }

                    user_doc = {
                        "id": user_id,
                        "name": r["name"],
                        "email": r["email"],
                        "role": r["role"],
                        "department": r["department"],
                        "phone_masked": "***",
                        "account_count": int(r["account_count"]),
                        "total_balance_cents": int(r["total_balance_cents"]),
                        "created_at": created_iso,
                    }

                    bulk_lines.append(json.dumps({"index": {"_index": profile_index, "_id": user_id}}))
                    bulk_lines.append(json.dumps(profile_doc, default=serialize_doc))

                    bulk_lines.append(json.dumps({"index": {"_index": user_index, "_id": user_id}}))
                    bulk_lines.append(json.dumps(user_doc, default=serialize_doc))

                ndjson_payload = "\n".join(bulk_lines) + "\n"
                bulk_resp = await es_client.post(
                    "/_bulk",
                    content=ndjson_payload,
                    headers={"Content-Type": "application/x-ndjson"},
                )
                if bulk_resp.status_code not in (200, 201):
                    logger.error("Bulk sync error: %s", bulk_resp.text[:500])
                    break

                last_row = rows[-1]
                last_created_at = last_row["created_at"]
                last_id = last_row["id"]
                synced_count += len(rows)

                logger.info(
                    "Synced %d / %d users to Elasticsearch (%.1f%%)",
                    synced_count,
                    target_sync,
                    (synced_count / target_sync) * 100,
                )
        finally:
            # Restore normal refresh interval and force commit
            logger.info("Restoring refresh interval and refreshing indices...")
            await es_client.put(f"/{profile_index}/_settings", json={"index": {"refresh_interval": "1s"}})
            await es_client.put(f"/{user_index}/_settings", json={"index": {"refresh_interval": "1s"}})
            await es_client.post(f"/{profile_index}/_refresh")
            await es_client.post(f"/{user_index}/_refresh")

        logger.info("Elasticsearch synchronization completed: %d users synced!", synced_count)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(sync_users())
