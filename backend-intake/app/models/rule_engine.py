import asyncio
import logging
import sys
import time

import asyncpg
import redis.asyncio as aioredis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("sentinel_rule_engine")

SLIDING_WINDOW_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window_start = tonumber(ARGV[2])
local event_id = ARGV[3]
local ttl = tonumber(ARGV[4])

-- STEP 1 : Remove events older than window_start
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- STEP 2 : Add the current event timestamp as the score
redis.call('ZADD', key, now, event_id)

-- STEP 3 : Count remaining events in the window
local count = redis.call('ZCARD', key)

-- STEP 4 : Keep key alive in redis memory only as long as window duration
redis.call('EXPIRE', key, ttl)

return count
"""


class DynamicRuleEngine:
    def __init__(self, postgres_dsn: str, redis_url: str):
        self.postgres_dsn = postgres_dsn
        self.redis_url = redis_url

        self.redis_client: aioredis.Redis | None = None
        self.lua_script_sha: str | None = None

        self.rules_cache: dict[str, list[dict]] = {}

    async def initialize(self):
        """Init Redis connection, registers Lua script, boots Pub/Sub listener"""
        self.redis_client = aioredis.from_url(self.redis_url, decode_responses=True)
        self.lua_script_sha = await self.redis_client.script_load(SLIDING_WINDOW_LUA)
        logger.info("[ENGINE INIT] Redis connection & Lua atomic scripts registered")
        await self.reload_rules_from_db()

    async def reload_rules_from_db(self):
        """Queries PostGreSQL for active rules and populates local cache"""
        query = """
            SELECT rule_id, tenant_id, rule_name, event_type, time_window_seconds,
                   max_events_allowed, cooldown_seconds, severity, group_by_field
            FROM tenant_rules
            WHERE is_enabled = TRUE;
        """
        try:
            conn = await asyncpg.connect(self.postgres_dsn)
            rows = await conn.fetch(query)
            await conn.close()

            new_cache: dict[str, list[dict]] = {}
            for row in rows:
                key = f"{row['tenant_id']}:{row['event_type']}"
                rule_dict = dict(row)
                rule_dict["rule_id"] = str(rule_dict["rule_id"])

                if key not in new_cache:
                    new_cache[key] = []
                new_cache[key].append(rule_dict)

            self.rules_cache = new_cache
            logger.info(
                f"[RULE CACHE] Reloaded {len(rows)} active rules into worker memory"
            )
        except Exception as e:
            logger.error(f"[RULE CACHE ERROR] Failed to fetch rules from DB: {e}")

    async def listen_for_rule_updates(self):
        """Background worker listening Redis Pub/Sub for live admin rule changes"""
        if self.redis_client is None:
            logger.error(
                "[PUBSUB ERROR] Cannot start listener: Redis client is not initialized!"
            )
            return

        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe("sentinel:rule_updates")
        logger.info("[PUBSUB] Listening for live rule modification signals...")

        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    logger.info(
                        f"[PUBSUB SIGNAL] Received rule update signal: {message['data']}. Invalidating cache..."
                    )
                    await self.reload_rules_from_db()
        except asyncio.CancelledError:
            await pubsub.unsubscribe("sentinel:rule_updates")

    async def evaluate_log(self, log_payload: dict) -> dict | None:
        """Evaluates a single telemetry log against matched active rules."""
        if self.redis_client is None:
            logger.error(
                "[ENGINE ERROR] Cannont evaluate log: Redis Client is not initialized!"
            )
            return None

        if self.lua_script_sha is None:
            logger.error(
                "[ENGINE ERROR] Cannot evaluate log: Lua script SHA is not loaded!"
            )
            return None

        tenant_id = log_payload.get("tenant_id")
        event_type = log_payload.get("event_type")

        lookup_key = f"{tenant_id}:{event_type}"
        matching_rules = self.rules_cache.get(lookup_key, [])

        if not matching_rules:
            return None

        now_ms = int(time.time() * 1000)

        for rule in matching_rules:
            group_field = rule.get("group_by_field", "actor_ip")
            if group_field == "actor_ip":
                entity_value = log_payload.get("actor_ip")
            else:
                entity_value = log_payload.get("metadata", {}).get(
                    group_field.replace("metadata.", ""), "unknown"
                )

            rule_id = rule["rule_id"]
            cooldown_seconds = rule["cooldown_seconds"]

            cooldown_key = f"sentinel:cooldown:{rule_id}:{entity_value}"
            zset_key = f"sentinel:window:{rule_id}:{entity_value}"

            is_muted = await self.redis_client.exists(cooldown_key)
            if is_muted:
                continue

            window_ms = rule["time_window_seconds"] * 1000
            window_start_ms = now_ms - window_ms
            log_id = log_payload.get("id", str(now_ms))

            current_count = await self.redis_client.evalsha(
                self.lua_script_sha,
                1,
                zset_key,
                str(now_ms),
                str(window_start_ms),
                str(log_id),
                str(rule["time_window_seconds"]),
            )

            if current_count > rule["max_events_allowed"]:
                await self.redis_client.set(cooldown_key, "1", ex=cooldown_seconds)

                logger.warning(
                    f"[ANOMALY DETECTED] Rule '{rule['rule_name']}' breached by {entity_value}!... {current_count}/{rule['max_events_allowed']} in {rule['time_window_seconds']}s"
                )
                return {
                    "anomaly_id": f"anom_{now_ms}",
                    "rule_id": rule_id,
                    "rule_name": rule["rule_name"],
                    "tenant_id": tenant_id,
                    "severity": rule["severity"],
                    "offending_entity": entity_value,
                    "group_by_field": group_field,
                    "event_count": current_count,
                    "time_window_seconds": rule["time_window_seconds"],
                    "trigger_log": log_payload,
                    "detected_at": log_payload.get("timestamp"),
                }
        return None
