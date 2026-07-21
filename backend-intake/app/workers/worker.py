import asyncio
import json
import logging
import os
from datetime import datetime

import asyncpg
from aiokafka.consumer import AIOKafkaConsumer, consumer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s - %(message)s"
)
logger = logging.getLogger("sentinel_worker")

# Infra configss
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL", "redpanda:19092")
# kafka 19092 is used for external work
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://sentinel:somepass@postgres:5432/sentinel_db",
)
TOPIC_NAME = "telemetry-raw-logs"
CONSUMER_GROUP = "sentinel-postgres-writers"


async def process_batch(pool: asyncpg.Pool, records: list):
    """parses and batch inserts telemtry logs into PostgreSQL."""
    insert_query = """
    INSERT INTO security_logs(tenant_id, event_source, event_type, actor_ip, metadata, timestamp)
    VALUES($1, $2, $3, $4, $5, $6);
    """
    rows_to_insert = []

    for record in records:
        try:
            payload = json.loads(record.value.decode("utf-8"))

            # ISO UTC long format to python datetime object
            timestamp_dt = datetime.fromisoformat(
                payload["timestamp"].replace("Z", "+00:00")
            )

            rows_to_insert.append(
                (
                    payload["tenant_id"],
                    payload["event_source"],
                    payload["event_type"],
                    payload["actor_ip"],
                    json.dumps(payload.get("metadata", {})),
                    timestamp_dt,
                )
            )

        except Exception as oopsError:
            logger.error(
                f"Failed to decode message offset {record.offset}: {oopsError}"
            )
            continue
    if rows_to_insert:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(insert_query, rows_to_insert)
        logger.info(
            f"Successfully persisted batch of {len(rows_to_insert)} security logs to Postgresql"
        )


async def start_consumer():
    logger.info("Initializing SentinelLog Background Consumer Worker...")

    # 1. Connect to postgres connection pool
    pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=2, max_size=10)
    logger.info("Database connection pool established.")

    # 2. Connect to our panda consumer
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info(f"Subscribed to topic '{TOPIC_NAME}'. Waiting for telemetry stream..")

    try:
        while True:
            batch_data = await consumer.getmany(timeout_ms=1000, max_records=200)

            for topic_partition, records in batch_data.items():
                if records:
                    await process_batch(pool, records)
                    # commit offsets only after successful DB persistence
                    await consumer.commit({topic_partition: records[-1].offset + 1})
    except asyncio.CancelledError:
        logger.info("Shutdown signal received///")
    finally:
        await consumer.stop()
        await pool.close()
        logger.info("Consumer worker cleanly shut down")


if __name__ == "__main__":
    try:
        asyncio.run(start_consumer())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")
