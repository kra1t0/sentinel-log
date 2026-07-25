import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from math import prod
from shutil import ExecError

import asyncpg
from aiokafka.consumer import AIOKafkaConsumer, consumer
from aiokafka.producer.producer import AIOKafkaProducer
from aiokafka.protocol import produce
from app.models import rule_engine
from app.models.rule_engine import DynamicRuleEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("sentinel_worker")

# Infra configss
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL", "redpanda:19092")
# kafka 19092 is used for external work
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://sentinel:somepass@postgres:5432/sentinel_db",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://sentinel-redis:6379/0")

TOPIC_NAME = "telemetry-raw-logs"
DLQ_TOPIC = "telemetry-dlq"
CONSUMER_GROUP = "sentinel-postgres-writers"
ANOMALY_TOPIC = "telemetry-anomalies"


async def send_to_dlq(producer: AIOKafkaProducer, raw_value: bytes, error_reason: str):
    """Routes corrupted payloads to the Dead Letter Queue DLQ"""
    try:
        timestamp_dt = datetime.now(timezone.utc)
        payload = {
            "error": error_reason,
            "raw_payload": raw_value.decode("utf-8", errors="replace"),
            "failed_at": timestamp_dt.isoformat().replace("+00:00", "Z"),
        }
        await producer.send_and_wait(DLQ_TOPIC, json.dumps(payload).encode("utf-8"))
        logger.warning(
            f"[DLQ] Routed poison pill message to '{DLQ_TOPIC}', Reason: {error_reason}"
        )
    except Exception as e:
        logger.critical(f"[DLQ FAILURE] Failed to write to DLQ: {e}")


def sanitize_and_parse(record_bytes: bytes) -> tuple:
    """Parses incoming JSON bytes abd ensures data structures match the DB schema"""
    payload = json.loads(record_bytes.decode("utf-8"))

    # Extract or Gen unique log ID
    log_id = payload.get("id")
    if not log_id:
        log_id = str(uuid.uuid4())

    tenant_id = str(payload["tenant_id"])
    event_source = str(payload["event_source"])
    event_type = str(payload["event_type"])
    actor_ip = str(payload["actor_ip"])

    # Safeguard the Metadata: it should be a dict before serialization
    metadata = payload.get("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {"raw_metadata": str(metadata)}

    timestamp_dt = datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))
    return (
        log_id,
        tenant_id,
        event_source,
        event_type,
        actor_ip,
        json.dumps(metadata),
        timestamp_dt,
    )


async def execute_db_batch_with_retry(
    pool: asyncpg.Pool, records: list, max_retries: int = 5
):
    """parses and batch inserts telemtry logs into PostgreSQL with Exponential Backoff circuit breaking."""
    insert_query = """
    INSERT INTO security_logs(id, tenant_id, event_source, event_type, actor_ip, metadata, timestamp)
    VALUES($1, $2, $3, $4, $5, $6, $7) ON CONFLICT(id) DO NOTHING;
    """
    delay = 2
    for attempt in range(1, max_retries + 1):
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(insert_query, records)
                return True
        except (asyncpg.PostgresError, OSError) as dberr:
            logger.error(
                f"[DB OUTAGE] Database write failed (Attempt {attempt}/{max_retries}) : {dberr}"
            )
            if attempt == max_retries:
                raise dberr
            await asyncio.sleep(delay)
            delay *= 2


async def start_consumer():
    logger.info("Initializing SentinelLog Background Consumer Worker...")

    # 1. Connect to postgres connection pool
    pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=2, max_size=10)
    logger.info("Database connection pool established.")
    # Init the worker's personal DLQ producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    logger.info("DLQ Kafka Producer engine started..")

    # 2. Connect to our panda consumer
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    rule_engine = DynamicRuleEngine(postgres_dsn=POSTGRES_DSN, redis_url=REDIS_URL)
    await rule_engine.initialize()  # wait for the initialization
    asyncio.create_task(rule_engine.listen_for_rule_updates())

    await consumer.start()
    logger.info(f"Subscribed to topic '{TOPIC_NAME}'. Waiting for telemetry stream..")

    try:
        while True:
            batch_data = await consumer.getmany(timeout_ms=1000, max_records=200)
            if not batch_data:
                continue
            rows_to_execute = []
            offsets_to_commit = {}
            records_in_batch = []

            for topic_partition, records in batch_data.items():
                for record in records:
                    try:
                        parsed_row, dict_payload = sanitize_and_parse(record.value)
                        rows_to_execute.append(parsed_row)
                        records_in_batch.append(record)

                        # check if there are anomalies
                        anomaly_payload = await rule_engine.evaluate_log(dict_payload)

                        if anomaly_payload:
                            # sanitize
                            santized_anomaly_payload = json.dumps(
                                anomaly_payload
                            ).encode("utf-8")
                            # publish anomaly to Kafka dedicated ANOMALY_TOPIC, for AI Evals
                            await producer.send_and_wait(
                                ANOMALY_TOPIC, santized_anomaly_payload
                            )
                            logger.info(
                                f"X [ ANOMALY EMITED ] Sent anomaly alert to {ANOMALY_TOPIC}"
                            )

                    except Exception as err:
                        # Fixed variable scoping: producer is now safely accessible here
                        await send_to_dlq(producer, record.value, str(err))
                if records:
                    offsets_to_commit[topic_partition] = records[-1].offset + 1

            # Batch insert into postgres with retry resilience
            if rows_to_execute:
                try:
                    await execute_db_batch_with_retry(pool, rows_to_execute)
                    logger.info(
                        f"Successfully persisted batch of {len(rows_to_execute)} security logs"
                    )
                except asyncpg.DataError as data_err:
                    logger.error(
                        f"[BAD DATA IN BATCH] Demultiplexing batch due to validation failure: {data_err}"
                    )

                    # Forces each record individually to isolate the single bad row
                    for rec in records_in_batch:
                        try:
                            single_row, _ = sanitize_and_parse(rec.value)
                            await execute_db_batch_with_retry(
                                pool, [single_row], max_retries=1
                            )
                        except (asyncpg.DataError, Exception) as single_err:
                            await send_to_dlq(
                                producer,
                                rec.value,
                                f"DB Type Casting Failure: {single_err}",
                            )

            if offsets_to_commit:
                await consumer.commit(offsets_to_commit)
    except asyncio.CancelledError:
        logger.info("Shutdown signal received///")
    finally:
        logger.info("Cleaning up resources...")
        _ = asyncio.create_task(rule_engine.listen_for_rule_updates()).cancel()
        logger.info("Rule change listener ---> closed")
        await consumer.stop()
        logger.info("Consumer worker ---> Stopped")
        await producer.stop()
        logger.info("Producer worker ----> Stopped")
        await pool.close()
        logger.info("Postgress DB pool ----> closed")
        logger.info("Clean Shutdown process complete...")


if __name__ == "__main__":
    try:
        asyncio.run(start_consumer())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")
