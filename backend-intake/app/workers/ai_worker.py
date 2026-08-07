import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone

import asyncpg
from aiokafka.consumer import AIOKafkaConsumer
from app.services.ai_analyzer import ThreatAnalysisReport, ThreatAnalyzerService
from app.workers.worker import KAFKA_BOOTSTRAP, POSTGRES_DSN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("sentinel_ai_worker")


# KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL", "redpanda:9092")
# POSTGRES_DSN = os.getenv("POSTGRES_DSN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

ANOMALY_TOPIC = "telemetry-anomalies"
CONSUMER_GROUP = "sentinel-ai-analysis-group"


async def save_incident_to_db(
    pool: asyncpg.Pool, anomaly: dict, report: ThreatAnalysisReport
):
    """
    Persists the structured threat report and anomaly payload into PostgreSQL
    """
    insert_query = """
    INSERT INTO security_incidents(anomaly_id, tenant_id, rule_name, severity,
    offending_entity, threat_title, incident_overview, potential_impact, risk_level,
    confidence_score, mitigation_plan, raw_anomaly_payload)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb, $13::jsonb);
    """
    rule_id = anomaly.get("rule_id")
    rule_uuid = None
    if rule_id:
        import uuid

        rule_uuid = uuid.UUID(rule_id)

    mitigation_json = json.dumps([step.model_dump() for step in report.mitigation_plan])

    async with pool.acquire() as conn:
        await conn.execute(
            insert_query,
            anomaly.get("anomaly_id"),
            anomaly.get("tenant_id"),
            rule_uuid,
            anomaly.get("rule_name", "Unknown rule"),
            anomaly.get("severity", "HIGH"),
            report.threat_title,
            report.incident_overview,
            report.potential_impact,
            report.rist_level.value,
            report.confidence_score,
            mitigation_json,
            json.dumps(anomaly),
        )


async def start_ai_worker():
    logger.info("Initializing SentinelLog AI Incident Analysis worker")
    # postgres pool
    pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=5)

    # google-genai
    ai_service = ThreatAnalyzerService(api_key=GEMINI_API_KEY)

    # redpanda consumer for real time breach notifications
    consumer = AIOKafkaConsumer(
        ANOMALY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info(f"AI Worker listening on {ANOMALY_TOPIC}")

    try:
        # Main consumption loop
        async for msg in consumer:
            try:
                anomaly_payload = json.loads(msg.value.decode("utf-8"))
                logger.info(
                    f"Received anomaly alert {anomaly_payload.get('anomaly_id')}"
                )
                logger.info(f"for entity {anomaly_payload.get('offending_entity')}")

                # Call gemini service and generate report
                report = await ai_service.analyze_anomaly_with_ai(anomaly_payload)

                # to postgres
                await save_incident_to_db(
                    pool=pool, anomaly=anomaly_payload, report=report
                )
                logger.info(
                    f"Persisted security incident report to DB - {report.threat_title}"
                )

                # commit kafka offset
                await consumer.commit()
            except Exception as e:
                logger.error(f"Error processing anomaly message '{e}'")
                await asyncio.sleep(2)

    except asyncio.CancelledError:
        logger.error("AI worker shutting down gracefully.")
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(start_ai_worker())
    except:
        logger.info("AI worker stopped by user")
