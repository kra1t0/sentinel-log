import logging
import os
import sys
from turtle import mode

from aiokafka.protocol import api
from app.models.anomaly_analysis import ThreatAnalysisReport
from google import genai

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("sentinel_ai_analyzer")


SYSTEM_PROMPT = """
You are SentinelLog's Lead SOC Incident Response AI.
Your role is to analyze incoming security anomaly alerts and enrich them with clear diagnostic insights for human security teams.

Analyze the telemetry anomaly context, evaluate the event velocity, explain the potential threat, and output clear, step-by-step investigation and mitigation instructions.

Do NOT attempt to write raw executable scripts or terminal syntax. Focus on explaining:
1. WHAT happened (incident overview).
2. WHY it is a risk (potential impact).
3. HOW the human security team should investigate and remediate it (mitigation plan).
"""


class ThreatAnalyzerService:
    def __init__(self, api_key: str | None) -> None:
        key = api_key or os.getenv("GEMINI_API_KEY")
        if not key:
            raise ValueError("Gemini API key not found in the environment")
        self.client = genai.Client(api_key=key)

    async def analyze_anomaly_with_ai(
        self, anomaly_payload: dict
    ) -> ThreatAnalysisReport:
        """
        Sends the anomaly context to Gemini Flash and parses a structured threat report.
        """
        prompt = f"""
                ANOMALY ALERT TRIGGERED:
                - Rule Name: {anomaly_payload.get("rule_name")}
                - Severity: {anomaly_payload.get("severity")}
                - Tenant ID: {anomaly_payload.get("tenant_id")}
                - Offending Entity: {anomaly_payload.get("offending_entity")}
                - Event Count: {anomaly_payload.get("event_count")} events in {anomaly_payload.get("time_window_seconds")} seconds
                - Triggering Log Details: {anomaly_payload.get("trigger_log")}

                Perform a risk assessment and generate a structured containment plan.
                """
        try:
            # call gemini man
            response = self.client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config={
                    "system_instruction": SYSTEM_PROMPT,
                    "response_mime_type": "application/json",
                    "response_schema": ThreatAnalysisReport.model_json_schema(),
                },
            )
            report = ThreatAnalysisReport.model_validate_json(response.text)
            logger.info(
                f" Successfully generated Threat Analysis for entity: {anomaly_payload.get('offending_entity')}"
            )
            return report
        except Exception as e:
            logger.error(f"Failed to generate AI response : {e}")
            raise e
