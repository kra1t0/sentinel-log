from enum import Enum

from pydantic import BaseModel, Field


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class MitigationStep(BaseModel):
    step_number: int = Field(description="Order of execution for human analysts")
    title: str = Field(
        description="Short summery of the action ( ex: Inspect NAT Gateway Logs )"
    )
    instructions: str = Field(
        description="Detailed human-readable instruction set to mitigation step"
    )
    recommended_policy_changes: str = Field(
        description="Suggested security policy or firewall rule adjustments for long term prevention"
    )


class ThreatAnalysisReport(BaseModel):
    threat_title: str = Field(
        description="A brief title summerizing the detected anomaly"
    )
    incident_overview: str = Field(
        description="Detailed narrative of what happened and why it is a risk, analyzing the event velocity and patterns"
    )
    potential_impact: str = Field(
        description="What could happen if this traffic pattern is left unaddressed (ex: Account takeovers, denial of service etc...)"
    )
    rist_level: RiskLevel = Field(description="Assessed Risk Level")
    confidence_score: float = Field(description="Confidence score between 0.0 - 1.0")
    mitigation_plan: list[MitigationStep] = Field(
        description="Sequential step by step instructions for SOC team to investigate and and resolve the issue"
    )
