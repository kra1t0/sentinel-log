from typing import Any, Dict

from pydantic import BaseModel, Field


# contains the input data types through the main log ingestion endpoint
class LogIngestPayload(BaseModel):
    tenant_id: str = Field(
        ...,
        description="Unique organization token used for multi-tenant data isolation boundaries.",
    )
    event_source: str = Field(
        ..., description="The source service or application generating the log"
    )
    event_type: str = Field(
        ...,
        description="The categorized event type signature (e.g., login_failed, unauthorized_access).",
    )
    actor_ip: str = Field(
        ..., description="The structural network origin IP of the logged action."
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="A flexible, non-indexed JSON block for custom log metrics.",
    )
    timestamp: str = Field(
        ...,
        description="Chronological event tracking timestamp strictly formatted in ISO-8601 UTC.",
    )
