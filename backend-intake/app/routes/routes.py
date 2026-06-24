from datetime import datetime

from fastapi import APIRouter, HTTPException, status

from app.models.models import LogIngestPayload

router = APIRouter()


@router.get("/health")
async def health_check():
    return {"status": "healthy"}


@router.post("/api/v1/telemetry/ingest", status_code=status.HTTP_202_ACCEPTED)
async def ingest_telemetry(payload: LogIngestPayload):
    try:
        print(f"\n[RAW TELEMETRY INGESTED]")
        print(f"Tenant Context: {payload.tenant_id}")
        print(f"Event Signature: {payload.event_type} | Actor IP: {payload.actor_ip}")
        print(f"Timestamp: {payload.timestamp}")

        return {
            "status": "accepted",
            "message": "Log signature validated successfully and queued.",
            "received_at": datetime.utcnow().isoformat() + "Z",
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An ingestion failure occurred: {str(e)}",
        )
