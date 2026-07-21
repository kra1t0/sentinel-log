import json
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request, status

from app.models.models import LogIngestPayload

router = APIRouter()


@router.get("/health")
async def health_check():
    return {"status": "healthy"}


@router.post("/api/v1/telemetry/ingest", status_code=status.HTTP_202_ACCEPTED)
async def ingest_telemetry(payload: LogIngestPayload, request: Request):
    try:
        # retrieve the connection pool from global app state
        producer = getattr(request.app.state, "producer", None)
        if not producer:
            raise HTTPException(status_code=503, detail="Streming engine unavailable")

        # pydantic object to raw JSON bytes
        serialized_data = json.dumps(payload.model_dump()).encode("utf-8")

        # Fire and Forget technique
        await producer.send_and_wait("telemetry-raw-logs", serialized_data)

        return {
            "status": "accepted",
            "message": "Log signature validated successfully and queued.",
            "received_at": datetime.now().isoformat().replace("+00:00", "Z"),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An ingestion failure occurred: {str(e)}",
        )
