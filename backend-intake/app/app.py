import uvicorn
from fastapi import FastAPI

from app.routes.routes import router

app = FastAPI(
    title="SentinelLog Ingestion Gateway",
    description="High-Throughput decoupled log intake engine for multi-tenant SecOps automation",
    version="1.0.0",
)

app.include_router(router)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
