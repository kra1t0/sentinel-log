import os
from contextlib import asynccontextmanager
# from sre_parse import State

from aiokafka.errors import KafkaConnectionError
from typing_extensions import AsyncIterator
import uvicorn
import asyncio
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI

from app.routes.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # when container boots up
    broker_url = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL", "localhost:9092")
    print(f"[SYSTEM INIT] Connecting to streaming engine at {broker_url}")

    producer = AIOKafkaProducer(bootstrap_servers=broker_url)

    try:
        retries = 5
        delay = 2

        for attempt in range(1, retries + 1):
            try:
                await producer.start()
                print("[SYSTEM INIT] Successfuly connected to streaming engine.")
                break
            except KafkaConnectionError as kafkaconerr:
                if attempt == retries:
                    print(f"[SYSTEM ERROR] Failed to connect after {retries} of retries")
                    raise kafkaconerr
                print(f"[SYSTEM WARN] Connection failed (Attempt {attempt}/{retries}). Retrying in {delay}s...")
                await asyncio.sleep(delay)
                delay = delay * 2
        app.state.producer = producer
        yield
    finally:
        print("[SYSTEM TEARDOWN] Flushing streams and closing connections...")
        await producer.stop()

app = FastAPI(
    title="SentinelLog Ingestion Gateway",
    description="High-Throughput decoupled log intake engine for multi-tenant SecOps automation",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
