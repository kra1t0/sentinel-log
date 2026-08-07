import asyncio
from datetime import datetime, timezone

import httpx

INTAKE_URL = "http://127.0.0.1:8000/api/v1/telemetry/ingest"


async def simulate_brute_force():
    async with httpx.AsyncClient() as client:
        print("[*] Blasting 7 failed login attempts to trigger anomaly threshold...\n")

        for i in range(1, 8):
            payload = {
                "tenant_id": "tenant_acme_corp",
                "event_source": "auth-service",
                "event_type": "login_failed",
                "actor_ip": "198.51.100.42",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metadata": {
                    "username": "admin",
                    "attempt_number": i,
                    "user_agent": "Mozilla/5.0 (Hydra-BruteForce-Test)",
                },
            }

            response = await client.post(INTAKE_URL, json=payload)
            print(f"[{i}/7] Sent log -> Status: {response.status_code}")
            await asyncio.sleep(0.2)  # 200ms gap between logs


if __name__ == "__main__":
    asyncio.run(simulate_brute_force())
