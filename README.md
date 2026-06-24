# SentinelLog Technical Specification (Living Document)
High-throughput, multi-tenant SecOps log ingestion pipeline and autonomous containment engine. Built with FastAPI, Kafka, Redis sliding-window analytics, and PostgreSQL row-level security isolation.

## 1. Summary
In modern cloud infrastructures, systems constantly generate gigabytes of log data from web servers, databases, and firewalls. Large enterprises buy massive, expensive tools like Splunk or Datadog and hire teams of 24/7 security analysts to watch those logs for signs of hacking or compromise.

Small-to-mid sized startups cannot afford these tools or the massive human headcount. SentinelLog is designed to bridge this specific gap. It is an automated, lightweight security orchestration system that securely collects logs from multiple client applications, uses fast memory checks to instantly detect suspicious patterns (like brute-force attacks), and brings in an AI engine to analyze complex alerts and generate precise server commands to block the attack, while waiting for a human administrator to click "Approve". And more features to come as we go.. 

## 2. Core Requirements
### 2.1 Functional Requirements

- **Log Ingestion Endpoint** A secure HTTP API endpoint that accepts structured JSON log streams from external client applications.
- **Live SecOps Dashboard** A realtime web interface where security teams can watch inbound log frequencies and monitor live infrastructure security status.
- **Multi-Tenant Isolation Guardrails** A security model ensureing that Company A's clients can only look at Company A's log data and containment settings, completely isolated from company B. ( Or this can be used as one software multiple projects / multiple monitoring dashboards )
- **Incident Containment Workflows** An automation framework that registers critical alert states, generates a mitigation strategy, and pauses for a human admin to explicitly confirm or reject the containment action. 

### 2.2 Non Functional Requirements
- **High Velocity Non Blocking Ingestion** The endpoint must accept log traffic spikes seamlessly, passing the data off instantly into a queue and responding within milliseconds rather than making the sender wait for a databse query to finish.
- **Strict Relational Partitioning** Log data must be locked down tight at the database level using strict row level filters or decoupled customer contexts. 
- **Sub Second Alert Propagation** When a security rule breaks, the alert must instantly pop up on the human adminnistrator's dashboard in less than a second.

## 3 Core Architecure: Compoenent breakdown
### 3.1 Current directory structure for Phase 1 (Ingestion Gateway Node):
```
.
└── sentinel-log/
    ├── backend-intake/
    │   ├── app/
    │   │   ├── __init__.py
    │   │   ├── app.py             # Application bootstrap & FastAPI initialization
    │   │   ├── models/
    │   │   │   ├── __init__.py
    │   │   │   └── models.py      # Pydantic data validation layer schema contracts
    │   │   └── routes/
    │   │       ├── __init__.py
    │   │       └── routes.py      # Ingestion routing definitions
    │   ├── Dockerfile             # Container definitions targeting lean runtimes
    │   └── requirements.txt       # Hardlocked python microservice dependencies
    ├── docker-compose.yml         # Containerized local environment orchestrator
    ├── LICENSE
    └── README.md
```
### 3.2 Ingestion Gateway API Verification
The isolated intake controller accepts asynchronously queued security telemetry logs directly from decoupled applications or infrastructure components. 

* Ingestion Endpoint: POST http://127.0.0.1:8000/api/v1/telemetry/ingest
* Transport Header: Content-Type: application/json

Sample Ingest Client Payload Envelope:
```
{
  "tenant_id": "tenant-secops-99",
  "event_type": "unauthorized_access",
  "actor_ip": "192.168.1.45",
  "metadata": {
    "severity": "CRITICAL",
    "system_id": "auth-srv-01"
  },
  "timestamp": "2026-06-24T22:00:00Z"
}
```

Sample Gateway Server Response (202 Accepted):
```
{
  "status": "accepted",
  "message": "Log signature validated successfully and queued.",
  "received_at": "2026-06-24T17:21:14.176879Z"
}
```

### 3.3 Strict Secure Coding Foundations
To ensure full defensive security across high-throughput data processing phases, every component integrated into this software repository must adhere to the following strict paradigms:

A. Input Form Invalidation Boundaries
- Rule: No raw, unvalidated, or unstructured byte frames are permitted deeper inside internal microservice logic.
- Implementation: Strict payload structures are mapped at the controller boundary using Pydantic typing. Malformed telemetry schemas are blocked instantly at the API gateway layer, auto-rejecting threats or runtime memory panics before hitting background brokers.

B. SQL and Command Injection Countermeasures
- Rule: Variable payloads such as event descriptions, parameters, and metadata strings must be evaluated as toxic vectors.
- Implementation: No component throughout this code ecosystem compiles raw dynamic string inputs into execution tracks. Interaction with storage systems or database engines must exclusively utilize explicit parameterized variables and sanitized Object-Relational Mappers (ORMs).

C. Cryptographic Tenant Leak Prevention
- Rule: A tenant identification key transmitted purely inside a raw client request body payload cannot be single-handedly trusted to map multi-tenant isolation borders.
- Implementation: As authentication mechanisms expand, client requests will require structured header tokens. The ingestion gateway will evaluate and decode the cryptographic signature, determine the validated operational scope, and programmatically override internal identity fields to guarantee bulletproof workspace segregation.
