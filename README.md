# SentinelLog Technical Specification (Living Document)
High-throughput, multi-tenant SecOps log ingestion pipeline and autonomous containment engine. Built with FastAPI, Kafka, Redis sliding-window analytics, and PostgreSQL row-level security isolation.

## 1. Summary
In modern cloud infrastructures, systems constantly generate gigabytes of log data from web servers, databases, and firewalls. Large enterprises buy massive, expensive tools like Splunk or Datadog and hire teams of 24/7 security analysts to watch those logs for signs of hacking or compromise.

Small-to-mid sized startups cannot afford these tools or the massive human headcount. SentinelLog is designed to bridge this specific gap. It is an automated, lightweight security orchestration system that securely collects logs from multiple client applications, uses fast memory checks to instantly detect suspicious patterns (like brute-force attacks), and brings in an AI engine to analyze complex alerts and generate precise server commands to block the attack, while waiting for a human administrator to click "Approve". And more features to come as we go.. 

## 2. Core Requirements
### 2.1 Functional Requirements

**Log Ingestion Endpoint** A secure HTTP API endpoint that accepts structured JSON log streams from external client applications.
**Live SecOps Dashboard** A realtime web interface where security teams can watch inbound log frequencies and monitor live infrastructure security status.
**Multi-Tenant Isolation Guardrails** A security model ensureing that Company A's clients can only look at Company A's log data and containment settings, completely isolated from company B. ( Or this can be used as one software multiple projects / multiple monitoring dashboards )
**Incident Containment Workflows** An automation framework that registers critical alert states, generates a mitigation strategy, and pauses for a human admin to explicitly confirm or reject the containment action. 

### 2.2 Non Functional Requirements
**High Velocity Non Blocking Ingestion** The endpoint must accept log traffic spikes seamlessly, passing the data off instantly into a queue and responding within milliseconds rather than making the sender wait for a databse query to finish.
**Strict Relational Partitioning** Log data must be locked down tight at the database level using strict row level filters or decoupled customer contexts. 
**Sub Second Alert Propagation** When a security rule breaks, the alert must instantly pop up on the human adminnistrator's dashboard in less than a second.
