# Requirements Specification — System Owner (Non-Functional Requirements & Constraints)

| | |
|---|---|
| **Document** | System Owner Requirements — Non-Functional Requirements & Constraints |
| **Project** | Cybersecurity Log Analytics Platform |
| **Version** | 0.1 |
| **Status** | Draft |
| **Last updated** | 2026-06-27 |
| **Owner** | Platform owner |

---

## 1. Purpose

This document specifies the durable non-functional requirements and constraints that the
Cybersecurity Log Analytics Platform must satisfy, as defined by the
platform owner. These hold across iterations and are independent of any single
release or demonstration scope.

---

## 2. ID scheme

| Prefix | Category |
|--------|----------|
| `QR-`  | Quality / non-functional requirement |
| `CR-`  | Constraint (mandated technology or method) |

---

## 3. Quality (non-functional) requirements

> The "how well" — each requirement includes a measurable fit criterion.

| ID | Requirement | Fit criterion |
|----|-------------|---------------|
| **QR-1** | *Reliability* — The system shall employ techniques ensuring reliable batch operation. | Every scheduled batch run is idempotent and re-runnable (no double counting on rerun, per ML-IF-7). On failure the orchestrator retries automatically (≥3 attempts with exponential backoff); a run that fails must recover and complete before the next scheduled run window. After all retries are exhausted, a structured failure alert is emitted to logs / a monitored channel. Demonstrated by injecting a transient failure into one run and showing automatic recovery to correct output. |
| **QR-2** | *Scalability* — The system shall be designed to scale with growing data volume. | The full 14-day demonstration subset of the LANL Cyber Security Dataset (all four sources at full per-day volume) is processed once during an initial backfill. Thereafter, a single 7-day-window feature-partition run — the unit delivered to the ML platform — completes in under 10 minutes locally on hardware with ≥8 GB RAM and ≥4 cores. Scale headroom shall be demonstrable by adjusting the processing engine's parallelism and memory configuration only, without architectural or code changes. |
| **QR-3** | *Maintainability* — The system shall be maintainable through clear separation of concerns and modularity. | Each microservice has a dedicated container build, environment-variable-based configuration (no hardcoded values), and a README. A linter/formatter is configured per service and passes in CI, and the build/test pipeline runs green. Service decomposition is documented (CR-2). |
| **QR-4** | *Security* — The system shall protect itself and its data against unauthorized access. | (1) Only required ports published to the host. (2) Inter-service communication on isolated container networks; the metadata catalog and internal datasets reside on networks reachable only by authorized services (access enforced by network isolation, not application-level authz). (3) No credentials or secrets in container images, code, or git history — supplied at runtime via gitignored env files / container secrets. (4) Delivered feature output not world-readable at the filesystem level. |
| **QR-5** | *Data governance* — The system shall apply data governance practices. | The metadata catalog records: (1) data lineage across all processing stages (ingestion → processing → delivery), (2) versioned schemas per stage, (3) delivery manifests (dataset version, schema version, timestamp, record count, SHA-256) for each delivery (aligns with ML-IF-5), (4) a documented access policy — which services may read or write each dataset (enforcement via QR-4 network isolation). |
| **QR-6** | *Data protection* — The system shall handle data in a privacy- and integrity-preserving way. | (1) SHA-256 checksums computed and validated at defined stage checkpoints (raw landing, post-transformation, delivery), stored in the metadata catalog; the delivery checksum is mandatory and carried in the manifest. (2) All transformations logged with input/output record counts, schema version, job ID, and timestamp. (3) Delivered feature files encrypted at rest before delivery; internal intermediate datasets are unencrypted internal artifacts. |
| **QR-7** | *Reproducibility* — Everything from design to build shall be reproducible. | The full platform (all containers, volumes, networks) starts from a single startup command on any host with a compatible container runtime installed, with no additional host configuration. Output is deterministic on a fixed container image and platform: identical input produces identical output, verifiable via QR-6 checksums. Cross-OS/architecture bit-identity is not guaranteed (floating-point features such as `auth_*_failure_rate`); this caveat is documented. |

---

## 4. Constraints

> Mandated technologies and methods.

| ID | Requirement |
|----|-------------|
| **CR-1** | *IaC* — Infrastructure shall be defined as code. A container orchestration configuration file is the primary IaC artifact for the local deployment. All service definitions, volumes, and networks are declared there. |
| **CR-2** | *Microservices* — The system shall be composed of isolated, independent microservices that interoperate while remaining decoupled. The service decomposition shall be designed and documented as a first-step project deliverable. |
| **CR-3** | *Containerization* — Components shall be containerized using official base images from a public container registry, modified minimally where necessary. Custom images shall be built via a CI/CD pipeline and published to a container registry. Local builds must also remain possible. |
| **CR-4** | *Version control* — All artifacts (design through build) shall be version-controlled in a git repository. This includes container configurations, pipeline code, and the schema registry. |
| **CR-5** | *State-of-the-art* — Solutions shall favor current best-practice concepts. |
