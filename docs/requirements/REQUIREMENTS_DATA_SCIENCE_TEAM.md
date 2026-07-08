# Requirements Document – Data Science Team

| | |
|---|---|
| **Document** | Requirements Specification for Anomaly Scoring Model |
| **Project** | Cybersecurity Log Analytics Platform |
| **Version** | 1.0 |
| **Status** | Draft |
| **Author** | Data Science Team |
| **Last updated** | 2026-06-25 |

---

## 1. Purpose

We require a computer-level feature table aggregated from existing security logs over a rolling aggregation window. The current model uses a 7-day window; however, the window length must be configurable. The table serves as input for an anomaly scoring model for daily scoring and monthly retraining based on delivered history.

---

## 2. Application Context

| Aspect | Requirement |
|--------|-------------|
| **Use case** | Detection of unusual computer behavior in the enterprise network (anomaly / threat detection at computer level) |
| **Model status** | We require preprocessed features for scoring. Incremental monthly retraining of the existing model takes place in our application based on delivered history (see ML-IF-2). |
| **Prediction unit** | One computer per aggregation window |
| **Data sources (feature vector)** | Four event streams: **`auth`** (authentications between computers), **`proc`** (process starts and ends), **`flows`** (network traffic), **`dns`** (DNS lookups). Features shall be aggregated per computer from these sources. |
| **Processing mode** | One feature row per computer per completed data day |
| **Aggregation window** | Rolling; all features refer to the same window block. Default for the current model: 7 days. Window length must be configurable. |

---

## 3. Entity and Key Requirements

| ID | Requirement |
|----|-------------|
| **ML-ENT-1** | Each row represents exactly one computer for exactly one aggregation window. |
| **ML-ENT-2** | Primary key: `(computer_id, anchor_day, window_days)` – where `anchor_day` is the last day of the window (anchor day) and `window_days` is the configured window length. |
| **ML-ENT-3** | Every computer that was active in the window in any source (in any role) receives exactly one row – even if it appears in only one source. |
| **ML-ENT-4** | Computers with no activity in any of the four sources within the window must not appear as rows (no null population). |

---

## 4. Feature Overview

The delivered data must contain the following features. Column names, data types, and semantics are binding.

### 4.1 Identification and Window Columns

| Column | Type | Description |
|--------|------|-------------|
| `computer_id` | string | Unique computer key (shared across all sources) |
| `anchor_day` | integer | Day index of the anchor day: `day = (time − 1) // 86400` |
| `window_days` | integer | Aggregation window length in days; must match the window length configured at generation time |

### 4.2 Authentication (`auth`)

Role *source* = outbound, role *destination* = inbound.

| Column | Type | Semantics |
|--------|------|-----------|
| `auth_out_event_count` | integer | Number of auth events (outbound) |
| `auth_in_event_count` | integer | Number of auth events (inbound) |
| `auth_out_distinct_targets` | integer | Distinct target computers (out-degree) |
| `auth_in_distinct_sources` | integer | Distinct source computers (in-degree) |
| `auth_out_distinct_users` | integer | Distinct users total (outbound) |
| `auth_in_distinct_users` | integer | Distinct users total (inbound) |
| `auth_out_distinct_human_users` | integer | Distinct human users (outbound; machine accounts excluded) |
| `auth_in_distinct_human_users` | integer | Distinct human users (inbound) |
| `auth_out_failed_count` | integer | Number of failed auths (outbound) |
| `auth_in_failed_count` | integer | Number of failed auths (inbound) |
| `auth_out_failure_rate` | float / NULL | Share of failed auths (outbound); only over events with known `success` |
| `auth_in_failure_rate` | float / NULL | Share of failed auths (inbound); only over events with known `success` |

### 4.3 Processes (`proc`)

| Column | Type | Semantics |
|--------|------|-----------|
| `proc_start_count` | integer | Number of process starts |
| `proc_distinct_process_names` | integer | Distinct process names |
| `proc_distinct_users` | integer | Distinct users total |
| `proc_distinct_human_users` | integer | Distinct human users |

### 4.4 Network Flows (`flows`)

Network flows frequently appear as identical duplicate rows. Before aggregation, identical flow records must be collapsed so that each unique flow is counted once. All flow features below are computed on this deduplicated basis.

| Column | Type | Semantics |
|--------|------|-----------|
| `flows_out_count_distinct` | integer | Flow count outbound |
| `flows_in_count_distinct` | integer | Flow count inbound |
| `flows_out_distinct_targets` | integer | Distinct target computers (fan-out) |
| `flows_in_distinct_sources` | integer | Distinct source computers (fan-in) |
| `flows_out_bytes_sum_distinct` | integer | Byte sum outbound |
| `flows_in_bytes_sum_distinct` | integer | Byte sum inbound |
| `flows_out_packets_sum_distinct` | integer | Packet sum outbound |
| `flows_in_packets_sum_distinct` | integer | Packet sum inbound |
| `flows_out_anonymized_port_count_distinct` | integer | Flows on anonymized ports (outbound) |
| `flows_in_anonymized_port_count_distinct` | integer | Flows on anonymized ports (inbound) |

### 4.5 DNS (`dns`)

| Column | Type | Semantics |
|--------|------|-----------|
| `dns_lookup_count` | integer | Number of DNS lookups |
| `dns_distinct_resolved_hosts` | integer | Distinct resolved computers |

### 4.6 Quality and Metadata Columns

| Column | Type | Semantics |
|--------|------|-----------|
| `source_present_auth` | boolean | Source `auth` fully delivered for all window days |
| `source_present_proc` | boolean | Source `proc` fully delivered for all window days |
| `source_present_flows` | boolean | Source `flows` fully delivered for all window days |
| `source_present_dns` | boolean | Source `dns` fully delivered for all window days |

The `source_present_*` flags distinguish "true zero activity" from "source not delivered". We require these columns unchanged in the dataset; they must not be replaced by imputation.

---

## 5. Deduplication Requirements

| ID | Requirement |
|----|-------------|
| **ML-DEDUP-1** | Before feature aggregation, duplicate events must be removed so that identical records within a source contribute only once to counters, distinct counts, and sums. |
---

## 6. Missing Value Semantics

| ID | Requirement |
|----|-------------|
| **ML-NULL-1** | Missing counters and sums are represented as `0` (meaning e.g. observed zero activity in the respective source/role). |
| **ML-NULL-2** | Missing ratios (e.g. auth failure rates without underlying auths with known `success`) are represented as `NULL` (undefined; do not impute with `0`). |

---

## 7. Window and Availability Requirements

| ID | Requirement |
|----|-------------|
| **ML-WIN-1** | Aggregation window is rolling; anchor day = each completed data day. Default window length for the current model: 7 days. |
| **ML-WIN-2** | A window partition is delivered to us only when all four sources are fully available for all window days. |
| **ML-WIN-3** | Features refer exclusively to the days of the window, not to the entire history. |
| **ML-WIN-4** | Window length is configurable. Feature rows must be regenerated retroactively for already available event data with a different window length – regardless of the window length used in previous calculations. |

---

## 8. Delivery Format and Interface

| ID | Requirement |
|----|-------------|
| **ML-IF-1** | **Daily partition:** After a data day completes, the partition for `(anchor_day)` is available for operational scoring. |
| **ML-IF-2** | **Monthly retraining delivery:** For incremental retraining of the existing model, we receive a consolidated dataset monthly at a defined delivery path. It contains all daily partitions for the delivery period with `window_days = 7` – the same feature rows as operational scoring (ML-IF-1), bundled across multiple `anchor_day` values. Only volume (historical depth) differs, not feature definition, window length, or aggregation logic. Rows must be identical to the corresponding daily partitions; there is no separate retraining feature set. |
| **ML-IF-3** | **Format:** Columnar file format; must be compatible with pandas, scikit-learn, and PySpark. |
| **ML-IF-4** | **Encryption:** Delivered files are encrypted at rest; decryption takes place in our application using provided keys. |
| **ML-IF-5** | **Delivery manifest:** Each delivery is accompanied by a manifest containing at least: dataset version, schema version, record count, timestamp, checksum (SHA-256). |
| **ML-IF-6** | **Schema conformance:** The delivered schema must match this feature overview exactly; deviations result in rejection of the delivery. |
| **ML-IF-7** | **Idempotency:** Late or repeated data deliveries for individual days must not cause double counting; affected windows must be recalculated consistently. |

---

## 9. Data Quality Requirements

| ID | Requirement | Fit Criterion |
|----|-------------|---------------|
| **ML-NFR-1** | **Determinism** | Identical input data → identical feature rows; verifiable via checksum. |
| **ML-NFR-2** | **Reproducibility** | Historical feature rows must be reproducible on recalculation. |
| **ML-NFR-3** | **Availability for scoring** | Daily partition is available no later than after the batch run for the data day completes. |
| **ML-NFR-4** | **Completeness** | Each partition contains all active computers in the window; record count in the manifest is plausible. |
| **ML-NFR-5** | **Traceability** | Origin of each feature row (source days, schema version, computation timestamp) is documented. |
| **ML-NFR-6** | **Access control** | Delivered data is readable only by our authorized data science team. |

---

## 10. Features Not Required (v1)

We do not require the following features in this version. They must not be added to the serving feature vector without our approval:

| Topic | Rationale |
|-------|-----------|
| Semantic conforming (e.g. merging synonymous auth type variants) | Not yet in the model |
| Categorical breakdowns (e.g. NTLM share, logon type distribution) | Not model input in v1 |
| Population-level rarity features (e.g. network-wide rare processes) | Requires global statistics; future extension |
| Deviation features (latest day vs. prior days as computer baseline) | Potential future feature |
| Streaming / real-time scoring | Batch model; daily scoring cadence sufficient |
| Hour-based features | Not aggregated for v1 model |
| Weekday features | Not derivable (no calendar alignment in the dataset) |

New features are requested via a **new schema version** of this contract.

---
