# Dataset Description — LANL Cyber Security Dataset

| | |
|---|---|
| **Document** | Dataset Description |
| **Project** | Cybersecurity Log Analytics Platform |
| **Version** | 1.0 |
| **Last updated** | 2026-06-27 |

---

## 1. Overview

The platform uses a subset of the **LANL Comprehensive, Multi-Source
Cyber-Security Events** dataset, released by **Los Alamos National Laboratory (LANL)** (https://csr.lanl.gov/data/cyber1/).
The dataset is a collection of de-identified event logs collected from LANL's
internal enterprise computer network over a contiguous period of 58 consecutive
days. It is a widely used public datasets for research into enterprise
network behavior, lateral-movement detection, and host-level anomaly and threat detection,
which makes it a realistic stand-in for real-world security logs.

The data represents the everyday activity of users and computers in a large corporate
network: people logging on, processes starting and stopping, machines talking to each
other over the network, and name-resolution traffic. Embedded within this normal
activity is a small, labeled set of known malicious (red-team) actions, which makes the
dataset suitable both for building features and for evaluating detection quality.

---

## 2. Key Characteristics

| Property | Description |
|----------|-------------|
| **Origin** | Los Alamos National Laboratory internal corporate network |
| **Time span** | 58 consecutive days of continuous collection |
| **Volume** | Billions of events across all sources |
| **Entities** | Computers (`C###`), users (`U###`), domains (`Dom###`), ports (`N###`) |
| **De-identification** | All identifying values are anonymized to opaque tokens; well-known ports are preserved as-is, other ports are anonymized |
| **Time representation** | Integer seconds since the start of data collection (`time = 1` is the first second); there is no calendar/wall-clock alignment |
| **Labels** | A separate red-team event file marks known malicious authentication events |

### 2.1 Time and day derivation

Time is stored as a monotonically increasing integer count of seconds, starting at
`1`. There is no absolute date and no time zone, so weekday or calendar features cannot be
derived. A day index can be derived directly from the timestamp:

```
day = (time - 1) // 86400
```

### 2.2 Anonymization and duplicates

- Users, computers, and domains are replaced by stable anonymized tokens (e.g. `U24`,
  `C625`, `Dom1`). The same token always refers to the same entity across all files.
- **Well-known ports** (e.g. `80`, `443`) are kept in clear; all other ports are
  anonymized (e.g. `N1`, `N2`).
- **Network flow records frequently appear as exact duplicate rows.** Identical records
  must be de-duplicated before any counting/aggregation so each unique event is counted
  once.

### 2.3 Account types (human vs. machine)

User tokens distinguish human and machine accounts by a trailing `$`: an account whose
name ends in `$` (e.g. `C625$`) is a **machine/computer account**, while accounts without
the suffix (e.g. `U24`) are **human users**. This convention is the basis for the
`*_distinct_human_users` features, which count distinct users after excluding machine
accounts. Well-known system accounts (e.g. `SYSTEM`) are kept verbatim.

---

## 3. Event Sources (Files)

Four event sources describe activity (`auth`, `proc`, `flows`, `dns`). A fifth file,
`redteam`, provides ground-truth labels for known malicious activity.

| File | Source | Approx. scale |
|------|--------|---------------|
| `auth.txt` | Authentication events | ~1.6B rows |
| `proc.txt` | Process start/stop events | ~500M rows |
| `flows.txt` | Network flow records | ~1B rows |
| `dns.txt` | DNS lookup events | ~200M rows |
| `redteam.txt` | Labeled malicious auth events | ~750 rows |

### 3.1 `auth.txt` — Authentication events

Authentication and logon activity between computers (including service/Kerberos/NTLM
authentication). Each row has a **source** computer and a **destination** computer,
capturing the direction of the authentication.

| Field | Description |
|-------|-------------|
| `time` | Event time in seconds |
| `source user@domain` | Authenticating user and domain |
| `destination user@domain` | Target user and domain |
| `source computer` | Computer initiating the authentication |
| `destination computer` | Computer being authenticated to |
| `authentication type` | E.g. Kerberos, NTLM, Negotiate |
| `logon type` | E.g. Network, Interactive, Service, Batch |
| `authentication orientation` | E.g. LogOn, LogOff, TGS, TGT |
| `success/failure` | Whether the authentication **succeeded** or **failed** (may be unknown for some events) |

### 3.2 `proc.txt` — Process events

Process start and stop events on individual computers.

| Field | Description |
|-------|-------------|
| `time` | Event time in seconds |
| `user@domain` | User context of the process |
| `computer` | Computer on which the process ran |
| `process name` | Anonymized process name |
| `start/end` | Whether this is a process **start** or **end** |

### 3.3 `flows.txt` — Network flows

Network communication flows between computers. Records are commonly **duplicated**.

| Field | Description |
|-------|-------------|
| `time` | Flow start time in seconds |
| `duration` | Flow duration in seconds |
| `source computer` | Originating computer |
| `source port` | Source port (well-known kept; otherwise anonymized) |
| `destination computer` | Destination computer |
| `destination port` | Destination port (well-known kept; otherwise anonymized) |
| `protocol` | Network protocol number |
| `packet count` | Number of packets in the flow |
| `byte count` | Number of bytes in the flow |

### 3.4 `dns.txt` — DNS lookups

DNS name-resolution events.

| Field | Description |
|-------|-------------|
| `time` | Event time in seconds |
| `source computer` | Computer making the lookup |
| `computer resolved` | Computer that was resolved |

### 3.5 `redteam.txt` — Red-team labels

A small set of authentication events that are known to be part of red-team
(adversarial) activity. Each row corresponds to a specific malicious authentication and
can be joined back to `auth` events.

| Field | Description |
|-------|-------------|
| `time` | Event time in seconds |
| `user@domain` | Compromised user used in the malicious auth |
| `source computer` | Source computer of the malicious auth |
| `destination computer` | Destination computer of the malicious auth |

---

## 4. Reference

A. D. Kent, “Comprehensive, Multi-Source Cybersecurity Events,”
Los Alamos National Laboratory, http://dx.doi.org/10.17021/1179829, 2015.
