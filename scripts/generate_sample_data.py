#!/usr/bin/env python3
"""Generate a tiny, deterministic, LANL-shaped synthetic dataset.

The real LANL dataset is billions of rows; for the v1 end-to-end skeleton we
only need a handful of days and computers so a full pipeline run completes in
seconds. Output files are header-less CSV, mirroring the source-native format
described in ``docs/dataset/DATASET_DESCRIPTION.md``.

Time is an integer second count starting at 1, so::

    day = (time - 1) // 86400

Outputs one file per source (``auth.txt``, ``proc.txt``, ``flows.txt``,
``dns.txt``) into the target directory.
"""

import argparse
import os
import random

SECONDS_PER_DAY = 86400
DAYS = 4  # day indices 0..3
N_COMPUTERS = 10  # C1..C10
SEED = 1337

AUTH_TYPES = ["Kerberos", "NTLM", "Negotiate"]
LOGON_TYPES = ["Network", "Interactive", "Service", "Batch"]
ORIENTATIONS = ["LogOn", "LogOff", "TGS", "TGT"]
SUCCESS_VALUES = ["Success", "Fail", "?"]
PROC_NAMES = [f"P{i}" for i in range(1, 9)]
WELL_KNOWN_PORTS = ["80", "443", "53"]
ANON_PORTS = [f"N{i}" for i in range(1, 8)]


def computers() -> list[str]:
    """Return the list of computer tokens (``C1``..``C10``)."""
    return [f"C{i}" for i in range(1, N_COMPUTERS + 1)]


def human_users() -> list[str]:
    """Return the list of human user tokens (``U1``..``U6``)."""
    return [f"U{i}" for i in range(1, 7)]


def machine_accounts(comps: list[str]) -> list[str]:
    """Return machine/computer account tokens (trailing ``$``)."""
    return [f"{c}$" for c in comps[:4]]


def t_for_day(rng: random.Random, day: int) -> int:
    """Return a random timestamp (seconds) within the given day index."""
    return day * SECONDS_PER_DAY + rng.randint(1, SECONDS_PER_DAY)


def gen_auth(
    rng: random.Random,
    comps: list[str],
    users: list[str],
    machines: list[str],
) -> list[list]:
    """Generate synthetic authentication events for all days."""
    rows = []
    for day in range(DAYS):
        for _ in range(60):
            src_c, dst_c = rng.sample(comps, 2)
            src_user = rng.choice(users + machines)
            dst_user = rng.choice(users + machines)
            rows.append(
                [
                    t_for_day(rng, day),
                    f"{src_user}@Dom1",
                    f"{dst_user}@Dom1",
                    src_c,
                    dst_c,
                    rng.choice(AUTH_TYPES),
                    rng.choice(LOGON_TYPES),
                    rng.choice(ORIENTATIONS),
                    rng.choice(SUCCESS_VALUES),
                ]
            )
    return rows


def gen_proc(
    rng: random.Random,
    comps: list[str],
    users: list[str],
    machines: list[str],
) -> list[list]:
    """Generate synthetic process start/end events for all days."""
    rows = []
    for day in range(DAYS):
        for _ in range(50):
            rows.append(
                [
                    t_for_day(rng, day),
                    f"{rng.choice(users + machines)}@Dom1",
                    rng.choice(comps),
                    rng.choice(PROC_NAMES),
                    rng.choice(["Start", "End"]),
                ]
            )
    return rows


def gen_flows(rng: random.Random, comps: list[str]) -> list[list]:
    """Generate synthetic network flows, including duplicate rows."""
    rows = []
    for day in range(DAYS):
        for _ in range(50):
            src_c, dst_c = rng.sample(comps, 2)
            row = [
                t_for_day(rng, day),
                rng.randint(0, 120),
                src_c,
                rng.choice(WELL_KNOWN_PORTS + ANON_PORTS),
                dst_c,
                rng.choice(WELL_KNOWN_PORTS + ANON_PORTS),
                rng.choice([6, 17]),
                rng.randint(1, 50),
                rng.randint(100, 50000),
            ]
            rows.append(row)
            # LANL flows frequently appear as exact duplicate rows.
            if rng.random() < 0.3:
                rows.append(list(row))
    return rows


def gen_dns(rng: random.Random, comps: list[str]) -> list[list]:
    """Generate synthetic DNS lookup events for all days."""
    rows = []
    for day in range(DAYS):
        for _ in range(40):
            src_c = rng.choice(comps)
            resolved = rng.choice([c for c in comps if c != src_c])
            rows.append([t_for_day(rng, day), src_c, resolved])
    return rows


def write_csv(path: str, rows: list[list]) -> None:
    """Write rows as header-less CSV, sorted chronologically by time."""
    rows = sorted(rows, key=lambda r: r[0])
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(",".join(str(x) for x in row) + "\n")
    print(f"wrote {len(rows):>5} rows -> {path}")


def main() -> None:
    """Parse arguments and write all four synthetic source files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "..", "data", "sample"),
        help="output directory for the generated source files",
    )
    args = parser.parse_args()

    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)

    rng = random.Random(SEED)
    comps = computers()
    users = human_users()
    machines = machine_accounts(comps)

    write_csv(os.path.join(out_dir, "auth.txt"), gen_auth(rng, comps, users, machines))
    write_csv(os.path.join(out_dir, "proc.txt"), gen_proc(rng, comps, users, machines))
    write_csv(os.path.join(out_dir, "flows.txt"), gen_flows(rng, comps))
    write_csv(os.path.join(out_dir, "dns.txt"), gen_dns(rng, comps))
    print(f"\nGenerated {DAYS} day(s) of synthetic data in {out_dir}")


if __name__ == "__main__":
    main()
