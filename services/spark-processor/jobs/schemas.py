"""Per-source column definitions for the LANL sources.

The source files are header-less CSV with a fixed column order (see
docs/dataset/DATASET_DESCRIPTION.md).
"""

# constant for the number of seconds in a day
SECONDS_PER_DAY = 86400

# auth: time, srcUser@dom, dstUser@dom, srcComp, dstComp, authType, logon, orient, ok
AUTH_COLUMNS = [
    "time",
    "src_user",
    "dst_user",
    "src_comp",
    "dst_comp",
    "auth_type",
    "logon_type",
    "auth_orientation",
    "success",
]

# proc: time, user@dom, computer, processName, start/end
PROC_COLUMNS = [
    "time",
    "user",
    "computer",
    "process_name",
    "event_type",
]

# flows: time, duration, srcComp, srcPort, dstComp, dstPort, protocol, packets, bytes
FLOWS_COLUMNS = [
    "time",
    "duration",
    "src_comp",
    "src_port",
    "dst_comp",
    "dst_port",
    "protocol",
    "packet_count",
    "byte_count",
]

# dns: time, srcComp, resolvedComp
DNS_COLUMNS = [
    "time",
    "src_comp",
    "resolved_comp",
]

SOURCE_COLUMNS = {
    "auth": AUTH_COLUMNS,
    "proc": PROC_COLUMNS,
    "flows": FLOWS_COLUMNS,
    "dns": DNS_COLUMNS,
}
