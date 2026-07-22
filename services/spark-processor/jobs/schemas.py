"""Per-source column definitions for the LANL sources.

The source files are header-less CSV with a fixed column order (see
docs/dataset/DATASET_DESCRIPTION.md).
"""

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

SOURCE_COLUMNS = {
    "auth": AUTH_COLUMNS,
}
