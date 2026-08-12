"""Pure helpers for delivery schema-check details.

Compares a delivery manifest's declared column list against the consumer
feature contract so the dashboard can explain Passed/Failed beyond a boolean.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence


def schema_match(actual: Sequence[str] | None, expected: Sequence[str]) -> bool:
    """Return True when ``actual`` equals ``expected`` (names and order)."""
    return list(actual or ()) == list(expected)


def schema_diff(
    actual: Sequence[str] | None,
    expected: Sequence[str],
) -> dict:
    """Summarise how a delivered column list compares to the contract.

    Returns a dict with:
      - ``ok``: exact match
      - ``expected_count`` / ``actual_count``
      - ``missing``: contract columns absent from the delivery
      - ``unexpected``: delivery columns not in the contract
      - ``order_mismatch``: same set of names, different order
      - ``first_order_diff``: ``(index, expected_name, actual_name)`` or ``None``
    """
    expected_list = list(expected)
    actual_list = list(actual or ())
    expected_set, actual_set = set(expected_list), set(actual_list)
    missing = [c for c in expected_list if c not in actual_set]
    unexpected = [c for c in actual_list if c not in expected_set]
    same_set = not missing and not unexpected and bool(expected_list)
    order_mismatch = same_set and actual_list != expected_list

    first_order_diff: tuple[int, str, str] | None = None
    if order_mismatch:
        for index, (want, got) in enumerate(zip(expected_list, actual_list, strict=True)):
            if want != got:
                first_order_diff = (index, want, got)
                break

    return {
        "ok": actual_list == expected_list,
        "expected_count": len(expected_list),
        "actual_count": len(actual_list),
        "missing": missing,
        "unexpected": unexpected,
        "order_mismatch": order_mismatch,
        "first_order_diff": first_order_diff,
    }


def schema_summary_rows(
    manifest: Mapping,
    expected: Sequence[str],
) -> list[tuple[str, str]]:
    """Return ``(label, value)`` pairs for the schema-details overview."""
    diff = schema_diff(manifest.get("columns"), expected)
    status = "Passed" if diff["ok"] else "Failed"
    rows: list[tuple[str, str]] = [
        ("Status", status),
        ("Schema version", str(manifest.get("schema_version") or "—")),
        ("Expected columns", str(diff["expected_count"])),
        ("Delivered columns", str(diff["actual_count"])),
        ("Dataset", str(manifest.get("dataset") or "—")),
        ("Dataset version", str(manifest.get("dataset_version") or "—")),
    ]
    if diff["missing"]:
        rows.append(("Missing", ", ".join(diff["missing"])))
    if diff["unexpected"]:
        rows.append(("Unexpected", ", ".join(diff["unexpected"])))
    if diff["order_mismatch"] and diff["first_order_diff"] is not None:
        index, want, got = diff["first_order_diff"]
        rows.append(
            (
                "First order difference",
                f"position {index}: expected `{want}`, got `{got}`",
            )
        )
    return rows


def schema_column_rows(
    actual: Sequence[str] | None,
    expected: Sequence[str],
    *,
    label_for,
) -> list[dict[str, str]]:
    """Return per-column comparison rows for a details table.

    ``label_for`` is a callable ``column -> plain-language label``. Each row has
    ``position``, ``column``, ``label``, and ``status``
    (``ok`` / ``missing`` / ``unexpected`` / ``wrong order``).
    """
    expected_list = list(expected)
    actual_list = list(actual or ())
    expected_set, actual_set = set(expected_list), set(actual_list)
    rows: list[dict[str, str]] = []

    for index, column in enumerate(expected_list):
        if column not in actual_set:
            status = "missing"
        elif index < len(actual_list) and actual_list[index] == column:
            status = "ok"
        else:
            status = "wrong order"
        rows.append(
            {
                "position": str(index + 1),
                "column": column,
                "label": str(label_for(column)),
                "status": status,
            }
        )

    for column in actual_list:
        if column not in expected_set:
            rows.append(
                {
                    "position": "—",
                    "column": column,
                    "label": str(label_for(column)),
                    "status": "unexpected",
                }
            )
    return rows
