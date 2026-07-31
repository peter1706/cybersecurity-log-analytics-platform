"""Unit tests for the ml-mock consumer's schema contract (no MinIO required)."""

from __future__ import annotations

import pytest

import feature_contract


class TestValidateSchema:
    def test_accepts_exact_contract(self):
        feature_contract.validate_schema(list(feature_contract.EXPECTED_COLUMNS))

    def test_rejects_missing_column(self):
        cols = [c for c in feature_contract.EXPECTED_COLUMNS if c != "dns_lookup_count"]
        with pytest.raises(feature_contract.SchemaContractError, match="missing"):
            feature_contract.validate_schema(cols)

    def test_rejects_unexpected_column(self):
        cols = [*feature_contract.EXPECTED_COLUMNS, "extra_feature"]
        with pytest.raises(feature_contract.SchemaContractError, match="unexpected"):
            feature_contract.validate_schema(cols)

    def test_rejects_wrong_order(self):
        cols = list(feature_contract.EXPECTED_COLUMNS)
        cols[1], cols[2] = cols[2], cols[1]  # swap anchor_day / window_days
        with pytest.raises(feature_contract.SchemaContractError, match="order"):
            feature_contract.validate_schema(cols)


def test_consumer_contract_matches_producer_and_delivery():
    """The three independent copies of the contract must agree (drift guard)."""
    features = pytest.importorskip("jobs.transforms.features")
    bundle = pytest.importorskip("bundle")
    assert feature_contract.EXPECTED_COLUMNS == features.COMPUTER_FEATURE_COLUMNS
    assert feature_contract.EXPECTED_COLUMNS == bundle.DELIVERED_COLUMNS
