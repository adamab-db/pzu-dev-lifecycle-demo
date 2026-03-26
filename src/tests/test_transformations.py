"""
Unit tests for claims transformation logic.
Uses pytest with fake data — no production tables touched.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, IntegerType, TimestampType,
)
from datetime import date, datetime
import sys, os

# Add helpers to path — handle both local runs and workspace runs
try:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
except NameError:
    pass  # __file__ not available in notebook context; runner notebook handles sys.path

from helpers.transformations import (
    clean_claims,
    aggregate_claims,
    filter_high_value,
    validate_claim_ids,
    validate_claim_types,
    VALID_CLAIM_TYPES,
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("unit-tests").getOrCreate()


@pytest.fixture(scope="module")
def raw_claims_schema():
    return StructType([
        StructField("claim_id", StringType(), True),
        StructField("policy_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("claim_type", StringType(), True),
        StructField("claim_amount", DoubleType(), True),
        StructField("claim_date", DateType(), True),
        StructField("region", StringType(), True),
        StructField("status", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("processing_days", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ])


@pytest.fixture(scope="module")
def sample_claims(spark, raw_claims_schema):
    """Create a small fake dataset mimicking bronze claims."""
    data = [
        ("CLM-000001", "POL-000001", "Jan Kowalski", "Motor", 5000.0,
         date(2025, 6, 15), "Mazowieckie", "Open", "Medium", 10, "Fender bender", datetime.now()),
        ("CLM-000002", "POL-000002", "Anna Nowak", "Health", 1200.0,
         date(2025, 7, 20), "Slaskie", "Approved", "Low", 5, "Routine checkup", datetime.now()),
        ("CLM-000003", "POL-000003", "Piotr Wisniewski", "Life", 250000.0,
         date(2025, 8, 1), "Pomorskie", "In Review", "High", 30, "Life insurance claim", datetime.now()),
        ("CLM-000004", "POL-000004", "Maria Kaminska", "Motor", 75000.0,
         date(2025, 9, 10), "Malopolskie", "Paid", "Medium", 15, "Collision damage", datetime.now()),
    ]
    return spark.createDataFrame(data, raw_claims_schema)


@pytest.fixture(scope="module")
def dirty_claims(spark, raw_claims_schema):
    """Claims with quality issues that should be filtered out."""
    data = [
        # Valid
        ("CLM-000010", "POL-000010", "Good Claim", "Motor", 5000.0,
         date(2025, 6, 15), "Mazowieckie", "Open", "Low", 10, "OK", datetime.now()),
        # Null claim_id
        (None, "POL-000011", "Null ID", "Motor", 3000.0,
         date(2025, 6, 16), "Slaskie", "Open", "Low", 5, "Bad", datetime.now()),
        # Negative amount
        ("CLM-000012", "POL-000012", "Neg Amount", "Health", -500.0,
         date(2025, 6, 17), "Pomorskie", "Open", "Low", 3, "Bad", datetime.now()),
        # Null region
        ("CLM-000013", "POL-000013", "Null Region", "Life", 10000.0,
         date(2025, 6, 18), None, "Open", "Low", 7, "Bad", datetime.now()),
        # Null claim_date
        ("CLM-000014", "POL-000014", "Null Date", "Travel", 2000.0,
         None, "Lodzkie", "Open", "Low", 2, "Bad", datetime.now()),
    ]
    return spark.createDataFrame(data, raw_claims_schema)


# --- clean_claims tests ---

class TestCleanClaims:
    def test_keeps_valid_rows(self, sample_claims):
        result = clean_claims(sample_claims)
        assert result.count() == 4

    def test_adds_processed_at_column(self, sample_claims):
        result = clean_claims(sample_claims)
        assert "processed_at" in result.columns

    def test_filters_null_claim_id(self, dirty_claims):
        result = clean_claims(dirty_claims)
        null_ids = result.filter("claim_id IS NULL").count()
        assert null_ids == 0

    def test_filters_negative_amounts(self, dirty_claims):
        result = clean_claims(dirty_claims)
        neg = result.filter("claim_amount <= 0").count()
        assert neg == 0

    def test_filters_null_regions(self, dirty_claims):
        result = clean_claims(dirty_claims)
        null_regions = result.filter("region IS NULL").count()
        assert null_regions == 0

    def test_filters_null_dates(self, dirty_claims):
        result = clean_claims(dirty_claims)
        null_dates = result.filter("claim_date IS NULL").count()
        assert null_dates == 0

    def test_dirty_data_yields_one_valid_row(self, dirty_claims):
        result = clean_claims(dirty_claims)
        assert result.count() == 1


# --- aggregate_claims tests ---

class TestAggregateClaims:
    def test_output_columns(self, sample_claims):
        cleaned = clean_claims(sample_claims)
        result = aggregate_claims(cleaned)
        expected_cols = {"claim_type", "region", "status", "claim_count",
                         "total_amount", "avg_amount", "min_amount",
                         "max_amount", "avg_processing_days"}
        assert expected_cols.issubset(set(result.columns))

    def test_total_amount_reconciles(self, sample_claims):
        cleaned = clean_claims(sample_claims)
        agg = aggregate_claims(cleaned)
        silver_total = cleaned.agg({"claim_amount": "sum"}).collect()[0][0]
        gold_total = agg.agg({"total_amount": "sum"}).collect()[0][0]
        assert abs(silver_total - gold_total) < 1.0

    def test_groups_by_type_region_status(self, sample_claims):
        cleaned = clean_claims(sample_claims)
        result = aggregate_claims(cleaned)
        # 4 claims, all different type/region/status combos → 4 rows
        assert result.count() == 4


# --- filter_high_value tests ---

class TestFilterHighValue:
    def test_default_threshold(self, sample_claims):
        result = filter_high_value(sample_claims)
        assert result.count() == 1  # Only the 250k Life claim
        assert result.collect()[0]["claim_id"] == "CLM-000003"

    def test_custom_threshold(self, sample_claims):
        result = filter_high_value(sample_claims, threshold=10000)
        assert result.count() == 2  # 250k and 75k


# --- validation tests ---

class TestValidation:
    def test_valid_claim_ids_pass(self, sample_claims):
        assert validate_claim_ids(sample_claims) == 0

    def test_invalid_claim_ids_detected(self, spark):
        bad_data = spark.createDataFrame(
            [("INVALID-1",), ("CLM-000001",), ("BAD",)],
            ["claim_id"],
        )
        assert validate_claim_ids(bad_data) == 2

    def test_valid_claim_types_pass(self, sample_claims):
        assert validate_claim_types(sample_claims) == set()

    def test_invalid_claim_types_detected(self, spark):
        bad_data = spark.createDataFrame(
            [("Motor",), ("InvalidType",), ("Health",)],
            ["claim_type"],
        )
        assert validate_claim_types(bad_data) == {"InvalidType"}
