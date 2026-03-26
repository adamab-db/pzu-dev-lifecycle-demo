"""
Shared transformation and validation functions for the claims pipeline.
Extracted from notebooks so they can be unit tested with pytest.
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


VALID_CLAIM_TYPES = {"Motor", "Property", "Health", "Life", "Travel", "Liability"}

VALID_REGIONS = {
    "Mazowieckie", "Malopolskie", "Slaskie", "Wielkopolskie",
    "Dolnoslaskie", "Pomorskie", "Lodzkie", "Lubelskie",
}


def clean_claims(df: DataFrame) -> DataFrame:
    """Apply silver-layer cleansing rules to raw claims data."""
    return (
        df.filter(F.col("claim_id").isNotNull())
        .filter(F.col("claim_amount") > 0)
        .filter(F.col("claim_date").isNotNull())
        .filter(F.col("region").isNotNull())
        .withColumn("processed_at", F.current_timestamp())
    )


def aggregate_claims(df: DataFrame) -> DataFrame:
    """Build gold-layer summary from silver claims."""
    return (
        df.groupBy("claim_type", "region", "status")
        .agg(
            F.count("*").alias("claim_count"),
            F.round(F.sum("claim_amount"), 2).alias("total_amount"),
            F.round(F.avg("claim_amount"), 2).alias("avg_amount"),
            F.round(F.min("claim_amount"), 2).alias("min_amount"),
            F.round(F.max("claim_amount"), 2).alias("max_amount"),
            F.round(F.avg("processing_days"), 1).alias("avg_processing_days"),
        )
    )


def filter_high_value(df: DataFrame, threshold: float = 100_000) -> DataFrame:
    """Return claims above the given amount threshold."""
    return df.filter(F.col("claim_amount") > threshold)


def validate_claim_ids(df: DataFrame) -> int:
    """Return count of claim_ids that don't match the CLM-XXXXXX pattern."""
    return df.filter(~F.col("claim_id").rlike(r"^CLM-\d{6}$")).count()


def validate_claim_types(df: DataFrame) -> set:
    """Return any claim types not in the allowed set."""
    actual = {row["claim_type"] for row in df.select("claim_type").distinct().collect()}
    return actual - VALID_CLAIM_TYPES
