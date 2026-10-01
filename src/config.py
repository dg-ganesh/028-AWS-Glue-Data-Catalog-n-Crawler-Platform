"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Project Configuration
"""

from __future__ import annotations

import os
from pathlib import Path


# ---------------------------------------------------------------------------
# Project Identity
# ---------------------------------------------------------------------------

PROJECT_ID = "028"
PROJECT_NAME = "AWS Glue Data Catalog & Crawler Platform"
APPLICATION_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# AWS Configuration
# ---------------------------------------------------------------------------

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1").strip()

S3_BUCKET_NAME = os.getenv(
    "AWS_GLUE_PROJECT_BUCKET",
    "",
).strip()


# ---------------------------------------------------------------------------
# AWS S3 Configuration
# ---------------------------------------------------------------------------

S3_PROJECT_PREFIX = "project-028"

S3_CUSTOMERS_PREFIX = f"{S3_PROJECT_PREFIX}/customers"
S3_TRANSACTIONS_PREFIX = f"{S3_PROJECT_PREFIX}/transactions"
S3_SAMPLE_DATA_PREFIX = f"{S3_PROJECT_PREFIX}/sample-data"


# ---------------------------------------------------------------------------
# AWS Glue Configuration
# ---------------------------------------------------------------------------

GLUE_DATABASE_NAME = os.getenv(
    "AWS_GLUE_DATABASE_NAME",
    "project_028_catalog",
).strip()

GLUE_CRAWLER_NAME = os.getenv(
    "AWS_GLUE_CRAWLER_NAME",
    "project-028-crawler",
).strip()

GLUE_IAM_ROLE_ARN = os.getenv(
    "AWS_GLUE_IAM_ROLE_ARN",
    "",
).strip()

GLUE_CRAWLER_S3_PATH = (
    f"s3://{S3_BUCKET_NAME}/{S3_PROJECT_PREFIX}/"
    if S3_BUCKET_NAME
    else ""
)


# ---------------------------------------------------------------------------
# Local Project Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIRECTORY = PROJECT_ROOT / "data"
INPUT_DIRECTORY = DATA_DIRECTORY / "input"
OUTPUT_DIRECTORY = DATA_DIRECTORY / "output"
SAMPLES_DIRECTORY = DATA_DIRECTORY / "samples"

# Backward-compatible name used by existing Slice 1 modules.
DATA_SAMPLES_DIRECTORY = SAMPLES_DIRECTORY

LOG_DIRECTORY = PROJECT_ROOT / "logs"

EXECUTION_REPORT_PATH = LOG_DIRECTORY / "execution_report.txt"

# Backward-compatible name used by execution_report_service.py.
EXECUTION_REPORT_FILE = EXECUTION_REPORT_PATH


# ---------------------------------------------------------------------------
# Dataset Configuration
# ---------------------------------------------------------------------------

CUSTOMERS_DATASET_NAME = "customers.parquet"
TRANSACTIONS_DATASET_NAME = "transactions.parquet"

CUSTOMERS_S3_KEY = (
    f"{S3_CUSTOMERS_PREFIX}/{CUSTOMERS_DATASET_NAME}"
)

TRANSACTIONS_S3_KEY = (
    f"{S3_TRANSACTIONS_PREFIX}/{TRANSACTIONS_DATASET_NAME}"
)


# ---------------------------------------------------------------------------
# Configuration Access
# ---------------------------------------------------------------------------

def get_required_configuration() -> dict[str, str]:
    """
    Return the AWS configuration values required by Project 028.
    """

    return {
        "AWS_REGION": AWS_REGION,
        "AWS_GLUE_PROJECT_BUCKET": S3_BUCKET_NAME,
        "AWS_GLUE_DATABASE_NAME": GLUE_DATABASE_NAME,
        "AWS_GLUE_CRAWLER_NAME": GLUE_CRAWLER_NAME,
        "AWS_GLUE_IAM_ROLE_ARN": GLUE_IAM_ROLE_ARN,
    }


def validate_configuration() -> tuple[bool, list[str]]:
    """
    Validate the configuration required for the current execution.

    Returns:
        Tuple containing:
            - True/False indicating whether configuration is valid.
            - List of validation errors.
    """

    errors: list[str] = []

    if not AWS_REGION:
        errors.append(
            "AWS region is not configured."
        )

    if not S3_BUCKET_NAME:
        errors.append(
            "S3 bucket name is not configured. "
            "Set the AWS_GLUE_PROJECT_BUCKET environment variable."
        )

    if not GLUE_DATABASE_NAME:
        errors.append(
            "Glue database name is not configured."
        )

    if not GLUE_CRAWLER_NAME:
        errors.append(
            "Glue crawler name is not configured."
        )

    return len(errors) == 0, errors


def validate_glue_configuration() -> tuple[bool, list[str]]:
    """
    Validate configuration specifically required for AWS Glue operations.

    Returns:
        Tuple containing:
            - True/False indicating whether Glue configuration is valid.
            - List of validation errors.
    """

    errors: list[str] = []

    if not AWS_REGION:
        errors.append(
            "AWS region is not configured."
        )

    if not S3_BUCKET_NAME:
        errors.append(
            "S3 bucket name is not configured."
        )

    if not GLUE_DATABASE_NAME:
        errors.append(
            "Glue database name is not configured."
        )

    if not GLUE_CRAWLER_NAME:
        errors.append(
            "Glue crawler name is not configured."
        )

    if not GLUE_IAM_ROLE_ARN:
        errors.append(
            "Glue IAM role ARN is not configured. "
            "Set the AWS_GLUE_IAM_ROLE_ARN environment variable."
        )

    if not GLUE_CRAWLER_S3_PATH:
        errors.append(
            "Glue crawler S3 path could not be constructed."
        )

    return len(errors) == 0, errors


# ---------------------------------------------------------------------------
# Public Interface
# ---------------------------------------------------------------------------
#
# Public configuration values:
#   PROJECT_ID
#   PROJECT_NAME
#   APPLICATION_VERSION
#   AWS_REGION
#   S3_BUCKET_NAME
#   S3_PROJECT_PREFIX
#   S3_CUSTOMERS_PREFIX
#   S3_TRANSACTIONS_PREFIX
#   S3_SAMPLE_DATA_PREFIX
#   GLUE_DATABASE_NAME
#   GLUE_CRAWLER_NAME
#   GLUE_IAM_ROLE_ARN
#   GLUE_CRAWLER_S3_PATH
#   PROJECT_ROOT
#   DATA_DIRECTORY
#   INPUT_DIRECTORY
#   OUTPUT_DIRECTORY
#   SAMPLES_DIRECTORY
#   DATA_SAMPLES_DIRECTORY
#   LOG_DIRECTORY
#   EXECUTION_REPORT_PATH
#   EXECUTION_REPORT_FILE
#   CUSTOMERS_DATASET_NAME
#   TRANSACTIONS_DATASET_NAME
#   CUSTOMERS_S3_KEY
#   TRANSACTIONS_S3_KEY
#
# Public functions:
#   get_required_configuration()
#   validate_configuration()
#   validate_glue_configuration()
#
# External dependencies:
#   Python standard library only.
# ---------------------------------------------------------------------------