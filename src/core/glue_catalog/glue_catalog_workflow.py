"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

AWS Glue Catalog Workflow
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from src import config
from src.services.catalog_service import (
    CatalogService,
    TableMetadata,
)
from src.services.glue_service import (
    GlueOperationResult,
    GlueService,
)


@dataclass
class WorkflowCheckpoint:
    """
    Represents one Slice 2 execution checkpoint.
    """

    name: str
    success: bool
    message: str


@dataclass
class GlueCatalogWorkflowResult:
    """
    Represents the complete Slice 2 workflow result.
    """

    success: bool
    message: str
    checkpoints: list[WorkflowCheckpoint] = field(
        default_factory=list
    )
    tables: list[TableMetadata] = field(
        default_factory=list
    )
    started_at: str = ""
    completed_at: str = ""


class GlueCatalogWorkflow:
    """
    Orchestrates AWS Glue database, crawler, and
    Data Catalog discovery for Project 028.
    """

    DATABASE_DESCRIPTION = (
        "Project 028 AWS Glue Data Catalog database."
    )

    EXPECTED_TABLES = (
        "customers",
        "transactions",
    )

    DEFAULT_POLL_INTERVAL_SECONDS = 10

    def __init__(
        self,
        glue_service: GlueService | None = None,
        catalog_service: CatalogService | None = None,
        poll_interval_seconds: int | None = None,
    ) -> None:
        """
        Initialize the Glue Catalog workflow.
        """

        self._glue_service = (
            glue_service or GlueService()
        )

        self._catalog_service = (
            catalog_service or CatalogService()
        )

        self._poll_interval_seconds = (
            poll_interval_seconds
            if poll_interval_seconds is not None
            else self.DEFAULT_POLL_INTERVAL_SECONDS
        )

    def execute(self) -> GlueCatalogWorkflowResult:
        """
        Execute the complete Slice 2 Glue workflow.

        Returns:
            GlueCatalogWorkflowResult.
        """

        started_at = self._timestamp()
        checkpoints: list[WorkflowCheckpoint] = []

        configuration_ok = self._validate_configuration(
            checkpoints
        )

        if not configuration_ok:
            return self._build_failure_result(
                "Glue configuration validation failed.",
                checkpoints,
                started_at,
            )

        database_result = self._ensure_database(
            checkpoints
        )

        if not database_result.success:
            return self._build_failure_result(
                database_result.message,
                checkpoints,
                started_at,
            )

        crawler_result = self._ensure_crawler(
            checkpoints
        )

        if not crawler_result.success:
            return self._build_failure_result(
                crawler_result.message,
                checkpoints,
                started_at,
            )

        start_result = self._start_crawler(
            checkpoints
        )

        if not start_result.success:
            return self._build_failure_result(
                start_result.message,
                checkpoints,
                started_at,
            )

        completion_result = self._wait_for_crawler(
            checkpoints
        )

        if not completion_result.success:
            return self._build_failure_result(
                completion_result.message,
                checkpoints,
                started_at,
            )

        catalog_result = self._discover_catalog(
            checkpoints
        )

        if not catalog_result.success:
            return self._build_failure_result(
                catalog_result.message,
                checkpoints,
                started_at,
            )

        validation_result = self._validate_catalog(
            catalog_result.tables,
            checkpoints,
        )

        if not validation_result:
            return self._build_failure_result(
                "Glue Catalog validation failed.",
                checkpoints,
                started_at,
            )

        completed_at = self._timestamp()

        return GlueCatalogWorkflowResult(
            success=True,
            message=(
                "AWS Glue metadata discovery completed "
                "successfully."
            ),
            checkpoints=checkpoints,
            tables=catalog_result.tables,
            started_at=started_at,
            completed_at=completed_at,
        )

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def _validate_configuration(
        self,
        checkpoints: list[WorkflowCheckpoint],
    ) -> bool:
        """
        Validate Glue-specific project configuration.
        """

        valid, errors = config.validate_glue_configuration()

        if valid:
            checkpoints.append(
                WorkflowCheckpoint(
                    name="Glue configuration validation",
                    success=True,
                    message=(
                        "Glue configuration is valid."
                    ),
                )
            )
            return True

        message = "; ".join(errors)

        checkpoints.append(
            WorkflowCheckpoint(
                name="Glue configuration validation",
                success=False,
                message=message,
            )
        )

        return False

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------

    def _ensure_database(
        self,
        checkpoints: list[WorkflowCheckpoint],
    ) -> GlueOperationResult:
        """
        Ensure the Glue Data Catalog database exists.
        """

        result = self._glue_service.ensure_database(
            database_name=config.GLUE_DATABASE_NAME,
            description=self.DATABASE_DESCRIPTION,
        )

        checkpoints.append(
            WorkflowCheckpoint(
                name="Glue database",
                success=result.success,
                message=result.message,
            )
        )

        return result

    # ------------------------------------------------------------------
    # Crawler
    # ------------------------------------------------------------------

    def _ensure_crawler(
        self,
        checkpoints: list[WorkflowCheckpoint],
    ) -> GlueOperationResult:
        """
        Ensure the Glue crawler exists.
        """

        existing = self._glue_service.crawler_exists(
            config.GLUE_CRAWLER_NAME,
        )

        if not existing.success:
            checkpoints.append(
                WorkflowCheckpoint(
                    name="Glue crawler lookup",
                    success=False,
                    message=existing.message,
                )
            )
            return existing

        if existing.data is not None:
            checkpoints.append(
                WorkflowCheckpoint(
                    name="Glue crawler",
                    success=True,
                    message=(
                        f"Glue crawler "
                        f"'{config.GLUE_CRAWLER_NAME}' "
                        "is ready."
                    ),
                )
            )

            return existing

        if not config.GLUE_IAM_ROLE_ARN:
            result = GlueOperationResult(
                success=False,
                message=(
                    "Glue crawler does not exist and "
                    "AWS_GLUE_IAM_ROLE_ARN is not configured."
                ),
            )

            checkpoints.append(
                WorkflowCheckpoint(
                    name="Glue crawler",
                    success=False,
                    message=result.message,
                )
            )

            return result

        result = self._glue_service.create_crawler(
            crawler_name=config.GLUE_CRAWLER_NAME,
            database_name=config.GLUE_DATABASE_NAME,
            role_arn=config.GLUE_IAM_ROLE_ARN,
            s3_path=config.GLUE_CRAWLER_S3_PATH,
        )

        checkpoints.append(
            WorkflowCheckpoint(
                name="Glue crawler",
                success=result.success,
                message=result.message,
            )
        )

        return result

    # ------------------------------------------------------------------
    # Crawler Execution
    # ------------------------------------------------------------------

    def _start_crawler(
        self,
        checkpoints: list[WorkflowCheckpoint],
    ) -> GlueOperationResult:
        """
        Start the configured Glue crawler.
        """

        result = self._glue_service.start_crawler(
            config.GLUE_CRAWLER_NAME,
        )

        checkpoints.append(
            WorkflowCheckpoint(
                name="Glue crawler start",
                success=result.success,
                message=result.message,
            )
        )

        return result

    def _wait_for_crawler(
        self,
        checkpoints: list[WorkflowCheckpoint],
    ) -> GlueOperationResult:
        """
        Wait until the Glue crawler reaches a terminal state.
        """

        while True:
            result = self._glue_service.get_crawler_status(
                config.GLUE_CRAWLER_NAME,
            )

            if not result.success:
                checkpoints.append(
                    WorkflowCheckpoint(
                        name="Glue crawler monitoring",
                        success=False,
                        message=result.message,
                    )
                )
                return result

            crawler_data = result.data or {}
            state = crawler_data.get("state")

            if state == "READY":
                checkpoints.append(
                    WorkflowCheckpoint(
                        name="Glue crawler completion",
                        success=True,
                        message=(
                            "Glue crawler completed "
                            "and is READY."
                        ),
                    )
                )

                return GlueOperationResult(
                    success=True,
                    message=(
                        "Glue crawler completed successfully."
                    ),
                    data=crawler_data,
                )

            if state in {
                "FAILED",
                "STOPPING",
            }:
                message = (
                    f"Glue crawler entered terminal "
                    f"state '{state}'."
                )

                checkpoints.append(
                    WorkflowCheckpoint(
                        name="Glue crawler completion",
                        success=False,
                        message=message,
                    )
                )

                return GlueOperationResult(
                    success=False,
                    message=message,
                    data=crawler_data,
                )

            time.sleep(
                self._poll_interval_seconds
            )

    # ------------------------------------------------------------------
    # Catalog Discovery
    # ------------------------------------------------------------------

    def _discover_catalog(
        self,
        checkpoints: list[WorkflowCheckpoint],
    ):
        """
        Retrieve tables discovered by the Glue crawler.
        """

        result = self._glue_service.get_tables(
            config.GLUE_DATABASE_NAME,
        )

        if not result.success:
            checkpoints.append(
                WorkflowCheckpoint(
                    name="Glue Catalog discovery",
                    success=False,
                    message=result.message,
                )
            )
            return self._catalog_failure_result(
                result.message
            )

        catalog_result = (
            self._catalog_service.build_catalog_metadata(
                config.GLUE_DATABASE_NAME,
                result.data or [],
            )
        )

        checkpoints.append(
            WorkflowCheckpoint(
                name="Glue Catalog discovery",
                success=catalog_result.success,
                message=catalog_result.message,
            )
        )

        return catalog_result

    # ------------------------------------------------------------------
    # Catalog Validation
    # ------------------------------------------------------------------

    def _validate_catalog(
        self,
        tables: list[TableMetadata],
        checkpoints: list[WorkflowCheckpoint],
    ) -> bool:
        """
        Validate the expected tables and their schemas.
        """

        expected_tables = set(
            self.EXPECTED_TABLES
        )

        actual_tables = {
            table.name.lower()
            for table in tables
        }

        missing_tables = (
            expected_tables - actual_tables
        )

        if missing_tables:
            message = (
                "Missing expected Glue Catalog table(s): "
                + ", ".join(sorted(missing_tables))
            )

            checkpoints.append(
                WorkflowCheckpoint(
                    name="Glue Catalog table validation",
                    success=False,
                    message=message,
                )
            )

            return False

        checkpoints.append(
            WorkflowCheckpoint(
                name="Glue Catalog table validation",
                success=True,
                message=(
                    "Expected Glue Catalog tables "
                    "were discovered."
                ),
            )
        )

        customers = self._catalog_service.find_table(
            tables,
            "customers",
        )

        transactions = self._catalog_service.find_table(
            tables,
            "transactions",
        )

        if customers is None or transactions is None:
            return False

        customers_valid, customer_errors = (
            self._catalog_service.validate_required_columns(
                customers,
                [
                    "customer_id",
                    "customer_name",
                    "country",
                    "customer_segment",
                ],
            )
        )

        if not customers_valid:
            checkpoints.append(
                WorkflowCheckpoint(
                    name="Customers schema validation",
                    success=False,
                    message="; ".join(
                        customer_errors
                    ),
                )
            )
            return False

        checkpoints.append(
            WorkflowCheckpoint(
                name="Customers schema validation",
                success=True,
                message=(
                    "Customers table contains all "
                    "expected columns."
                ),
            )
        )

        transactions_valid, transaction_errors = (
            self._catalog_service.validate_required_columns(
                transactions,
                [
                    "transaction_id",
                    "customer_id",
                    "transaction_date",
                    "amount",
                    "currency",
                ],
            )
        )

        if not transactions_valid:
            checkpoints.append(
                WorkflowCheckpoint(
                    name="Transactions schema validation",
                    success=False,
                    message="; ".join(
                        transaction_errors
                    ),
                )
            )
            return False

        checkpoints.append(
            WorkflowCheckpoint(
                name="Transactions schema validation",
                success=True,
                message=(
                    "Transactions table contains all "
                    "expected columns."
                ),
            )
        )

        location_validation = (
            self._validate_table_locations(
                customers,
                transactions,
                checkpoints,
            )
        )

        return location_validation

    def _validate_table_locations(
        self,
        customers: TableMetadata,
        transactions: TableMetadata,
        checkpoints: list[WorkflowCheckpoint],
    ) -> bool:
        """
        Validate Catalog S3 locations for discovered tables.
        """

        customer_prefix = (
            f"s3://{config.S3_BUCKET_NAME}/"
            f"{config.S3_CUSTOMERS_PREFIX}"
        )

        transaction_prefix = (
            f"s3://{config.S3_BUCKET_NAME}/"
            f"{config.S3_TRANSACTIONS_PREFIX}"
        )

        customers_valid, customers_message = (
            self._catalog_service.validate_table_location(
                customers,
                customer_prefix,
            )
        )

        if not customers_valid:
            checkpoints.append(
                WorkflowCheckpoint(
                    name="Customers S3 location validation",
                    success=False,
                    message=customers_message,
                )
            )
            return False

        checkpoints.append(
            WorkflowCheckpoint(
                name="Customers S3 location validation",
                success=True,
                message=customers_message,
            )
        )

        transactions_valid, transactions_message = (
            self._catalog_service.validate_table_location(
                transactions,
                transaction_prefix,
            )
        )

        if not transactions_valid:
            checkpoints.append(
                WorkflowCheckpoint(
                    name="Transactions S3 location validation",
                    success=False,
                    message=transactions_message,
                )
            )
            return False

        checkpoints.append(
            WorkflowCheckpoint(
                name="Transactions S3 location validation",
                success=True,
                message=transactions_message,
            )
        )

        return True

    # ------------------------------------------------------------------
    # Result Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _catalog_failure_result(
        message: str,
    ):
        """
        Create a failed catalog result without requiring
        an additional public result dependency.
        """

        from src.services.catalog_service import (
            CatalogMetadataResult,
        )

        return CatalogMetadataResult(
            success=False,
            message=message,
        )

    @staticmethod
    def _build_failure_result(
        message: str,
        checkpoints: list[WorkflowCheckpoint],
        started_at: str,
    ) -> GlueCatalogWorkflowResult:
        """
        Build a failed workflow result.
        """

        return GlueCatalogWorkflowResult(
            success=False,
            message=message,
            checkpoints=checkpoints,
            started_at=started_at,
            completed_at=GlueCatalogWorkflow._timestamp(),
        )

    @staticmethod
    def _timestamp() -> str:
        """
        Return the current UTC timestamp.
        """

        return datetime.now(
            timezone.utc
        ).isoformat()


# ---------------------------------------------------------------------------
# Public Interface
# ---------------------------------------------------------------------------
#
# Public data models:
#   WorkflowCheckpoint
#   GlueCatalogWorkflowResult
#
# Public class:
#   GlueCatalogWorkflow
#
# Public method:
#   execute()
#
# External dependencies:
#   boto3 indirectly through GlueService.
# ---------------------------------------------------------------------------