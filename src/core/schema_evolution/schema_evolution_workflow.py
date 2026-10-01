"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Schema Evolution Workflow
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.config import (
    CUSTOMERS_DATASET_NAME,
    S3_BUCKET_NAME,
    S3_CUSTOMERS_PREFIX,
    SAMPLES_DIRECTORY,
)
from src.services.catalog_service import CatalogService
from src.services.glue_service import GlueService
from src.services.s3_service import S3Service
from src.services.schema_evolution_service import (
    SchemaEvolutionService,
    SchemaComparisonResult,
)


@dataclass
class SchemaEvolutionWorkflowResult:
    """
    Represent the complete schema-evolution workflow result.
    """

    success: bool
    status: str
    checkpoints: list[dict[str, Any]] = field(
        default_factory=list
    )
    comparison: SchemaComparisonResult | None = None
    baseline_schema: list[dict[str, Any]] = field(
        default_factory=list
    )
    current_schema: list[dict[str, Any]] = field(
        default_factory=list
    )


class SchemaEvolutionWorkflow:
    """
    Orchestrate the Project 028 schema-evolution demonstration.

    The workflow:
        1. Retrieves the existing customers Catalog schema.
        2. Creates a controlled evolved Parquet dataset locally.
        3. Adds customer_segment without removing existing columns.
        4. Uploads the evolved dataset to the existing customers S3 path.
        5. Runs the existing Glue crawler.
        6. Retrieves the updated Catalog schema.
        7. Compares baseline and current schemas.
        8. Validates the expected customer_segment addition.
    """

    EXPECTED_TABLE_NAME = "customers"
    EXPECTED_NEW_COLUMN = "customer_segment"
    EXPECTED_NEW_COLUMN_TYPE = "string"

    def __init__(
        self,
        glue_service: GlueService | None = None,
        s3_service: S3Service | None = None,
        catalog_service: CatalogService | None = None,
        schema_service: SchemaEvolutionService | None = None,
        sample_directory: Path | None = None,
    ) -> None:
        """
        Initialize the schema-evolution workflow.

        Args:
            glue_service: Optional Glue service.
            s3_service: Optional S3 service.
            catalog_service: Optional Catalog service.
            schema_service: Optional schema-evolution service.
            sample_directory: Optional local dataset directory.
        """

        self.glue_service = (
            glue_service or GlueService()
        )

        self.s3_service = (
            s3_service or S3Service()
        )

        self.catalog_service = (
            catalog_service or CatalogService()
        )

        self.schema_service = (
            schema_service
            or SchemaEvolutionService()
        )

        self.sample_directory = (
            sample_directory
            or SAMPLES_DIRECTORY
        )

        self.evolved_directory = (
            self.sample_directory.parent
            / "output"
            / "schema_evolution"
        )

    # ------------------------------------------------------------------
    # Main Workflow
    # ------------------------------------------------------------------

    def execute(self) -> SchemaEvolutionWorkflowResult:
        """
        Execute the complete schema-evolution workflow.

        Returns:
            SchemaEvolutionWorkflowResult.
        """

        checkpoints: list[dict[str, Any]] = []

        try:
            baseline_result = (
                self._capture_baseline_schema()
            )

            checkpoints.append(
                baseline_result
            )

            if not baseline_result["success"]:
                return self._build_result(
                    False,
                    checkpoints,
                )

            evolved_result = (
                self._prepare_evolved_dataset()
            )

            checkpoints.append(
                evolved_result
            )

            if not evolved_result["success"]:
                return self._build_result(
                    False,
                    checkpoints,
                )

            upload_result = (
                self._upload_evolved_dataset(
                    evolved_result["path"]
                )
            )

            checkpoints.append(
                upload_result
            )

            if not upload_result["success"]:
                return self._build_result(
                    False,
                    checkpoints,
                )

            crawler_result = (
                self._run_crawler()
            )

            checkpoints.extend(
                crawler_result["checkpoints"]
            )

            if not crawler_result["success"]:
                return self._build_result(
                    False,
                    checkpoints,
                )

            current_result = (
                self._capture_current_schema()
            )

            checkpoints.append(
                current_result
            )

            if not current_result["success"]:
                return self._build_result(
                    False,
                    checkpoints,
                )

            comparison_result = (
                self._compare_schemas(
                    baseline_result["columns"],
                    current_result["columns"],
                )
            )

            checkpoints.append(
                comparison_result["checkpoint"]
            )

            if not comparison_result[
                "checkpoint"
            ]["success"]:
                return self._build_result(
                    False,
                    checkpoints,
                    comparison_result.get(
                        "comparison"
                    ),
                    baseline_result["columns"],
                    current_result["columns"],
                )

            validation_result = (
                self._validate_expected_change(
                    comparison_result["comparison"]
                )
            )

            checkpoints.append(
                validation_result
            )

            return self._build_result(
                validation_result["success"],
                checkpoints,
                comparison_result["comparison"],
                baseline_result["columns"],
                current_result["columns"],
            )

        except Exception as exc:
            checkpoints.append(
                {
                    "name": "Schema evolution workflow",
                    "success": False,
                    "message": (
                        "Unhandled schema-evolution "
                        f"error: {exc}"
                    ),
                }
            )

            return self._build_result(
                False,
                checkpoints,
            )

    # ------------------------------------------------------------------
    # Baseline
    # ------------------------------------------------------------------

    def _capture_baseline_schema(
        self,
    ) -> dict[str, Any]:
        """
        Capture the current customers Catalog schema.

        Returns:
            Baseline schema checkpoint.
        """

        result = self.glue_service.get_table(
            database_name=self.glue_service.database_name,
            table_name=self.EXPECTED_TABLE_NAME,
        )

        if not result.success:
            return {
                "name": "Baseline schema capture",
                "success": False,
                "message": result.message,
            }

        table = (
            result.data or {}
        ).get(
            "table",
            result.data or {},
        )

        metadata = (
            self.catalog_service.parse_table(
                table
            )
        )

        columns = [
            {
                "name": column.name,
                "data_type": column.data_type,
                "ordinal_position": (
                    column.ordinal_position
                ),
            }
            for column in metadata.columns
        ]

        return {
            "name": "Baseline schema capture",
            "success": True,
            "message": (
                f"Captured baseline schema for "
                f"'{self.EXPECTED_TABLE_NAME}' "
                f"with {len(columns)} column(s)."
            ),
            "columns": columns,
        }

    # ------------------------------------------------------------------
    # Controlled Dataset Evolution
    # ------------------------------------------------------------------

    def _prepare_evolved_dataset(
        self,
    ) -> dict[str, Any]:
        """
        Create a controlled evolved customers dataset.

        The source dataset is copied and a new string column named
        customer_segment is added. Existing columns remain unchanged.

        Returns:
            Dataset preparation checkpoint.
        """

        source_path = (
            self.sample_directory
            / CUSTOMERS_DATASET_NAME
        )

        if not source_path.is_file():
            return {
                "name": "Schema-evolved dataset preparation",
                "success": False,
                "message": (
                    "Baseline customers dataset was "
                    f"not found: {source_path}"
                ),
            }

        self.evolved_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        evolved_path = (
            self.evolved_directory
            / "customers_schema_evolved.parquet"
        )

        try:
            table = pq.read_table(
                source_path
            )

            row_count = table.num_rows

            customer_segment = pa.array(
                [
                    self._derive_customer_segment(
                        index
                    )
                    for index in range(row_count)
                ],
                type=pa.string(),
            )

            evolved_table = table.append_column(
                self.EXPECTED_NEW_COLUMN,
                customer_segment,
            )

            pq.write_table(
                evolved_table,
                evolved_path,
            )

            return {
                "name": (
                    "Schema-evolved dataset preparation"
                ),
                "success": True,
                "message": (
                    "Created controlled Parquet dataset "
                    f"with new column "
                    f"'{self.EXPECTED_NEW_COLUMN}'."
                ),
                "path": evolved_path,
            }

        except Exception as exc:
            return {
                "name": (
                    "Schema-evolved dataset preparation"
                ),
                "success": False,
                "message": (
                    "Unable to create evolved dataset: "
                    f"{exc}"
                ),
            }

    @staticmethod
    def _derive_customer_segment(
        row_number: int,
    ) -> str:
        """
        Generate deterministic demonstration values.

        Args:
            row_number: Zero-based row number.

        Returns:
            Customer segment value.
        """

        segments = (
            "STANDARD",
            "PREMIUM",
            "PREMIUM",
            "STANDARD",
        )

        return segments[
            row_number % len(segments)
        ]

    # ------------------------------------------------------------------
    # S3 Upload
    # ------------------------------------------------------------------

    def _upload_evolved_dataset(
        self,
        evolved_path: Path,
    ) -> dict[str, Any]:
        """
        Upload the evolved customers dataset.

        Args:
            evolved_path: Local evolved Parquet path.

        Returns:
            Upload checkpoint.
        """

        if not evolved_path.is_file():
            return {
                "name": "Schema-evolved dataset upload",
                "success": False,
                "message": (
                    "Evolved dataset does not exist: "
                    f"{evolved_path}"
                ),
            }

        object_key = (
            f"{S3_CUSTOMERS_PREFIX}/"
            f"{CUSTOMERS_DATASET_NAME}"
        )

        try:
            s3_uri = self.s3_service.upload_file(
                local_file=evolved_path,
                s3_key=object_key,
            )

            exists = (
                self.s3_service.object_exists(
                    object_key
                )
            )

            if not exists:
                return {
                    "name": (
                        "Schema-evolved dataset upload"
                    ),
                    "success": False,
                    "message": (
                        "Uploaded dataset could not "
                        "be verified in S3."
                    ),
                }

            return {
                "name": (
                    "Schema-evolved dataset upload"
                ),
                "success": True,
                "message": (
                    "Evolved customers dataset uploaded "
                    f"to {s3_uri}."
                ),
                "s3_uri": s3_uri,
            }

        except Exception as exc:
            return {
                "name": (
                    "Schema-evolved dataset upload"
                ),
                "success": False,
                "message": (
                    "Unable to upload evolved dataset: "
                    f"{exc}"
                ),
            }

    # ------------------------------------------------------------------
    # Glue Crawler
    # ------------------------------------------------------------------

    def _run_crawler(
        self,
    ) -> dict[str, Any]:
        """
        Run the existing Project 028 Glue crawler.

        Returns:
            Crawler checkpoints and overall status.
        """

        checkpoints: list[dict[str, Any]] = []

        start_result = (
            self.glue_service.start_crawler()
        )

        checkpoints.append(
            {
                "name": "Schema evolution crawler start",
                "success": start_result.success,
                "message": start_result.message,
            }
        )

        if not start_result.success:
            return {
                "success": False,
                "checkpoints": checkpoints,
            }

        completion_result = (
            self.glue_service.wait_for_crawler(
                timeout_seconds=600
            )
        )

        checkpoints.append(
            {
                "name": (
                    "Schema evolution crawler completion"
                ),
                "success": completion_result.success,
                "message": completion_result.message,
            }
        )

        return {
            "success": completion_result.success,
            "checkpoints": checkpoints,
        }

    # ------------------------------------------------------------------
    # Current Schema
    # ------------------------------------------------------------------

    def _capture_current_schema(
        self,
    ) -> dict[str, Any]:
        """
        Capture the post-crawler customers Catalog schema.

        Returns:
            Current schema checkpoint.
        """

        result = self.glue_service.get_table(
            database_name=self.glue_service.database_name,
            table_name=self.EXPECTED_TABLE_NAME,
        )

        if not result.success:
            return {
                "name": "Current schema capture",
                "success": False,
                "message": result.message,
            }

        table = (
            result.data or {}
        ).get(
            "table",
            result.data or {},
        )

        metadata = (
            self.catalog_service.parse_table(
                table
            )
        )

        columns = [
            {
                "name": column.name,
                "data_type": column.data_type,
                "ordinal_position": (
                    column.ordinal_position
                ),
            }
            for column in metadata.columns
        ]

        return {
            "name": "Current schema capture",
            "success": True,
            "message": (
                f"Captured current schema for "
                f"'{self.EXPECTED_TABLE_NAME}' "
                f"with {len(columns)} column(s)."
            ),
            "columns": columns,
        }

    # ------------------------------------------------------------------
    # Schema Comparison
    # ------------------------------------------------------------------

    def _compare_schemas(
        self,
        baseline_columns: list[dict[str, Any]],
        current_columns: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Compare baseline and current schemas.

        Returns:
            Comparison checkpoint and comparison object.
        """

        comparison = (
            self.schema_service.compare_schemas(
                baseline_columns=baseline_columns,
                current_columns=current_columns,
                table_name=self.EXPECTED_TABLE_NAME,
            )
        )

        summary = (
            self.schema_service.summarize(
                comparison
            )
        )

        return {
            "comparison": comparison,
            "checkpoint": {
                "name": "Schema comparison",
                "success": comparison.has_changes,
                "message": (
                    "Schema changes detected: "
                    f"{summary['change_count']}."
                ),
                "summary": summary,
            },
        }

    # ------------------------------------------------------------------
    # Expected Change Validation
    # ------------------------------------------------------------------

    def _validate_expected_change(
        self,
        comparison: SchemaComparisonResult,
    ) -> dict[str, Any]:
        """
        Validate the expected customer_segment addition.

        Returns:
            Validation checkpoint.
        """

        success, message = (
            self.schema_service.validate_expected_addition(
                comparison=comparison,
                column_name=self.EXPECTED_NEW_COLUMN,
                expected_type=(
                    self.EXPECTED_NEW_COLUMN_TYPE
                ),
            )
        )

        return {
            "name": (
                "Expected schema evolution validation"
            ),
            "success": success,
            "message": message,
        }

    # ------------------------------------------------------------------
    # Result Construction
    # ------------------------------------------------------------------

    @staticmethod
    def _build_result(
        success: bool,
        checkpoints: list[dict[str, Any]],
        comparison: SchemaComparisonResult | None = None,
        baseline_schema: list[dict[str, Any]] | None = None,
        current_schema: list[dict[str, Any]] | None = None,
    ) -> SchemaEvolutionWorkflowResult:
        """
        Build the final workflow result.

        Args:
            success: Overall status.
            checkpoints: Workflow checkpoints.
            comparison: Optional schema comparison.
            baseline_schema: Optional baseline schema.
            current_schema: Optional current schema.

        Returns:
            Structured workflow result.
        """

        return SchemaEvolutionWorkflowResult(
            success=success,
            status="PASS" if success else "FAIL",
            checkpoints=checkpoints,
            comparison=comparison,
            baseline_schema=(
                baseline_schema or []
            ),
            current_schema=(
                current_schema or []
            ),
        )


# ---------------------------------------------------------------------------
# Public Interface
# ---------------------------------------------------------------------------
#
# Public classes:
#   SchemaEvolutionWorkflowResult
#   SchemaEvolutionWorkflow
#
# Public methods:
#   SchemaEvolutionWorkflow.execute()
#
# Dependencies:
#   pyarrow
#   Project GlueService
#   Project S3Service
#   Project CatalogService
#   Project SchemaEvolutionService
#
# Responsibility:
#   Orchestrate the controlled customers schema-evolution demonstration.
# ---------------------------------------------------------------------------


__all__ = [
    "SchemaEvolutionWorkflowResult",
    "SchemaEvolutionWorkflow",
]