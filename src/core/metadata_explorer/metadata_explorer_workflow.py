"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Metadata Explorer Workflow
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.config import GLUE_DATABASE_NAME
from src.services.catalog_service import (
    CatalogService,
    CatalogMetadataResult,
)
from src.services.glue_service import GlueService


@dataclass(frozen=True)
class MetadataExplorerResult:
    """
    Represents the result of the metadata exploration workflow.
    """

    success: bool
    database_name: str
    table_count: int = 0
    column_count: int = 0
    metadata: CatalogMetadataResult | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    message: str = ""


class MetadataExplorerWorkflow:
    """
    Explore metadata exposed by the AWS Glue Data Catalog.

    The workflow is intentionally read-only. AWS interaction is delegated
    to GlueService and metadata interpretation is delegated to
    CatalogService.
    """

    def __init__(
        self,
        glue_service: GlueService | None = None,
        catalog_service: CatalogService | None = None,
    ) -> None:
        """
        Initialize the metadata explorer workflow.

        Args:
            glue_service: Optional Glue service instance.
            catalog_service: Optional catalog service instance.
        """
        self.glue_service = glue_service or GlueService()
        self.catalog_service = (
            catalog_service or CatalogService()
        )

    # ------------------------------------------------------------------
    # Main Workflow
    # ------------------------------------------------------------------

    def execute(self) -> dict[str, Any]:
        """
        Execute the metadata exploration workflow.

        Returns:
            Dictionary containing workflow status, metadata and summary.
        """
        configuration_result = self._validate_configuration()

        if not configuration_result["success"]:
            return configuration_result

        database_result = self._retrieve_database()

        if not database_result["success"]:
            return database_result

        tables_result = self._retrieve_tables()

        if not tables_result["success"]:
            return tables_result

        metadata_result = self._build_metadata(
            tables_result["tables"],
        )

        if not metadata_result["success"]:
            return metadata_result

        validation_result = self._validate_metadata(
            metadata_result["metadata"],
        )

        if not validation_result["success"]:
            return validation_result

        summary = metadata_result["summary"]

        return {
            "success": True,
            "name": "Metadata Explorer",
            "message": (
                "Glue Data Catalog metadata explored successfully."
            ),
            "database_name": GLUE_DATABASE_NAME,
            "table_count": summary.get("table_count", 0),
            "column_count": summary.get("column_count", 0),
            "metadata": metadata_result["metadata"],
            "summary": summary,
        }

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def _validate_configuration(self) -> dict[str, Any]:
        """
        Validate that the configured Glue database name is available.

        Returns:
            Configuration checkpoint.
        """
        if not GLUE_DATABASE_NAME:
            return {
                "success": False,
                "name": "Metadata explorer configuration",
                "message": (
                    "Glue database name is not configured."
                ),
            }

        return {
            "success": True,
            "name": "Metadata explorer configuration",
            "message": "Metadata explorer configuration is valid.",
        }

    # ------------------------------------------------------------------
    # Glue Catalog Retrieval
    # ------------------------------------------------------------------

    def _retrieve_database(self) -> dict[str, Any]:
        """
        Verify that the configured Glue database exists.

        Returns:
            Database retrieval checkpoint.
        """
        result = self.glue_service.database_exists(
            GLUE_DATABASE_NAME,
        )

        if not result.success:
            return {
                "success": False,
                "name": "Metadata database validation",
                "message": result.message,
            }

        if not result.data.get("exists", False):
            return {
                "success": False,
                "name": "Metadata database validation",
                "message": (
                    f"Glue database '{GLUE_DATABASE_NAME}' "
                    "does not exist."
                ),
            }

        return {
            "success": True,
            "name": "Metadata database validation",
            "message": (
                f"Glue database '{GLUE_DATABASE_NAME}' is available."
            ),
        }

    def _retrieve_tables(self) -> dict[str, Any]:
        """
        Retrieve all tables from the Glue Data Catalog.

        Returns:
            Table retrieval checkpoint.
        """
        result = self.glue_service.get_tables(
            database_name=GLUE_DATABASE_NAME,
        )

        if not result.success:
            return {
                "success": False,
                "name": "Metadata table retrieval",
                "message": result.message,
            }

        tables = result.data.get("tables", [])

        return {
            "success": True,
            "name": "Metadata table retrieval",
            "message": (
                f"Retrieved {len(tables)} table(s) from "
                f"Glue database '{GLUE_DATABASE_NAME}'."
            ),
            "tables": tables,
        }

    # ------------------------------------------------------------------
    # Metadata Interpretation
    # ------------------------------------------------------------------

    def _build_metadata(
        self,
        tables: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Convert Glue table payloads into project metadata models.

        Args:
            tables: Raw Glue table definitions.

        Returns:
            Metadata construction checkpoint.
        """
        try:
            metadata = self.catalog_service.build_catalog_metadata(
                database_name=GLUE_DATABASE_NAME,
                tables=tables,
            )

            summary = self.catalog_service.summarize_catalog(
                metadata,
            )

            return {
                "success": True,
                "name": "Metadata model construction",
                "message": (
                    "Glue Catalog metadata model built successfully."
                ),
                "metadata": metadata,
                "summary": summary,
            }

        except Exception as exc:
            return {
                "success": False,
                "name": "Metadata model construction",
                "message": (
                    "Failed to build catalog metadata: "
                    f"{exc}"
                ),
            }

    def _validate_metadata(
        self,
        metadata: CatalogMetadataResult,
    ) -> dict[str, Any]:
        """
        Validate the basic completeness of the catalog metadata.

        Args:
            metadata: Parsed catalog metadata.

        Returns:
            Metadata validation checkpoint.
        """
        if metadata is None:
            return {
                "success": False,
                "name": "Metadata validation",
                "message": "No catalog metadata was produced.",
            }

        tables = getattr(metadata, "tables", [])

        if not tables:
            return {
                "success": False,
                "name": "Metadata validation",
                "message": (
                    "Glue Catalog contains no discoverable tables."
                ),
            }

        invalid_tables: list[str] = []

        for table in tables:
            table_name = getattr(table, "name", "unknown")
            columns = getattr(table, "columns", [])

            if not columns:
                invalid_tables.append(str(table_name))

        if invalid_tables:
            return {
                "success": False,
                "name": "Metadata validation",
                "message": (
                    "The following tables contain no columns: "
                    + ", ".join(sorted(invalid_tables))
                ),
            }

        return {
            "success": True,
            "name": "Metadata validation",
            "message": (
                f"Validated {len(tables)} catalog table(s) "
                "with discoverable columns."
            ),
        }

    # ------------------------------------------------------------------
    # Human-Readable Output
    # ------------------------------------------------------------------

    @staticmethod
    def format_summary(
        result: dict[str, Any],
    ) -> str:
        """
        Format metadata exploration results for console output.

        Args:
            result: Metadata explorer result dictionary.

        Returns:
            Human-readable metadata summary.
        """
        if not result.get("success", False):
            return (
                "Metadata Explorer FAILED: "
                f"{result.get('message', 'Unknown error')}"
            )

        metadata = result.get("metadata")

        lines = [
            "Glue Data Catalog Metadata Explorer",
            "-" * 48,
            f"Database : {result.get('database_name', '')}",
            f"Tables   : {result.get('table_count', 0)}",
            f"Columns  : {result.get('column_count', 0)}",
            "",
        ]

        if metadata is not None:
            tables = getattr(metadata, "tables", [])

            for table in tables:
                table_name = getattr(table, "name", "unknown")
                columns = getattr(table, "columns", [])
                location = getattr(table, "location", "")

                lines.append(f"Table    : {table_name}")
                lines.append(f"Location : {location}")
                lines.append(f"Columns  : {len(columns)}")

                for column in columns:
                    column_name = getattr(
                        column,
                        "name",
                        "unknown",
                    )
                    data_type = getattr(
                        column,
                        "data_type",
                        "unknown",
                    )

                    lines.append(
                        f"  - {column_name}: {data_type}"
                    )

                lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Public Interface
    # ------------------------------------------------------------------

    def get_catalog_metadata(self) -> dict[str, Any]:
        """
        Execute metadata exploration and return the result.

        Returns:
            Metadata explorer result.
        """
        return self.execute()

    def get_summary(
        self,
        result: dict[str, Any],
    ) -> str:
        """
        Format an existing metadata explorer result.

        Args:
            result: Metadata explorer result.

        Returns:
            Human-readable summary.
        """
        return self.format_summary(result)


# ==============================================================================
# Public Methods
# ==============================================================================
#
# MetadataExplorerWorkflow.execute()
# MetadataExplorerWorkflow.get_catalog_metadata()
# MetadataExplorerWorkflow.get_summary()
# MetadataExplorerWorkflow.format_summary()
#
# ==============================================================================

__all__ = [
    "MetadataExplorerResult",
    "MetadataExplorerWorkflow",
]