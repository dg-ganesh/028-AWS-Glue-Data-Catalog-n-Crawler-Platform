"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Glue Data Catalog Service
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ColumnMetadata:
    """
    Represents one column discovered in the Glue Data Catalog.
    """

    name: str
    data_type: str
    comment: str = ""


@dataclass
class TableMetadata:
    """
    Represents one Glue Data Catalog table.
    """

    name: str
    database_name: str
    table_type: str
    s3_location: str
    columns: list[ColumnMetadata] = field(default_factory=list)
    partitions: list[ColumnMetadata] = field(default_factory=list)


@dataclass
class CatalogMetadataResult:
    """
    Represents the result of Catalog metadata processing.
    """

    success: bool
    message: str
    tables: list[TableMetadata] = field(default_factory=list)


class CatalogService:
    """
    Converts AWS Glue Data Catalog responses into
    project-level metadata models.
    """

    def parse_table(
        self,
        table_data: dict[str, Any],
    ) -> TableMetadata:
        """
        Convert one raw Glue table response into TableMetadata.

        Args:
            table_data: Raw table dictionary returned by Glue.

        Returns:
            Parsed TableMetadata instance.
        """

        storage_descriptor = table_data.get(
            "StorageDescriptor",
            {},
        )

        columns = self._parse_columns(
            storage_descriptor.get(
                "Columns",
                [],
            )
        )

        partitions = self._parse_columns(
            table_data.get(
                "PartitionKeys",
                [],
            )
        )

        return TableMetadata(
            name=table_data.get(
                "Name",
                "",
            ),
            database_name=table_data.get(
                "DatabaseName",
                "",
            ),
            table_type=table_data.get(
                "TableType",
                "",
            ),
            s3_location=storage_descriptor.get(
                "Location",
                "",
            ),
            columns=columns,
            partitions=partitions,
        )

    def parse_tables(
        self,
        tables_data: list[dict[str, Any]],
    ) -> list[TableMetadata]:
        """
        Convert multiple raw Glue table responses.

        Args:
            tables_data: List of raw Glue table dictionaries.

        Returns:
            List of parsed TableMetadata objects.
        """

        return [
            self.parse_table(table)
            for table in tables_data
        ]

    def build_catalog_metadata(
        self,
        database_name: str,
        tables_data: list[dict[str, Any]],
    ) -> CatalogMetadataResult:
        """
        Build a complete project-level Catalog metadata result.

        Args:
            database_name: Glue database name.
            tables_data: Raw Glue table metadata.

        Returns:
            CatalogMetadataResult containing parsed tables.
        """

        tables = self.parse_tables(
            tables_data,
        )

        if not tables:
            return CatalogMetadataResult(
                success=False,
                message=(
                    f"No tables were discovered in "
                    f"Glue database '{database_name}'."
                ),
            )

        return CatalogMetadataResult(
            success=True,
            message=(
                f"Discovered {len(tables)} table(s) "
                f"in Glue database '{database_name}'."
            ),
            tables=tables,
        )

    def find_table(
        self,
        tables: list[TableMetadata],
        table_name: str,
    ) -> TableMetadata | None:
        """
        Find a table by name.

        Args:
            tables: Parsed Catalog tables.
            table_name: Table name to locate.

        Returns:
            Matching TableMetadata or None.
        """

        normalized_name = table_name.strip().lower()

        for table in tables:
            if table.name.lower() == normalized_name:
                return table

        return None

    def table_exists(
        self,
        tables: list[TableMetadata],
        table_name: str,
    ) -> bool:
        """
        Determine whether a table exists.
        """

        return (
            self.find_table(
                tables,
                table_name,
            )
            is not None
        )

    def get_column(
        self,
        table: TableMetadata,
        column_name: str,
    ) -> ColumnMetadata | None:
        """
        Find a column within a Catalog table.
        """

        normalized_name = column_name.strip().lower()

        for column in table.columns:
            if column.name.lower() == normalized_name:
                return column

        return None

    def validate_required_columns(
        self,
        table: TableMetadata,
        required_columns: list[str],
    ) -> tuple[bool, list[str]]:
        """
        Validate that a table contains required columns.

        Args:
            table: Catalog table metadata.
            required_columns: Expected column names.

        Returns:
            Tuple containing validation status and errors.
        """

        errors: list[str] = []

        actual_columns = {
            column.name.lower()
            for column in table.columns
        }

        for required_column in required_columns:
            if required_column.lower() not in actual_columns:
                errors.append(
                    f"Required column '{required_column}' "
                    f"is missing from table '{table.name}'."
                )

        return len(errors) == 0, errors

    def validate_table_location(
        self,
        table: TableMetadata,
        expected_s3_prefix: str,
    ) -> tuple[bool, str]:
        """
        Validate that a Catalog table points to the expected S3 prefix.

        Args:
            table: Catalog table metadata.
            expected_s3_prefix: Expected S3 location prefix.

        Returns:
            Tuple containing validation status and message.
        """

        actual_location = table.s3_location.rstrip(
            "/"
        ).lower()

        expected_location = expected_s3_prefix.rstrip(
            "/"
        ).lower()

        if actual_location.startswith(
            expected_location
        ):
            return (
                True,
                (
                    f"Table '{table.name}' points to "
                    f"expected S3 location."
                ),
            )

        return (
            False,
            (
                f"Table '{table.name}' location mismatch. "
                f"Expected prefix '{expected_s3_prefix}', "
                f"actual location '{table.s3_location}'."
            ),
        )

    def summarize_table(
        self,
        table: TableMetadata,
    ) -> str:
        """
        Produce a concise human-readable table summary.
        """

        column_summary = ", ".join(
            f"{column.name}:{column.data_type}"
            for column in table.columns
        )

        partition_summary = ", ".join(
            f"{partition.name}:{partition.data_type}"
            for partition in table.partitions
        )

        summary = (
            f"Table: {table.name}\n"
            f"Database: {table.database_name}\n"
            f"Type: {table.table_type}\n"
            f"S3 Location: {table.s3_location}\n"
            f"Columns: {column_summary}"
        )

        if partition_summary:
            summary += (
                f"\nPartitions: {partition_summary}"
            )

        return summary

    def summarize_catalog(
        self,
        tables: list[TableMetadata],
    ) -> str:
        """
        Produce a human-readable summary of the Catalog.
        """

        if not tables:
            return "No Catalog tables discovered."

        summaries = [
            self.summarize_table(table)
            for table in tables
        ]

        return "\n\n".join(summaries)

    # ------------------------------------------------------------------
    # Internal Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_columns(
        columns_data: list[dict[str, Any]],
    ) -> list[ColumnMetadata]:
        """
        Convert raw Glue column definitions into ColumnMetadata.
        """

        columns: list[ColumnMetadata] = []

        for column in columns_data:
            columns.append(
                ColumnMetadata(
                    name=column.get(
                        "Name",
                        "",
                    ),
                    data_type=column.get(
                        "Type",
                        "",
                    ),
                    comment=column.get(
                        "Comment",
                        "",
                    ) or "",
                )
            )

        return columns


# ---------------------------------------------------------------------------
# Public Interface
# ---------------------------------------------------------------------------
#
# Public data models:
#   ColumnMetadata
#   TableMetadata
#   CatalogMetadataResult
#
# Public class:
#   CatalogService
#
# CatalogService public methods:
#   parse_table()
#   parse_tables()
#   build_catalog_metadata()
#   find_table()
#   table_exists()
#   get_column()
#   validate_required_columns()
#   validate_table_location()
#   summarize_table()
#   summarize_catalog()
#
# External dependencies:
#   Python standard library only.
# ---------------------------------------------------------------------------