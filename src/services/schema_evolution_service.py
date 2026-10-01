"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Schema Evolution Service
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SchemaColumn:
    """
    Represent a normalized schema column.
    """

    name: str
    data_type: str
    ordinal_position: int = 0


@dataclass(frozen=True)
class SchemaChange:
    """
    Represent a single schema change.
    """

    change_type: str
    column_name: str
    previous_type: str | None = None
    current_type: str | None = None


@dataclass
class SchemaComparisonResult:
    """
    Represent the result of comparing two schemas.
    """

    table_name: str
    changes: list[SchemaChange] = field(
        default_factory=list
    )

    @property
    def has_changes(self) -> bool:
        """
        Return whether any schema changes were detected.
        """

        return bool(self.changes)

    @property
    def added_columns(self) -> list[SchemaChange]:
        """
        Return columns added to the schema.
        """

        return [
            change
            for change in self.changes
            if change.change_type == "ADDED"
        ]

    @property
    def removed_columns(self) -> list[SchemaChange]:
        """
        Return columns removed from the schema.
        """

        return [
            change
            for change in self.changes
            if change.change_type == "REMOVED"
        ]

    @property
    def changed_columns(self) -> list[SchemaChange]:
        """
        Return columns whose data type changed.
        """

        return [
            change
            for change in self.changes
            if change.change_type == "TYPE_CHANGED"
        ]

    @property
    def unchanged_columns(self) -> list[str]:
        """
        Return columns that did not change.

        The comparison result currently records only changes,
        therefore unchanged columns are not retained here.
        """

        return []


class SchemaEvolutionService:
    """
    Compare and interpret Glue Catalog schemas.

    This service contains no AWS API orchestration.
    It operates on normalized schema representations so
    workflow modules can independently obtain schemas
    from AWS Glue and pass them here.
    """

    @staticmethod
    def normalize_columns(
        columns: list[Any],
    ) -> list[SchemaColumn]:
        """
        Normalize raw schema columns.

        Supported inputs:
            - SchemaColumn instances
            - dictionaries containing name/data_type
            - dictionaries containing name/type
            - objects containing name and data_type
            - objects containing name and type

        Args:
            columns:
                Raw schema columns.

        Returns:
            Normalized SchemaColumn objects.

        Raises:
            ValueError:
                If a column has no usable name or data type.
        """

        normalized: list[SchemaColumn] = []

        for position, column in enumerate(columns):

            if isinstance(column, SchemaColumn):
                normalized.append(column)
                continue

            if isinstance(column, dict):
                name = column.get(
                    "name",
                    "",
                )

                data_type = column.get(
                    "data_type",
                    column.get(
                        "type",
                        "",
                    ),
                )

                ordinal_position = column.get(
                    "ordinal_position",
                    position,
                )

            else:
                name = getattr(
                    column,
                    "name",
                    "",
                )

                data_type = getattr(
                    column,
                    "data_type",
                    getattr(
                        column,
                        "type",
                        "",
                    ),
                )

                ordinal_position = getattr(
                    column,
                    "ordinal_position",
                    position,
                )

            normalized_name = str(name).strip()

            normalized_type = str(
                data_type
            ).strip().lower()

            if not normalized_name:
                raise ValueError(
                    "Schema column name cannot be empty."
                )

            if not normalized_type:
                raise ValueError(
                    f"Schema column '{normalized_name}' "
                    "does not have a data type."
                )

            normalized.append(
                SchemaColumn(
                    name=normalized_name,
                    data_type=normalized_type,
                    ordinal_position=int(
                        ordinal_position
                    ),
                )
            )

        return normalized

    def compare_schemas(
        self,
        baseline_columns: list[Any],
        current_columns: list[Any],
        table_name: str,
    ) -> SchemaComparisonResult:
        """
        Compare a baseline schema with a current schema.

        Args:
            baseline_columns:
                Columns from the baseline schema.

            current_columns:
                Columns from the current schema.

            table_name:
                Name of the table being compared.

        Returns:
            Structured schema comparison result.

        Raises:
            ValueError:
                If table name is empty or schema data is invalid.
        """

        normalized_table_name = table_name.strip()

        if not normalized_table_name:
            raise ValueError(
                "Table name cannot be empty."
            )

        baseline = self.normalize_columns(
            baseline_columns
        )

        current = self.normalize_columns(
            current_columns
        )

        baseline_by_name = {
            column.name.lower(): column
            for column in baseline
        }

        current_by_name = {
            column.name.lower(): column
            for column in current
        }

        changes: list[SchemaChange] = []

        for current_name, current_column in (
            current_by_name.items()
        ):

            if current_name not in baseline_by_name:

                changes.append(
                    SchemaChange(
                        change_type="ADDED",
                        column_name=current_column.name,
                        current_type=current_column.data_type,
                    )
                )

                continue

            baseline_column = (
                baseline_by_name[current_name]
            )

            if (
                baseline_column.data_type
                != current_column.data_type
            ):
                changes.append(
                    SchemaChange(
                        change_type="TYPE_CHANGED",
                        column_name=current_column.name,
                        previous_type=(
                            baseline_column.data_type
                        ),
                        current_type=(
                            current_column.data_type
                        ),
                    )
                )

        for baseline_name, baseline_column in (
            baseline_by_name.items()
        ):

            if baseline_name not in current_by_name:

                changes.append(
                    SchemaChange(
                        change_type="REMOVED",
                        column_name=baseline_column.name,
                        previous_type=(
                            baseline_column.data_type
                        ),
                    )
                )

        changes.sort(
            key=lambda change: (
                change.column_name.lower(),
                change.change_type,
            )
        )

        return SchemaComparisonResult(
            table_name=normalized_table_name,
            changes=changes,
        )

    def has_schema_changed(
        self,
        baseline_columns: list[Any],
        current_columns: list[Any],
    ) -> bool:
        """
        Determine whether two schemas differ.
        """

        result = self.compare_schemas(
            baseline_columns=baseline_columns,
            current_columns=current_columns,
            table_name="schema_comparison",
        )

        return result.has_changes

    def get_added_columns(
        self,
        baseline_columns: list[Any],
        current_columns: list[Any],
    ) -> list[SchemaChange]:
        """
        Return columns added to the current schema.
        """

        result = self.compare_schemas(
            baseline_columns=baseline_columns,
            current_columns=current_columns,
            table_name="schema_comparison",
        )

        return result.added_columns

    def get_removed_columns(
        self,
        baseline_columns: list[Any],
        current_columns: list[Any],
    ) -> list[SchemaChange]:
        """
        Return columns removed from the current schema.
        """

        result = self.compare_schemas(
            baseline_columns=baseline_columns,
            current_columns=current_columns,
            table_name="schema_comparison",
        )

        return result.removed_columns

    def get_changed_columns(
        self,
        baseline_columns: list[Any],
        current_columns: list[Any],
    ) -> list[SchemaChange]:
        """
        Return columns whose data type changed.
        """

        result = self.compare_schemas(
            baseline_columns=baseline_columns,
            current_columns=current_columns,
            table_name="schema_comparison",
        )

        return result.changed_columns

    def validate_expected_addition(
        self,
        comparison: SchemaComparisonResult,
        column_name: str,
        expected_type: str,
    ) -> tuple[bool, str]:
        """
        Validate that a specific column was added.

        Args:
            comparison:
                Schema comparison result.

            column_name:
                Expected added column.

            expected_type:
                Expected data type.

        Returns:
            Tuple containing validation status and message.
        """

        normalized_name = (
            column_name.strip().lower()
        )

        normalized_type = (
            expected_type.strip().lower()
        )

        if not normalized_name:
            return (
                False,
                "Expected column name cannot be empty.",
            )

        if not normalized_type:
            return (
                False,
                "Expected column type cannot be empty.",
            )

        matching_changes = [
            change
            for change in comparison.added_columns
            if change.column_name.lower()
            == normalized_name
        ]

        if not matching_changes:
            return (
                False,
                (
                    f"Expected column '{column_name}' "
                    "was not detected."
                ),
            )

        detected_type = (
            matching_changes[0].current_type
            or ""
        ).lower()

        if detected_type != normalized_type:
            return (
                False,
                (
                    f"Column '{column_name}' was detected "
                    f"with type '{detected_type}', "
                    f"expected '{expected_type}'."
                ),
            )

        return (
            True,
            (
                f"Column '{column_name}' was added "
                f"with expected type '{expected_type}'."
            ),
        )

    @staticmethod
    def summarize(
        comparison: SchemaComparisonResult,
    ) -> dict[str, Any]:
        """
        Build a concise schema comparison summary.
        """

        return {
            "table_name": comparison.table_name,
            "has_changes": comparison.has_changes,
            "added_columns": [
                {
                    "name": change.column_name,
                    "type": change.current_type,
                }
                for change in comparison.added_columns
            ],
            "removed_columns": [
                {
                    "name": change.column_name,
                    "type": change.previous_type,
                }
                for change in comparison.removed_columns
            ],
            "changed_columns": [
                {
                    "name": change.column_name,
                    "previous_type": change.previous_type,
                    "current_type": change.current_type,
                }
                for change in comparison.changed_columns
            ],
            "change_count": len(
                comparison.changes
            ),
        }


# ---------------------------------------------------------------------------
# Public Interface
# ---------------------------------------------------------------------------
#
# Public classes:
#     SchemaColumn
#     SchemaChange
#     SchemaComparisonResult
#     SchemaEvolutionService
#
# Public methods:
#     normalize_columns()
#     compare_schemas()
#     has_schema_changed()
#     get_added_columns()
#     get_removed_columns()
#     get_changed_columns()
#     validate_expected_addition()
#     summarize()
#
# Dependencies:
#     Python standard library only.
#
# Responsibility:
#     Normalize and compare data schemas.
#     AWS orchestration belongs to the Slice 3 workflow layer.
# ---------------------------------------------------------------------------


__all__ = [
    "SchemaColumn",
    "SchemaChange",
    "SchemaComparisonResult",
    "SchemaEvolutionService",
]