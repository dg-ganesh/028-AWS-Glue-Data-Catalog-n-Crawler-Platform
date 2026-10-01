"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Athena Service
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.config import (
    AWS_REGION,
    GLUE_DATABASE_NAME,
    S3_BUCKET_NAME,
)


# ==============================================================================
# Data Models
# ==============================================================================


@dataclass(frozen=True)
class AthenaOperationResult:
    """
    Represents the result of an Athena operation.
    """

    success: bool
    message: str
    data: dict[str, Any]


@dataclass(frozen=True)
class AthenaQueryResult:
    """
    Represents the normalized result of an Athena query.
    """

    success: bool
    query_execution_id: str
    state: str
    columns: list[str]
    rows: list[dict[str, Any]]
    message: str


# ==============================================================================
# Athena Service
# ==============================================================================


class AthenaService:
    """
    Provide controlled access to Amazon Athena.

    Responsibilities:
        - Athena client creation.
        - SQL query submission.
        - Query execution polling.
        - Query result retrieval.
        - Athena response normalization.

    The service does not contain business-specific SQL.
    """

    DEFAULT_OUTPUT_PREFIX = "project-028/athena-results/"

    TERMINAL_STATES = {
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    }

    def __init__(
        self,
        region_name: str | None = None,
        database_name: str | None = None,
        output_location: str | None = None,
        client: Any | None = None,
    ) -> None:
        """
        Initialize the Athena service.

        Args:
            region_name:
                AWS region used by Athena.
            database_name:
                Default Glue Data Catalog database.
            output_location:
                S3 location where Athena stores query results.
            client:
                Optional boto3 Athena client, primarily useful for testing.
        """
        self.region_name = (
            region_name
            or AWS_REGION
        )

        self.database_name = (
            database_name
            or GLUE_DATABASE_NAME
        )

        self.output_location = (
            output_location
            or os.getenv(
                "AWS_ATHENA_OUTPUT_LOCATION",
                "",
            ).strip()
        )

        if not self.output_location:
            if not S3_BUCKET_NAME:
                raise ValueError(
                    "S3 bucket name is required to construct "
                    "the Athena output location."
                )

            self.output_location = (
                f"s3://{S3_BUCKET_NAME}/"
                f"{self.DEFAULT_OUTPUT_PREFIX}"
            )

        self._client = client

    # ------------------------------------------------------------------
    # Client
    # ------------------------------------------------------------------

    @property
    def client(self) -> Any:
        """
        Return the boto3 Athena client.

        Returns:
            boto3 Athena client.
        """
        if self._client is None:
            self._client = boto3.client(
                "athena",
                region_name=self.region_name,
            )

        return self._client

    # ------------------------------------------------------------------
    # Query Submission
    # ------------------------------------------------------------------

    def start_query(
        self,
        query: str,
        database_name: str | None = None,
    ) -> AthenaOperationResult:
        """
        Submit a SQL query to Athena.

        Args:
            query:
                SQL query to execute.
            database_name:
                Optional Glue Catalog database override.

        Returns:
            AthenaOperationResult containing the query execution ID.
        """
        normalized_query = query.strip()

        if not normalized_query:
            return AthenaOperationResult(
                success=False,
                message="Athena query cannot be empty.",
                data={},
            )

        database = (
            database_name
            or self.database_name
        )

        if not database:
            return AthenaOperationResult(
                success=False,
                message="Athena database name is not configured.",
                data={},
            )

        try:
            response = self.client.start_query_execution(
                QueryString=normalized_query,
                QueryExecutionContext={
                    "Database": database,
                },
                ResultConfiguration={
                    "OutputLocation": self.output_location,
                },
            )

            execution_id = response.get(
                "QueryExecutionId",
                "",
            )

            if not execution_id:
                return AthenaOperationResult(
                    success=False,
                    message=(
                        "Athena did not return a "
                        "query execution ID."
                    ),
                    data=response,
                )

            return AthenaOperationResult(
                success=True,
                message=(
                    "Athena query submitted successfully."
                ),
                data={
                    "query_execution_id": execution_id,
                    "query": normalized_query,
                    "database": database,
                    "output_location": self.output_location,
                },
            )

        except (ClientError, BotoCoreError) as exc:
            return AthenaOperationResult(
                success=False,
                message=self._format_aws_error(
                    "Athena query submission",
                    exc,
                ),
                data={},
            )

    # ------------------------------------------------------------------
    # Query Status
    # ------------------------------------------------------------------

    def get_query_status(
        self,
        query_execution_id: str,
    ) -> AthenaOperationResult:
        """
        Retrieve the current Athena query status.

        Args:
            query_execution_id:
                Athena query execution identifier.

        Returns:
            AthenaOperationResult containing status information.
        """
        if not query_execution_id:
            return AthenaOperationResult(
                success=False,
                message="Query execution ID is required.",
                data={},
            )

        try:
            response = self.client.get_query_execution(
                QueryExecutionId=query_execution_id,
            )

            execution = response.get(
                "QueryExecution",
                {},
            )

            status = execution.get(
                "Status",
                {},
            )

            statistics = execution.get(
                "Statistics",
                {},
            )

            state = status.get(
                "State",
                "",
            )

            return AthenaOperationResult(
                success=True,
                message="Athena query status retrieved.",
                data={
                    "query_execution_id": query_execution_id,
                    "state": state,
                    "state_change_reason": status.get(
                        "StateChangeReason",
                        "",
                    ),
                    "submission_time": status.get(
                        "SubmissionDateTime",
                    ),
                    "completion_time": status.get(
                        "CompletionDateTime",
                    ),
                    "data_scanned_bytes": statistics.get(
                        "DataScannedInBytes",
                        0,
                    ),
                    "execution_time_millis": statistics.get(
                        "EngineExecutionTimeInMillis",
                        0,
                    ),
                    "query_execution": execution,
                },
            )

        except (ClientError, BotoCoreError) as exc:
            return AthenaOperationResult(
                success=False,
                message=self._format_aws_error(
                    "Athena query status retrieval",
                    exc,
                ),
                data={},
            )

    # ------------------------------------------------------------------
    # Query Polling
    # ------------------------------------------------------------------

    def wait_for_query(
        self,
        query_execution_id: str,
        timeout_seconds: int = 600,
        poll_interval_seconds: int = 2,
    ) -> AthenaOperationResult:
        """
        Wait until an Athena query reaches a terminal state.

        Args:
            query_execution_id:
                Athena query execution identifier.
            timeout_seconds:
                Maximum time to wait.
            poll_interval_seconds:
                Delay between status checks.

        Returns:
            AthenaOperationResult containing final query status.
        """
        if timeout_seconds <= 0:
            return AthenaOperationResult(
                success=False,
                message="Timeout must be greater than zero.",
                data={},
            )

        if poll_interval_seconds <= 0:
            return AthenaOperationResult(
                success=False,
                message=(
                    "Poll interval must be greater than zero."
                ),
                data={},
            )

        start_time = time.monotonic()

        while True:
            status_result = self.get_query_status(
                query_execution_id,
            )

            if not status_result.success:
                return status_result

            state = status_result.data.get(
                "state",
                "",
            )

            if state in self.TERMINAL_STATES:
                if state == "SUCCEEDED":
                    return AthenaOperationResult(
                        success=True,
                        message=(
                            "Athena query completed successfully."
                        ),
                        data=status_result.data,
                    )

                return AthenaOperationResult(
                    success=False,
                    message=(
                        f"Athena query ended with state "
                        f"'{state}'."
                    ),
                    data=status_result.data,
                )

            elapsed_seconds = (
                time.monotonic() - start_time
            )

            if elapsed_seconds >= timeout_seconds:
                return AthenaOperationResult(
                    success=False,
                    message=(
                        "Athena query polling timed out "
                        f"after {timeout_seconds} seconds."
                    ),
                    data=status_result.data,
                )

            time.sleep(poll_interval_seconds)

    # ------------------------------------------------------------------
    # Result Retrieval
    # ------------------------------------------------------------------

    def get_query_results(
        self,
        query_execution_id: str,
    ) -> AthenaOperationResult:
        """
        Retrieve all available Athena query results.

        Args:
            query_execution_id:
                Athena query execution identifier.

        Returns:
            AthenaOperationResult containing normalized rows.
        """
        if not query_execution_id:
            return AthenaOperationResult(
                success=False,
                message="Query execution ID is required.",
                data={},
            )

        try:
            rows: list[list[str | None]] = []
            column_info: list[dict[str, Any]] = []

            next_token: str | None = None
            first_response = True

            while True:
                request: dict[str, Any] = {
                    "QueryExecutionId": query_execution_id,
                }

                if next_token:
                    request["NextToken"] = next_token

                response = self.client.get_query_results(
                    **request,
                )

                if first_response:
                    result_set = response.get(
                        "ResultSet",
                        {},
                    )

                    metadata = result_set.get(
                        "ResultSetMetadata",
                        {},
                    )

                    column_info = metadata.get(
                        "ColumnInfo",
                        [],
                    )

                    first_response = False

                result_set = response.get(
                    "ResultSet",
                    {},
                )

                response_rows = result_set.get(
                    "Rows",
                    [],
                )

                for row in response_rows:
                    values = row.get(
                        "Data",
                        [],
                    )

                    rows.append([
                        value.get("VarCharValue")
                        if value is not None
                        else None
                        for value in values
                    ])

                next_token = response.get(
                    "NextToken",
                )

                if not next_token:
                    break

            columns = [
                str(column.get("Name", ""))
                for column in column_info
            ]

            normalized_rows = self._normalize_rows(
                columns,
                rows,
            )

            return AthenaOperationResult(
                success=True,
                message=(
                    "Athena query results retrieved successfully."
                ),
                data={
                    "query_execution_id": query_execution_id,
                    "columns": columns,
                    "rows": normalized_rows,
                    "row_count": len(normalized_rows),
                },
            )

        except (ClientError, BotoCoreError) as exc:
            return AthenaOperationResult(
                success=False,
                message=self._format_aws_error(
                    "Athena query result retrieval",
                    exc,
                ),
                data={},
            )

    # ------------------------------------------------------------------
    # Complete Query Execution
    # ------------------------------------------------------------------

    def execute_query(
        self,
        query: str,
        database_name: str | None = None,
        timeout_seconds: int = 600,
        poll_interval_seconds: int = 2,
    ) -> AthenaQueryResult:
        """
        Submit, execute and retrieve an Athena query.

        Args:
            query:
                SQL query.
            database_name:
                Optional Glue Catalog database.
            timeout_seconds:
                Maximum execution wait time.
            poll_interval_seconds:
                Delay between status checks.

        Returns:
            AthenaQueryResult containing normalized query results.
        """
        start_result = self.start_query(
            query=query,
            database_name=database_name,
        )

        if not start_result.success:
            return AthenaQueryResult(
                success=False,
                query_execution_id="",
                state="SUBMISSION_FAILED",
                columns=[],
                rows=[],
                message=start_result.message,
            )

        execution_id = start_result.data[
            "query_execution_id"
        ]

        wait_result = self.wait_for_query(
            query_execution_id=execution_id,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )

        final_state = wait_result.data.get(
            "state",
            "UNKNOWN",
        )

        if not wait_result.success:
            return AthenaQueryResult(
                success=False,
                query_execution_id=execution_id,
                state=final_state,
                columns=[],
                rows=[],
                message=wait_result.message,
            )

        results = self.get_query_results(
            query_execution_id=execution_id,
        )

        if not results.success:
            return AthenaQueryResult(
                success=False,
                query_execution_id=execution_id,
                state=final_state,
                columns=[],
                rows=[],
                message=results.message,
            )

        return AthenaQueryResult(
            success=True,
            query_execution_id=execution_id,
            state=final_state,
            columns=results.data.get(
                "columns",
                [],
            ),
            rows=results.data.get(
                "rows",
                [],
            ),
            message=(
                "Athena query executed and results "
                "retrieved successfully."
            ),
        )

    # ------------------------------------------------------------------
    # Result Normalization
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_rows(
        columns: list[str],
        rows: list[list[str | None]],
    ) -> list[dict[str, Any]]:
        """
        Convert Athena row arrays into dictionaries.

        Athena includes the column-header row as the first row.
        That row is removed when it matches the returned column names.

        Args:
            columns:
                Athena column names.
            rows:
                Raw Athena row values.

        Returns:
            List of dictionaries.
        """
        if not rows:
            return []

        data_rows = rows

        if data_rows:
            header = [
                "" if value is None else str(value)
                for value in data_rows[0]
            ]

            if header[:len(columns)] == columns:
                data_rows = data_rows[1:]

        normalized: list[dict[str, Any]] = []

        for row in data_rows:
            record: dict[str, Any] = {}

            for index, column in enumerate(columns):
                value = (
                    row[index]
                    if index < len(row)
                    else None
                )

                record[column] = value

            normalized.append(record)

        return normalized

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_configuration(self) -> tuple[bool, list[str]]:
        """
        Validate the configuration required by Athena.

        Returns:
            Tuple containing validation status and errors.
        """
        errors: list[str] = []

        if not self.region_name:
            errors.append(
                "AWS region is not configured."
            )

        if not self.database_name:
            errors.append(
                "Glue database name is not configured."
            )

        if not self.output_location:
            errors.append(
                "Athena output location is not configured."
            )

        elif not self.output_location.startswith(
            "s3://",
        ):
            errors.append(
                "Athena output location must be an S3 URI."
            )

        return (
            len(errors) == 0,
            errors,
        )

    # ------------------------------------------------------------------
    # Error Handling
    # ------------------------------------------------------------------

    @staticmethod
    def _format_aws_error(
        operation: str,
        error: Exception,
    ) -> str:
        """
        Format an AWS SDK exception into a readable message.

        Args:
            operation:
                Name of the AWS operation.
            error:
                Raised exception.

        Returns:
            Formatted error message.
        """
        if isinstance(error, ClientError):
            error_data = error.response.get(
                "Error",
                {},
            )

            code = error_data.get(
                "Code",
                "Unknown",
            )

            message = error_data.get(
                "Message",
                str(error),
            )

            return (
                f"{operation} failed "
                f"[{code}]: {message}"
            )

        return f"{operation} failed: {error}"


# ==============================================================================
# Public Interface
# ==============================================================================
#
# AthenaOperationResult
# AthenaQueryResult
#
# AthenaService.client
# AthenaService.start_query()
# AthenaService.get_query_status()
# AthenaService.wait_for_query()
# AthenaService.get_query_results()
# AthenaService.execute_query()
# AthenaService.validate_configuration()
#
# Dependencies
# ------------------------------------------------------------------------------
# boto3
# botocore
# src.config
#
# ==============================================================================

__all__ = [
    "AthenaOperationResult",
    "AthenaQueryResult",
    "AthenaService",
]