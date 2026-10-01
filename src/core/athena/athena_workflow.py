"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Athena Workflow
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.config import (
    GLUE_DATABASE_NAME,
    validate_glue_configuration,
)
from src.services.athena_service import (
    AthenaQueryResult,
    AthenaService,
)


@dataclass
class AthenaWorkflowResult:
    """
    Represent the result of the Athena workflow.
    """

    success: bool
    message: str
    queries: list[AthenaQueryResult]


class AthenaWorkflow:
    """
    Orchestrate predefined Athena analytical queries.

    The workflow is responsible for:
        - validating Athena-related configuration
        - executing predefined SQL queries
        - validating query completion
        - validating returned results
        - exposing query results to the caller

    Athena API interaction remains inside AthenaService.
    """

    CUSTOMER_COUNT_QUERY = """
        SELECT COUNT(*) AS customer_count
        FROM customers
    """

    TRANSACTION_COUNT_QUERY = """
        SELECT COUNT(*) AS transaction_count
        FROM transactions
    """

    TRANSACTION_AGGREGATION_QUERY = """
        SELECT
            SUM(amount) AS total_transaction_amount,
            AVG(amount) AS average_transaction_amount,
            COUNT(*) AS transaction_count
        FROM transactions
    """

    def __init__(
        self,
        athena_service: AthenaService | None = None,
    ) -> None:
        """
        Initialize the Athena workflow.

        Args:
            athena_service:
                Optional Athena service instance.
        """
        self.athena_service = (
            athena_service
            or AthenaService()
        )

        self._query_results: list[AthenaQueryResult] = []

    # ------------------------------------------------------------------
    # Main Workflow
    # ------------------------------------------------------------------

    def execute(self) -> dict[str, Any]:
        """
        Execute the complete Athena analytical workflow.

        Returns:
            Dictionary containing workflow status,
            checkpoints, and query results.
        """

        checkpoints: list[dict[str, Any]] = []

        configuration_result = self._validate_configuration()

        checkpoints.append(configuration_result)

        if not configuration_result["success"]:
            return self._build_result(
                success=False,
                message=(
                    "Athena configuration validation failed."
                ),
                checkpoints=checkpoints,
            )

        query_definitions = [
            (
                "Customer count query",
                self.CUSTOMER_COUNT_QUERY,
            ),
            (
                "Transaction count query",
                self.TRANSACTION_COUNT_QUERY,
            ),
            (
                "Transaction aggregation query",
                self.TRANSACTION_AGGREGATION_QUERY,
            ),
        ]

        self._query_results = []

        for query_name, sql in query_definitions:

            query_checkpoint = self._execute_query(
                query_name=query_name,
                sql=sql,
            )

            checkpoints.append(query_checkpoint)

            if not query_checkpoint["success"]:
                return self._build_result(
                    success=False,
                    message=(
                        f"Athena workflow failed at "
                        f"{query_name}."
                    ),
                    checkpoints=checkpoints,
                )

        return self._build_result(
            success=True,
            message=(
                "Athena analytical workflow completed "
                "successfully."
            ),
            checkpoints=checkpoints,
        )

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def _validate_configuration(self) -> dict[str, Any]:
        """
        Validate configuration required by Project 028.

        Returns:
            Configuration checkpoint.
        """

        valid, errors = validate_glue_configuration()

        if not valid:
            return {
                "name": "Athena configuration validation",
                "success": False,
                "message": (
                    "Glue/Athena configuration validation failed."
                ),
                "errors": errors,
            }

        return {
            "name": "Athena configuration validation",
            "success": True,
            "message": (
                "Athena configuration is valid."
            ),
        }

    # ------------------------------------------------------------------
    # Query Execution
    # ------------------------------------------------------------------

    def _execute_query(
        self,
        query_name: str,
        sql: str,
    ) -> dict[str, Any]:
        """
        Execute and validate one Athena query.

        Args:
            query_name:
                Human-readable query name.

            sql:
                SQL statement to execute.

        Returns:
            Query execution checkpoint.
        """

        normalized_sql = " ".join(
            sql.split()
        )

        if not normalized_sql:
            return {
                "name": query_name,
                "success": False,
                "message": "SQL query is empty.",
            }

        try:
            result = self.athena_service.execute_query(
                query=normalized_sql,
                database_name=GLUE_DATABASE_NAME,
            )

        except Exception as exc:
            return {
                "name": query_name,
                "success": False,
                "message": (
                    f"Athena query execution failed: {exc}"
                ),
                "errors": [str(exc)],
            }

        if not isinstance(result, AthenaQueryResult):
            return {
                "name": query_name,
                "success": False,
                "message": (
                    "Athena service returned an unexpected "
                    "query result type."
                ),
            }

        self._query_results.append(result)

        if not result.success:
            return {
                "name": query_name,
                "success": False,
                "message": result.message,
                "errors": (
                    [result.message]
                    if result.message
                    else []
                ),
            }

        if not result.rows:
            return {
                "name": query_name,
                "success": False,
                "message": (
                    "Athena query succeeded but returned "
                    "no result rows."
                ),
            }

        return {
            "name": query_name,
            "success": True,
            "message": (
                f"{query_name} completed successfully."
            ),
            "query_execution_id": (
                result.query_execution_id
            ),
            "row_count": len(result.rows),
            "rows": result.rows,
        }

    # ------------------------------------------------------------------
    # Result Construction
    # ------------------------------------------------------------------

    def _build_result(
        self,
        success: bool,
        message: str,
        checkpoints: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Build the workflow result.

        Args:
            success:
                Overall workflow status.

            message:
                Workflow summary.

            checkpoints:
                Workflow checkpoints.

        Returns:
            Workflow result dictionary.
        """

        return {
            "success": success,
            "message": message,
            "database": GLUE_DATABASE_NAME,
            "queries": self._query_results,
            "checkpoints": checkpoints,
        }

    # ------------------------------------------------------------------
    # Public Interface
    # ------------------------------------------------------------------

    def get_query_results(self) -> list[AthenaQueryResult]:
        """
        Return the results generated by the latest execution.
        """

        return list(self._query_results)

    def get_customer_count_result(
        self,
    ) -> AthenaQueryResult | None:
        """
        Return the customer-count query result.
        """

        if len(self._query_results) < 1:
            return None

        return self._query_results[0]

    def get_transaction_count_result(
        self,
    ) -> AthenaQueryResult | None:
        """
        Return the transaction-count query result.
        """

        if len(self._query_results) < 2:
            return None

        return self._query_results[1]

    def get_transaction_aggregation_result(
        self,
    ) -> AthenaQueryResult | None:
        """
        Return the transaction aggregation result.
        """

        if len(self._query_results) < 3:
            return None

        return self._query_results[2]


__all__ = [
    "AthenaWorkflow",
    "AthenaWorkflowResult",
]