"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Application Entry Point
"""

from __future__ import annotations

from typing import Any

from src.config import (
    GLUE_DATABASE_NAME,
    PROJECT_ID,
    PROJECT_NAME,
)
from src.core.athena.athena_workflow import (
    AthenaWorkflow,
)
from src.core.cloud_foundation.foundation_workflow import (
    FoundationWorkflow,
)
from src.core.glue_catalog.glue_catalog_workflow import (
    GlueCatalogWorkflow,
)
from src.core.metadata_explorer.metadata_explorer_workflow import (
    MetadataExplorerWorkflow,
)
from src.core.schema_evolution.schema_evolution_workflow import (
    SchemaEvolutionWorkflow,
)
from src.services.execution_report_service import (
    ExecutionReportService,
)


def _print_header() -> None:
    """
    Print the application execution header.
    """

    print(
        f"Project {PROJECT_ID} - "
        f"{PROJECT_NAME}"
    )

    print(
        "Execution Slice 3 - "
        "Schema Evolution, Metadata Explorer & Athena"
    )

    print("-" * 72)


def _print_checkpoint(
    checkpoint: dict[str, Any],
) -> None:
    """
    Print a workflow checkpoint.

    Args:
        checkpoint:
            Checkpoint dictionary.
    """

    name = checkpoint.get(
        "name",
        checkpoint.get(
            "checkpoint",
            "Unknown checkpoint",
        ),
    )

    success = checkpoint.get(
        "success",
        checkpoint.get(
            "status",
            "FAIL",
        ) == "PASS",
    )

    message = checkpoint.get(
        "message",
        "",
    )

    status = "PASS" if success else "FAIL"

    print(
        f"[{status}] {name}"
    )

    if message:
        print(
            f"      {message}"
        )


def _normalize_checkpoint(
    checkpoint: Any,
) -> dict[str, Any]:
    """
    Normalize a workflow checkpoint.

    Args:
        checkpoint:
            Checkpoint dictionary or checkpoint object.

    Returns:
        Dictionary representation of the checkpoint.
    """

    if isinstance(
        checkpoint,
        dict,
    ):
        return checkpoint

    return {
        "name": getattr(
            checkpoint,
            "name",
            "Unknown checkpoint",
        ),
        "success": getattr(
            checkpoint,
            "success",
            False,
        ),
        "message": getattr(
            checkpoint,
            "message",
            "",
        ),
    }


def _normalize_checkpoints(
    checkpoints: Any,
) -> list[dict[str, Any]]:
    """
    Normalize a workflow checkpoint collection.

    Args:
        checkpoints:
            Checkpoint collection.

    Returns:
        List of checkpoint dictionaries.
    """

    if not checkpoints:
        return []

    return [
        _normalize_checkpoint(
            checkpoint
        )
        for checkpoint in checkpoints
    ]


def _extract_workflow_value(
    result: Any,
    key: str,
    default: Any = None,
) -> Any:
    """
    Extract a value from either a dictionary or object result.
    """

    if isinstance(
        result,
        dict,
    ):
        return result.get(
            key,
            default,
        )

    return getattr(
        result,
        key,
        default,
    )


def _record_workflow_result(
    report_service: ExecutionReportService,
    result: Any,
) -> tuple[bool, list[dict[str, Any]]]:
    """
    Record and print workflow checkpoints.

    Args:
        report_service:
            Execution report service.

        result:
            Workflow execution result.

    Returns:
        Tuple containing workflow success and
        normalized checkpoints.
    """

    success = bool(
        _extract_workflow_value(
            result,
            "success",
            False,
        )
    )

    checkpoints = _normalize_checkpoints(
        _extract_workflow_value(
            result,
            "checkpoints",
            [],
        )
    )

    if checkpoints:
        report_service.record_checkpoints(
            checkpoints
        )

        for checkpoint in checkpoints:
            _print_checkpoint(
                checkpoint
            )

    else:
        report_service.record_checkpoint(
            {
                "name": "Workflow execution",
                "success": success,
                "message": _extract_workflow_value(
                    result,
                    "message",
                    "",
                ),
            }
        )

        _print_checkpoint(
            {
                "name": "Workflow execution",
                "success": success,
                "message": _extract_workflow_value(
                    result,
                    "message",
                    "",
                ),
            }
        )

    return success, checkpoints


def _print_schema_evolution_result(
    result: Any,
) -> None:
    """
    Print schema evolution summary.
    """

    summary = _extract_workflow_value(
        result,
        "summary",
        None,
    )

    if not summary:
        return

    print()
    print(
        "Schema Evolution Summary"
    )
    print("-" * 72)

    print(
        f"Table: "
        f"{summary.get('table_name', 'unknown')}"
    )

    print(
        f"Changes detected: "
        f"{summary.get('has_changes', False)}"
    )

    print(
        f"Added columns: "
        f"{summary.get('added_columns', [])}"
    )

    print(
        f"Removed columns: "
        f"{summary.get('removed_columns', [])}"
    )

    print(
        f"Changed columns: "
        f"{summary.get('changed_columns', [])}"
    )


def _print_metadata_result(
    result: Any,
) -> None:
    """
    Print Metadata Explorer output when available.
    """

    summary = _extract_workflow_value(
        result,
        "summary",
        None,
    )

    if summary is None:
        return

    print()
    print(
        "Metadata Explorer Summary"
    )
    print("-" * 72)

    if isinstance(
        summary,
        str,
    ):
        print(summary)
        return

    if isinstance(
        summary,
        dict,
    ):
        for key, value in summary.items():
            print(
                f"{key}: {value}"
            )


def _print_athena_result(
    result: Any,
) -> None:
    """
    Print Athena query results when available.
    """

    queries = _extract_workflow_value(
        result,
        "queries",
        [],
    )

    if not queries:
        return

    print()
    print(
        "Athena Query Results"
    )
    print("-" * 72)

    for index, query_result in enumerate(
        queries,
        start=1,
    ):
        success = getattr(
            query_result,
            "success",
            False,
        )

        message = getattr(
            query_result,
            "message",
            "",
        )

        query_execution_id = getattr(
            query_result,
            "query_execution_id",
            None,
        )

        rows = getattr(
            query_result,
            "rows",
            [],
        )

        print()
        print(
            f"Query {index}: "
            f"{'PASS' if success else 'FAIL'}"
        )

        if message:
            print(
                f"  {message}"
            )

        if query_execution_id:
            print(
                f"  Execution ID: "
                f"{query_execution_id}"
            )

        if rows:
            for row in rows:
                print(
                    f"  {row}"
                )


def run_slice_3() -> int:
    """
    Execute Slice 3 with Slice 2 regression verification.

    Returns:
        Process exit code.
    """

    report_service = (
        ExecutionReportService()
    )

    report_service.start_execution()

    try:

        _print_header()

        # ==============================================================
        # 1. Slice 1 Regression Verification
        # ==============================================================

        print()
        print(
            "[INFO] Verifying Slice 1 foundation..."
        )

        foundation_workflow = (
            FoundationWorkflow()
        )

        foundation_result = (
            foundation_workflow.execute()
        )

        foundation_success, _ = (
            _record_workflow_result(
                report_service,
                foundation_result,
            )
        )

        if not foundation_success:

            report_service.record_checkpoint(
                {
                    "name": (
                        "Slice 1 regression verification"
                    ),
                    "success": False,
                    "message": (
                        "S3 foundation regression detected."
                    ),
                }
            )

            print()
            print(
                "[FAIL] Slice 1 regression verification"
            )

            report_path = (
                report_service.generate_report(
                    success=False
                )
            )

            print(
                f"Execution report: {report_path}"
            )

            return 1

        report_service.record_checkpoint(
            {
                "name": (
                    "Slice 1 regression verification"
                ),
                "success": True,
                "message": (
                    "S3 foundation remains operational."
                ),
            }
        )

        print()
        print(
            "[PASS] Slice 1 regression verification"
        )

        print(
            "      S3 foundation remains operational."
        )

        # ==============================================================
        # 2. Slice 2 Regression - Glue Catalog / Crawler
        # ==============================================================

        print()
        print(
            "[INFO] Verifying Slice 2 "
            "Glue Catalog foundation..."
        )

        glue_workflow = (
            GlueCatalogWorkflow()
        )

        glue_result = (
            glue_workflow.execute()
        )

        glue_success, _ = (
            _record_workflow_result(
                report_service,
                glue_result,
            )
        )

        if not glue_success:

            print()
            print(
                "[FAIL] Slice 2 Glue Catalog regression"
            )

            report_path = (
                report_service.generate_report(
                    success=False
                )
            )

            print(
                f"Execution report: {report_path}"
            )

            return 1

        report_service.record_checkpoint(
            {
                "name": (
                    "Slice 2 regression verification"
                ),
                "success": True,
                "message": (
                    "Glue database, crawler and "
                    "Catalog remain operational."
                ),
            }
        )

        print()
        print(
            "[PASS] Slice 2 regression verification"
        )

        print(
            f"      Database: {GLUE_DATABASE_NAME}"
        )

        # ==============================================================
        # 3. Schema Evolution
        # ==============================================================

        print()
        print(
            "[INFO] Starting Schema Evolution workflow..."
        )

        schema_workflow = (
            SchemaEvolutionWorkflow()
        )

        schema_result = (
            schema_workflow.execute()
        )

        schema_success, _ = (
            _record_workflow_result(
                report_service,
                schema_result,
            )
        )

        _print_schema_evolution_result(
            schema_result
        )

        if not schema_success:

            print()
            print(
                "[FAIL] Schema Evolution workflow"
            )

            report_path = (
                report_service.generate_report(
                    success=False
                )
            )

            print(
                f"Execution report: {report_path}"
            )

            return 1

        print()
        print(
            "[PASS] Schema Evolution workflow"
        )

        # ==============================================================
        # 4. Metadata Explorer
        # ==============================================================

        print()
        print(
            "[INFO] Starting Metadata Explorer workflow..."
        )

        metadata_workflow = (
            MetadataExplorerWorkflow()
        )

        metadata_result = (
            metadata_workflow.execute()
        )

        metadata_success, _ = (
            _record_workflow_result(
                report_service,
                metadata_result,
            )
        )

        _print_metadata_result(
            metadata_result
        )

        if not metadata_success:

            print()
            print(
                "[FAIL] Metadata Explorer workflow"
            )

            report_path = (
                report_service.generate_report(
                    success=False
                )
            )

            print(
                f"Execution report: {report_path}"
            )

            return 1

        print()
        print(
            "[PASS] Metadata Explorer workflow"
        )

        # ==============================================================
        # 5. Athena
        # ==============================================================

        print()
        print(
            "[INFO] Starting Athena workflow..."
        )

        athena_workflow = (
            AthenaWorkflow()
        )

        athena_result = (
            athena_workflow.execute()
        )

        athena_success, _ = (
            _record_workflow_result(
                report_service,
                athena_result,
            )
        )

        _print_athena_result(
            athena_result
        )

        if not athena_success:

            print()
            print(
                "[FAIL] Athena workflow"
            )

            report_path = (
                report_service.generate_report(
                    success=False
                )
            )

            print(
                f"Execution report: {report_path}"
            )

            return 1

        print()
        print(
            "[PASS] Athena workflow"
        )

        # ==============================================================
        # 6. Slice 3 Completion
        # ==============================================================

        report_path = (
            report_service.generate_report(
                success=True
            )
        )

        print()
        print(
            "SLICE 3 STATUS: PASS"
        )

        print(
            f"Execution report: {report_path}"
        )

        return 0

    except Exception as error:

        print()
        print(
            "[FAIL] Unexpected application error"
        )

        print(
            f"      {type(error).__name__}: {error}"
        )

        error_checkpoint = {
            "name": "Unexpected application error",
            "success": False,
            "message": (
                f"{type(error).__name__}: {error}"
            ),
        }

        try:

            report_service.record_checkpoint(
                error_checkpoint
            )

            report_path = (
                report_service.generate_report(
                    success=False
                )
            )

            print(
                f"Execution report: {report_path}"
            )

        except Exception as report_error:

            print(
                "[FAIL] Execution report generation"
            )

            print(
                f"      {type(report_error).__name__}: "
                f"{report_error}"
            )

        return 1


def main() -> int:
    """
    Application entry point.

    Returns:
        Process exit code.
    """

    return run_slice_3()


if __name__ == "__main__":
    raise SystemExit(
        main()
    )


# ---------------------------------------------------------------------------
# Public Interface
# ---------------------------------------------------------------------------
#
# Public functions:
#     main()
#     run_slice_3()
#
# Dependencies:
#     src.config
#     src.core.cloud_foundation.foundation_workflow
#     src.core.glue_catalog.glue_catalog_workflow
#     src.core.schema_evolution.schema_evolution_workflow
#     src.core.metadata_explorer.metadata_explorer_workflow
#     src.core.athena.athena_workflow
#     src.services.execution_report_service
#
# Responsibility:
#     Application bootstrap and Slice 3 orchestration only.
# ---------------------------------------------------------------------------