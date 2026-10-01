"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Execution Report Service
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import (
    APPLICATION_VERSION,
    EXECUTION_REPORT_FILE,
)


class ExecutionReportService:
    """
    Generate runtime execution reports for Project 028.

    The service converts workflow checkpoints into the standard
    execution_report.txt runtime artifact.
    """

    def __init__(
        self,
        report_path: str | Path | None = None,
    ) -> None:
        """
        Initialize the execution report service.

        Args:
            report_path: Optional override for the execution report path.
        """
        self.report_path = Path(
            report_path or EXECUTION_REPORT_FILE,
        )

        self._start_time: datetime | None = None
        self._checkpoints: list[dict[str, Any]] = []
        self._last_successful_checkpoint: str | None = None
        self._errors: list[str] = []

    # ------------------------------------------------------------------
    # Execution Lifecycle
    # ------------------------------------------------------------------

    def start_execution(self) -> None:
        """Start a new execution-report session."""
        self._start_time = datetime.now(timezone.utc)
        self._checkpoints = []
        self._last_successful_checkpoint = None
        self._errors = []

    def record_checkpoint(
        self,
        checkpoint: dict[str, Any],
    ) -> None:
        """
        Record a workflow checkpoint.

        Args:
            checkpoint: Workflow checkpoint dictionary.
        """
        self._checkpoints.append(checkpoint)

        checkpoint_name = str(
            checkpoint.get(
                "name",
                "Unnamed checkpoint",
            ),
        )

        success = bool(
            checkpoint.get(
                "success",
                False,
            ),
        )

        if success:
            self._last_successful_checkpoint = checkpoint_name
        else:
            message = str(
                checkpoint.get(
                    "message",
                    "Checkpoint failed.",
                ),
            )

            self._errors.append(
                f"{checkpoint_name}: {message}",
            )

            errors = checkpoint.get(
                "errors",
                [],
            )

            if isinstance(errors, list):
                for error in errors:
                    self._errors.append(
                        f"{checkpoint_name}: {error}",
                    )

    def record_checkpoints(
        self,
        checkpoints: list[dict[str, Any]],
    ) -> None:
        """
        Record multiple workflow checkpoints.

        Args:
            checkpoints: List of workflow checkpoints.
        """
        for checkpoint in checkpoints:
            self.record_checkpoint(checkpoint)

    # ------------------------------------------------------------------
    # Report Generation
    # ------------------------------------------------------------------

    def generate_report(
        self,
        success: bool,
    ) -> Path:
        """
        Generate the execution report.

        Args:
            success: Overall execution status.

        Returns:
            Path to the generated execution report.
        """
        if self._start_time is None:
            self.start_execution()

        end_time = datetime.now(timezone.utc)
        duration_seconds = (
            end_time - self._start_time
        ).total_seconds()

        report_content = self._build_report_content(
            success=success,
            end_time=end_time,
            duration_seconds=duration_seconds,
        )

        self.report_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.report_path.write_text(
            report_content,
            encoding="utf-8",
        )

        return self.report_path

    # ------------------------------------------------------------------
    # Report Formatting
    # ------------------------------------------------------------------

    def _build_report_content(
        self,
        success: bool,
        end_time: datetime,
        duration_seconds: float,
    ) -> str:
        """
        Build the complete execution report.

        Args:
            success: Overall execution status.
            end_time: Execution completion timestamp.
            duration_seconds: Execution duration.

        Returns:
            Formatted execution report.
        """
        start_time = self._start_time

        start_text = (
            self._format_timestamp(start_time)
            if start_time
            else "Unknown"
        )

        end_text = self._format_timestamp(
            end_time,
        )

        status = "PASS" if success else "FAIL"

        lines: list[str] = [
            "PROJECT 028 - EXECUTION REPORT",
            "=" * 72,
            "",
            f"Application Version : {APPLICATION_VERSION}",
            f"Execution Start     : {start_text}",
            f"Execution End       : {end_text}",
            f"Execution Duration  : {duration_seconds:.3f} seconds",
            f"Overall Status      : {status}",
            "",
            "CHECKPOINTS",
            "-" * 72,
        ]

        if self._checkpoints:
            for index, checkpoint in enumerate(
                self._checkpoints,
                start=1,
            ):
                checkpoint_name = str(
                    checkpoint.get(
                        "name",
                        "Unnamed checkpoint",
                    ),
                )

                checkpoint_status = (
                    "PASS"
                    if checkpoint.get(
                        "success",
                        False,
                    )
                    else "FAIL"
                )

                message = str(
                    checkpoint.get(
                        "message",
                        "",
                    ),
                )

                lines.extend(
                    [
                        (
                            f"{index}. "
                            f"[{checkpoint_status}] "
                            f"{checkpoint_name}"
                        ),
                        f"   {message}",
                    ],
                )

        else:
            lines.append(
                "No checkpoints were recorded.",
            )

        lines.extend(
            [
                "",
                "LAST SUCCESSFUL CHECKPOINT",
                "-" * 72,
                (
                    self._last_successful_checkpoint
                    or "None"
                ),
                "",
                "ERROR INFORMATION",
                "-" * 72,
            ],
        )

        if self._errors:
            lines.extend(
                f"- {error}"
                for error in self._errors
            )
        else:
            lines.append(
                "No errors recorded.",
            )

        lines.extend(
            [
                "",
                "END OF EXECUTION REPORT",
                "=" * 72,
                "",
            ],
        )

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Public Interface
    # ------------------------------------------------------------------

    def get_report_path(self) -> Path:
        """
        Return the configured execution report path.

        Returns:
            Execution report path.
        """
        return self.report_path

    def get_last_successful_checkpoint(self) -> str | None:
        """
        Return the last successful checkpoint.

        Returns:
            Last successful checkpoint name, if available.
        """
        return self._last_successful_checkpoint

    def get_errors(self) -> list[str]:
        """
        Return recorded execution errors.

        Returns:
            Copy of recorded errors.
        """
        return list(self._errors)

    # ------------------------------------------------------------------
    # Formatting Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _format_timestamp(
        timestamp: datetime,
    ) -> str:
        """
        Format an execution timestamp.

        Args:
            timestamp: Timestamp to format.

        Returns:
            ISO-formatted UTC timestamp.
        """
        return timestamp.astimezone(
            timezone.utc,
        ).isoformat(
            timespec="seconds",
        )


__all__ = [
    "ExecutionReportService",
]