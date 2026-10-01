"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Dataset Preparation Service
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.config import (
    DATA_SAMPLES_DIRECTORY,
    S3_CUSTOMERS_PREFIX,
    S3_TRANSACTIONS_PREFIX,
)


@dataclass(frozen=True)
class DatasetDefinition:
    """Describe a Project 028 dataset."""

    name: str
    filename: str
    s3_prefix: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class DatasetPreparationResult:
    """Represent the outcome of dataset preparation."""

    success: bool
    message: str
    datasets: list[Path] | None = None


class DatasetService:
    """
    Create and validate the sample Parquet datasets used by Project 028.

    This service performs local dataset preparation only. It does not
    communicate with AWS or make S3 decisions.
    """

    CUSTOMER_DATASET = DatasetDefinition(
        name="customers",
        filename="customers.parquet",
        s3_prefix=S3_CUSTOMERS_PREFIX,
        columns=(
            "customer_id",
            "customer_name",
            "country",
            "customer_segment",
        ),
    )

    TRANSACTION_DATASET = DatasetDefinition(
        name="transactions",
        filename="transactions.parquet",
        s3_prefix=S3_TRANSACTIONS_PREFIX,
        columns=(
            "transaction_id",
            "customer_id",
            "transaction_date",
            "amount",
            "currency",
        ),
    )

    def __init__(
        self,
        samples_directory: str | Path | None = None,
    ) -> None:
        """
        Initialize the dataset service.

        Args:
            samples_directory: Optional override for the local
                sample-data directory.
        """
        self.samples_directory = Path(
            samples_directory or DATA_SAMPLES_DIRECTORY,
        )

    # ------------------------------------------------------------------
    # Dataset Preparation
    # ------------------------------------------------------------------

    def prepare_datasets(self) -> DatasetPreparationResult:
        """
        Create the approved Project 028 Parquet datasets.

        Returns:
            DatasetPreparationResult containing generated files.
        """
        try:
            self.samples_directory.mkdir(
                parents=True,
                exist_ok=True,
            )

            customer_path = (
                self.samples_directory
                / self.CUSTOMER_DATASET.filename
            )

            transaction_path = (
                self.samples_directory
                / self.TRANSACTION_DATASET.filename
            )

            self._create_customer_dataset(
                customer_path,
            )

            self._create_transaction_dataset(
                transaction_path,
            )

            datasets = [
                customer_path,
                transaction_path,
            ]

            validation_result = self.validate_datasets(
                datasets,
            )

            if not validation_result.success:
                return validation_result

            return DatasetPreparationResult(
                success=True,
                message=(
                    "Project 028 Parquet datasets prepared "
                    "and validated successfully."
                ),
                datasets=datasets,
            )

        except (OSError, pa.ArrowException) as exc:
            return DatasetPreparationResult(
                success=False,
                message=(
                    "Unable to prepare Project 028 datasets: "
                    f"{exc}"
                ),
            )

    # ------------------------------------------------------------------
    # Dataset Validation
    # ------------------------------------------------------------------

    def validate_datasets(
        self,
        dataset_paths: list[Path],
    ) -> DatasetPreparationResult:
        """
        Validate that the supplied files are readable Parquet datasets.

        Args:
            dataset_paths: Dataset paths to validate.

        Returns:
            DatasetPreparationResult containing validation status.
        """
        if not dataset_paths:
            return DatasetPreparationResult(
                success=False,
                message="No dataset paths were supplied.",
            )

        for dataset_path in dataset_paths:
            if not dataset_path.exists():
                return DatasetPreparationResult(
                    success=False,
                    message=(
                        f"Dataset does not exist: "
                        f"{dataset_path}"
                    ),
                )

            if not dataset_path.is_file():
                return DatasetPreparationResult(
                    success=False,
                    message=(
                        f"Dataset path is not a file: "
                        f"{dataset_path}"
                    ),
                )

            if dataset_path.suffix.lower() != ".parquet":
                return DatasetPreparationResult(
                    success=False,
                    message=(
                        f"Dataset is not a Parquet file: "
                        f"{dataset_path}"
                    ),
                )

            try:
                parquet_file = pq.ParquetFile(
                    dataset_path,
                )

                if parquet_file.metadata.num_rows == 0:
                    return DatasetPreparationResult(
                        success=False,
                        message=(
                            f"Dataset contains no rows: "
                            f"{dataset_path}"
                        ),
                    )

            except (OSError, pa.ArrowException) as exc:
                return DatasetPreparationResult(
                    success=False,
                    message=(
                        f"Unable to read Parquet dataset "
                        f"'{dataset_path}': {exc}"
                    ),
                )

        return DatasetPreparationResult(
            success=True,
            message="All supplied Parquet datasets are valid.",
            datasets=dataset_paths,
        )

    # ------------------------------------------------------------------
    # Dataset Metadata
    # ------------------------------------------------------------------

    def get_dataset_metadata(
        self,
        dataset_path: str | Path,
    ) -> dict[str, Any]:
        """
        Return basic metadata for a Parquet dataset.

        Args:
            dataset_path: Path to the Parquet dataset.

        Returns:
            Dictionary containing dataset metadata.

        Raises:
            FileNotFoundError: If the dataset does not exist.
            ValueError: If the path is not a Parquet file.
        """
        path = Path(dataset_path)

        if not path.exists():
            raise FileNotFoundError(
                f"Dataset does not exist: {path}",
            )

        if path.suffix.lower() != ".parquet":
            raise ValueError(
                f"Dataset is not a Parquet file: {path}",
            )

        parquet_file = pq.ParquetFile(path)
        schema = parquet_file.schema_arrow

        return {
            "filename": path.name,
            "path": str(path),
            "row_count": parquet_file.metadata.num_rows,
            "column_count": len(schema),
            "columns": [
                {
                    "name": field.name,
                    "type": str(field.type),
                }
                for field in schema
            ],
        }

    # ------------------------------------------------------------------
    # Dataset Definitions
    # ------------------------------------------------------------------

    def get_dataset_definitions(self) -> list[DatasetDefinition]:
        """
        Return the datasets approved for Slice 1.

        Returns:
            List of dataset definitions.
        """
        return [
            self.CUSTOMER_DATASET,
            self.TRANSACTION_DATASET,
        ]

    # ------------------------------------------------------------------
    # Private Dataset Builders
    # ------------------------------------------------------------------

    def _create_customer_dataset(
        self,
        output_path: Path,
    ) -> None:
        """Create the deterministic customer Parquet dataset."""
        data = {
            "customer_id": [
                "C001",
                "C002",
                "C003",
                "C004",
                "C005",
            ],
            "customer_name": [
                "Aarav Sharma",
                "Meera Iyer",
                "Rahul Menon",
                "Ananya Rao",
                "Vikram Patel",
            ],
            "country": [
                "IN",
                "IN",
                "IN",
                "IN",
                "IN",
            ],
            "customer_segment": [
                "Retail",
                "Premium",
                "Retail",
                "Premium",
                "Retail",
            ],
        }

        table = pa.table(data)
        pq.write_table(table, output_path)

    def _create_transaction_dataset(
        self,
        output_path: Path,
    ) -> None:
        """Create the deterministic transaction Parquet dataset."""
        data = {
            "transaction_id": [
                "T001",
                "T002",
                "T003",
                "T004",
                "T005",
                "T006",
            ],
            "customer_id": [
                "C001",
                "C002",
                "C003",
                "C004",
                "C005",
                "C001",
            ],
            "transaction_date": [
                "2026-09-01",
                "2026-09-02",
                "2026-09-03",
                "2026-09-04",
                "2026-09-05",
                "2026-09-06",
            ],
            "amount": [
                1250.50,
                3400.00,
                875.25,
                5200.75,
                1450.00,
                975.50,
            ],
            "currency": [
                "INR",
                "INR",
                "INR",
                "INR",
                "INR",
                "INR",
            ],
        }

        table = pa.table(data)
        pq.write_table(table, output_path)

    # ------------------------------------------------------------------
    # Public Interface
    # ------------------------------------------------------------------

    def get_samples_directory(self) -> Path:
        """
        Return the configured local sample-data directory.

        Returns:
            Sample-data directory path.
        """
        return self.samples_directory


__all__ = [
    "DatasetDefinition",
    "DatasetPreparationResult",
    "DatasetService",
]