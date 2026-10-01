"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Cloud Foundation Workflow
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.config import (
    S3_CUSTOMERS_PREFIX,
    S3_TRANSACTIONS_PREFIX,
    validate_configuration,
)
from src.services.dataset_service import (
    DatasetPreparationResult,
    DatasetService,
)
from src.services.s3_service import (
    S3OperationResult,
    S3Service,
)


class FoundationWorkflow:
    """
    Orchestrate the Project 028 Slice 1 cloud-foundation workflow.

    The workflow coordinates configuration validation, local dataset
    preparation, S3 upload, and upload verification.
    """

    def __init__(
        self,
        s3_service: S3Service | None = None,
        dataset_service: DatasetService | None = None,
    ) -> None:
        """
        Initialize the Slice 1 workflow.

        Args:
            s3_service: Optional S3 service instance.
            dataset_service: Optional dataset service instance.
        """
        self.s3_service = s3_service or S3Service()
        self.dataset_service = (
            dataset_service or DatasetService()
        )

    # ------------------------------------------------------------------
    # Main Workflow
    # ------------------------------------------------------------------

    def execute(self) -> dict[str, Any]:
        """
        Execute the complete Slice 1 workflow.

        Returns:
            Dictionary containing workflow status and checkpoints.
        """
        checkpoints: list[dict[str, Any]] = []

        configuration_result = self._validate_configuration()
        checkpoints.append(configuration_result)

        if not configuration_result["success"]:
            return self._build_result(
                success=False,
                checkpoints=checkpoints,
            )

        bucket_result = self._verify_s3_access()
        checkpoints.append(bucket_result)

        if not bucket_result["success"]:
            return self._build_result(
                success=False,
                checkpoints=checkpoints,
            )

        dataset_result = self._prepare_datasets()
        checkpoints.append(dataset_result)

        if not dataset_result["success"]:
            return self._build_result(
                success=False,
                checkpoints=checkpoints,
            )

        upload_result = self._upload_datasets(
            dataset_result["datasets"],
        )
        checkpoints.append(upload_result)

        if not upload_result["success"]:
            return self._build_result(
                success=False,
                checkpoints=checkpoints,
            )

        verification_result = self._verify_uploaded_datasets(
            upload_result["objects"],
        )
        checkpoints.append(verification_result)

        return self._build_result(
            success=verification_result["success"],
            checkpoints=checkpoints,
        )

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def _validate_configuration(self) -> dict[str, Any]:
        """
        Validate Project 028 configuration.

        Returns:
            Configuration checkpoint.
        """
        valid, errors = validate_configuration()

        if valid:
            return {
                "name": "Configuration validation",
                "success": True,
                "message": "Project configuration is valid.",
            }

        return {
            "name": "Configuration validation",
            "success": False,
            "message": "Configuration validation failed.",
            "errors": errors,
        }

    # ------------------------------------------------------------------
    # AWS Foundation
    # ------------------------------------------------------------------

    def _verify_s3_access(self) -> dict[str, Any]:
        """
        Verify access to the configured S3 bucket.

        Returns:
            S3 access checkpoint.
        """
        result = self.s3_service.verify_bucket_access()

        return {
            "name": "S3 access verification",
            "success": result.success,
            "message": result.message,
        }

    # ------------------------------------------------------------------
    # Dataset Preparation
    # ------------------------------------------------------------------

    def _prepare_datasets(self) -> dict[str, Any]:
        """
        Prepare the approved local Parquet datasets.

        Returns:
            Dataset preparation checkpoint.
        """
        result = self.dataset_service.prepare_datasets()

        if not result.success:
            return {
                "name": "Dataset preparation",
                "success": False,
                "message": result.message,
            }

        datasets = result.datasets or []

        return {
            "name": "Dataset preparation",
            "success": True,
            "message": result.message,
            "datasets": [
                str(dataset)
                for dataset in datasets
            ],
        }

    # ------------------------------------------------------------------
    # Dataset Upload
    # ------------------------------------------------------------------

    def _upload_datasets(
        self,
        datasets: list[str],
    ) -> dict[str, Any]:
        """
        Upload the approved datasets to their S3 prefixes.

        Args:
            datasets: Local dataset paths.

        Returns:
            Dataset upload checkpoint.
        """
        if not datasets:
            return {
                "name": "Dataset upload",
                "success": False,
                "message": "No datasets were supplied for upload.",
            }

        upload_targets = self._build_upload_targets(
            datasets,
        )

        uploaded_objects: list[str] = []

        for local_path, object_key in upload_targets:
            result = self.s3_service.upload_file(
                local_file_path=str(local_path),
                object_key=object_key,
            )

            if not result.success:
                return {
                    "name": "Dataset upload",
                    "success": False,
                    "message": result.message,
                    "uploaded_objects": uploaded_objects,
                }

            uploaded_objects.append(object_key)

        return {
            "name": "Dataset upload",
            "success": True,
            "message": (
                f"Successfully uploaded "
                f"{len(uploaded_objects)} dataset(s)."
            ),
            "objects": uploaded_objects,
        }

    def _build_upload_targets(
        self,
        datasets: list[str],
    ) -> list[tuple[Path, str]]:
        """
        Map local datasets to their approved S3 object keys.

        Args:
            datasets: Local dataset paths.

        Returns:
            List of local-path/S3-key pairs.
        """
        targets: list[tuple[Path, str]] = []

        for dataset in datasets:
            path = Path(dataset)

            if path.name == "customers.parquet":
                object_key = (
                    f"{S3_CUSTOMERS_PREFIX}/"
                    f"{path.name}"
                )

            elif path.name == "transactions.parquet":
                object_key = (
                    f"{S3_TRANSACTIONS_PREFIX}/"
                    f"{path.name}"
                )

            else:
                continue

            targets.append(
                (
                    path,
                    object_key,
                ),
            )

        return targets

    # ------------------------------------------------------------------
    # Upload Verification
    # ------------------------------------------------------------------

    def _verify_uploaded_datasets(
        self,
        object_keys: list[str],
    ) -> dict[str, Any]:
        """
        Verify that all uploaded S3 objects are accessible.

        Args:
            object_keys: Uploaded S3 object keys.

        Returns:
            Upload verification checkpoint.
        """
        if not object_keys:
            return {
                "name": "S3 upload verification",
                "success": False,
                "message": (
                    "No uploaded objects were supplied "
                    "for verification."
                ),
            }

        verified_objects: list[str] = []

        for object_key in object_keys:
            result = self.s3_service.object_exists(
                object_key,
            )

            if not result.success:
                return {
                    "name": "S3 upload verification",
                    "success": False,
                    "message": result.message,
                    "verified_objects": verified_objects,
                }

            object_exists = (
                result.data or {}
            ).get(
                "exists",
                False,
            )

            if not object_exists:
                return {
                    "name": "S3 upload verification",
                    "success": False,
                    "message": (
                        "Uploaded object could not be verified: "
                        f"{object_key}"
                    ),
                    "verified_objects": verified_objects,
                }

            verified_objects.append(
                object_key,
            )

        return {
            "name": "S3 upload verification",
            "success": True,
            "message": (
                f"Successfully verified "
                f"{len(verified_objects)} S3 object(s)."
            ),
            "verified_objects": verified_objects,
        }

    # ------------------------------------------------------------------
    # Result Construction
    # ------------------------------------------------------------------

    @staticmethod
    def _build_result(
        success: bool,
        checkpoints: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Build the final workflow result.

        Args:
            success: Overall workflow status.
            checkpoints: Completed workflow checkpoints.

        Returns:
            Final workflow result.
        """
        return {
            "success": success,
            "status": "PASS" if success else "FAIL",
            "checkpoints": checkpoints,
        }

    # ------------------------------------------------------------------
    # Public Interface
    # ------------------------------------------------------------------

    def get_s3_service(self) -> S3Service:
        """
        Return the configured S3 service.

        Returns:
            S3Service instance.
        """
        return self.s3_service

    def get_dataset_service(self) -> DatasetService:
        """
        Return the configured dataset service.

        Returns:
            DatasetService instance.
        """
        return self.dataset_service


__all__ = [
    "FoundationWorkflow",
]