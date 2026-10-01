"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Amazon S3 Service
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.config import AWS_REGION, S3_BUCKET_NAME


@dataclass(frozen=True)
class S3OperationResult:
    """Represent the result of an S3 operation."""

    success: bool
    message: str
    data: dict[str, Any] | None = None


class S3Service:
    """
    Provide the application's Amazon S3 operations.

    The service owns AWS S3 communication but does not contain
    Project 028 business workflow decisions.
    """

    def __init__(
        self,
        bucket_name: str | None = None,
        region_name: str | None = None,
    ) -> None:
        """
        Initialize the S3 service.

        Args:
            bucket_name: Optional S3 bucket override.
            region_name: Optional AWS region override.
        """
        self.bucket_name = (
            bucket_name.strip()
            if bucket_name
            else S3_BUCKET_NAME
        )
        self.region_name = region_name or AWS_REGION

        self._client = boto3.client(
            "s3",
            region_name=self.region_name,
        )

    # ------------------------------------------------------------------
    # Bucket Operations
    # ------------------------------------------------------------------

    def verify_bucket_access(self) -> S3OperationResult:
        """
        Verify that the configured S3 bucket exists and is accessible.

        Returns:
            S3OperationResult containing the verification outcome.
        """
        if not self.bucket_name:
            return S3OperationResult(
                success=False,
                message="S3 bucket name is not configured.",
            )

        try:
            response = self._client.head_bucket(
                Bucket=self.bucket_name,
            )

            return S3OperationResult(
                success=True,
                message=(
                    f"S3 bucket '{self.bucket_name}' "
                    "is accessible."
                ),
                data={
                    "bucket_name": self.bucket_name,
                    "response_metadata": response.get(
                        "ResponseMetadata",
                        {},
                    ),
                },
            )

        except (ClientError, BotoCoreError) as exc:
            return S3OperationResult(
                success=False,
                message=self._format_aws_error(
                    "Unable to access S3 bucket",
                    exc,
                ),
            )

    # ------------------------------------------------------------------
    # Object Operations
    # ------------------------------------------------------------------

    def upload_file(
        self,
        local_file_path: str,
        object_key: str,
    ) -> S3OperationResult:
        """
        Upload a local file to the configured S3 bucket.

        Args:
            local_file_path: Local file to upload.
            object_key: Destination S3 object key.

        Returns:
            S3OperationResult containing the upload outcome.
        """
        if not self.bucket_name:
            return S3OperationResult(
                success=False,
                message="S3 bucket name is not configured.",
            )

        if not local_file_path:
            return S3OperationResult(
                success=False,
                message="Local file path cannot be empty.",
            )

        if not object_key:
            return S3OperationResult(
                success=False,
                message="S3 object key cannot be empty.",
            )

        try:
            self._client.upload_file(
                Filename=local_file_path,
                Bucket=self.bucket_name,
                Key=object_key,
            )

            return S3OperationResult(
                success=True,
                message=(
                    f"Uploaded '{local_file_path}' to "
                    f"s3://{self.bucket_name}/{object_key}."
                ),
                data={
                    "bucket_name": self.bucket_name,
                    "object_key": object_key,
                    "local_file_path": local_file_path,
                },
            )

        except (ClientError, BotoCoreError, OSError) as exc:
            return S3OperationResult(
                success=False,
                message=self._format_aws_error(
                    "Unable to upload file",
                    exc,
                ),
            )

    def object_exists(
        self,
        object_key: str,
    ) -> S3OperationResult:
        """
        Determine whether an S3 object exists.

        Args:
            object_key: S3 object key to check.

        Returns:
            S3OperationResult containing existence information.
        """
        if not self.bucket_name:
            return S3OperationResult(
                success=False,
                message="S3 bucket name is not configured.",
            )

        if not object_key:
            return S3OperationResult(
                success=False,
                message="S3 object key cannot be empty.",
            )

        try:
            response = self._client.head_object(
                Bucket=self.bucket_name,
                Key=object_key,
            )

            return S3OperationResult(
                success=True,
                message=(
                    f"S3 object exists: "
                    f"s3://{self.bucket_name}/{object_key}."
                ),
                data={
                    "exists": True,
                    "bucket_name": self.bucket_name,
                    "object_key": object_key,
                    "content_length": response.get(
                        "ContentLength",
                    ),
                    "content_type": response.get(
                        "ContentType",
                    ),
                    "etag": response.get("ETag"),
                    "last_modified": response.get(
                        "LastModified",
                    ),
                },
            )

        except ClientError as exc:
            error_code = exc.response.get(
                "Error",
                {},
            ).get(
                "Code",
                "",
            )

            if error_code in {
                "404",
                "NoSuchKey",
                "NotFound",
            }:
                return S3OperationResult(
                    success=True,
                    message=(
                        f"S3 object does not exist: "
                        f"s3://{self.bucket_name}/{object_key}."
                    ),
                    data={
                        "exists": False,
                        "bucket_name": self.bucket_name,
                        "object_key": object_key,
                    },
                )

            return S3OperationResult(
                success=False,
                message=self._format_aws_error(
                    "Unable to check S3 object",
                    exc,
                ),
            )

        except BotoCoreError as exc:
            return S3OperationResult(
                success=False,
                message=self._format_aws_error(
                    "Unable to check S3 object",
                    exc,
                ),
            )

    def list_objects(
        self,
        prefix: str = "",
    ) -> S3OperationResult:
        """
        List objects under an S3 prefix.

        Args:
            prefix: Optional S3 prefix used to filter objects.

        Returns:
            S3OperationResult containing discovered objects.
        """
        if not self.bucket_name:
            return S3OperationResult(
                success=False,
                message="S3 bucket name is not configured.",
            )

        try:
            paginator = self._client.get_paginator(
                "list_objects_v2",
            )

            objects: list[dict[str, Any]] = []

            for page in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix,
            ):
                objects.extend(
                    page.get("Contents", []),
                )

            return S3OperationResult(
                success=True,
                message=(
                    f"Retrieved {len(objects)} object(s) "
                    f"from S3 prefix '{prefix}'."
                ),
                data={
                    "bucket_name": self.bucket_name,
                    "prefix": prefix,
                    "object_count": len(objects),
                    "objects": objects,
                },
            )

        except (ClientError, BotoCoreError) as exc:
            return S3OperationResult(
                success=False,
                message=self._format_aws_error(
                    "Unable to list S3 objects",
                    exc,
                ),
            )

    # ------------------------------------------------------------------
    # Public Interface
    # ------------------------------------------------------------------

    def get_bucket_name(self) -> str:
        """
        Return the configured S3 bucket name.

        Returns:
            Configured bucket name.
        """
        return self.bucket_name

    def get_region_name(self) -> str:
        """
        Return the configured AWS region.

        Returns:
            Configured AWS region.
        """
        return self.region_name

    # ------------------------------------------------------------------
    # Internal Error Handling
    # ------------------------------------------------------------------

    @staticmethod
    def _format_aws_error(
        operation: str,
        exception: Exception,
    ) -> str:
        """
        Format an AWS-related exception for application reporting.

        Args:
            operation: Description of the failed operation.
            exception: Raised exception.

        Returns:
            Human-readable error message.
        """
        if isinstance(exception, ClientError):
            error = exception.response.get(
                "Error",
                {},
            )

            error_code = error.get(
                "Code",
                "Unknown",
            )

            error_message = error.get(
                "Message",
                str(exception),
            )

            return (
                f"{operation}. "
                f"AWS error {error_code}: {error_message}"
            )

        return f"{operation}: {exception}"

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------

    @property
    def client(self) -> Any:
        """
        Expose the underlying boto3 S3 client for controlled integration.

        Returns:
            boto3 S3 client.
        """
        return self._client


__all__ = [
    "S3OperationResult",
    "S3Service",
]