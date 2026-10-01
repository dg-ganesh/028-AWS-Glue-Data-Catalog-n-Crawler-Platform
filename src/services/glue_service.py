"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

AWS Glue Service
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src import config


@dataclass
class GlueOperationResult:
    """
    Represents the result of an AWS Glue operation.
    """

    success: bool
    message: str
    data: Any = None


class GlueService:
    """
    Provides AWS Glue API operations for Project 028.
    """

    def __init__(
        self,
        region_name: str | None = None,
    ) -> None:
        """
        Initialize the AWS Glue client.

        Args:
            region_name: AWS region. Uses project configuration when omitted.
        """

        self._region_name = (
            region_name or config.AWS_REGION
        )

        self._client = boto3.client(
            "glue",
            region_name=self._region_name,
        )

    # ------------------------------------------------------------------
    # Database Operations
    # ------------------------------------------------------------------

    def database_exists(
        self,
        database_name: str,
    ) -> GlueOperationResult:
        """
        Check whether a Glue database exists.

        Args:
            database_name: Glue database name.

        Returns:
            GlueOperationResult indicating whether the database exists.
        """

        try:
            response = self._client.get_database(
                Name=database_name,
            )

            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue database '{database_name}' exists."
                ),
                data=response.get("Database"),
            )

        except self._client.exceptions.EntityNotFoundException:
            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue database '{database_name}' does not exist."
                ),
                data=None,
            )

        except (ClientError, BotoCoreError) as error:
            return GlueOperationResult(
                success=False,
                message=self._format_aws_error(
                    "checking Glue database",
                    error,
                ),
            )

    def create_database(
        self,
        database_name: str,
        description: str,
    ) -> GlueOperationResult:
        """
        Create a Glue Data Catalog database.

        Args:
            database_name: Name of the database.
            description: Database description.

        Returns:
            GlueOperationResult containing the API response.
        """

        try:
            response = self._client.create_database(
                DatabaseInput={
                    "Name": database_name,
                    "Description": description,
                }
            )

            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue database '{database_name}' "
                    "created successfully."
                ),
                data=response,
            )

        except self._client.exceptions.AlreadyExistsException:
            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue database '{database_name}' "
                    "already exists."
                ),
            )

        except (ClientError, BotoCoreError) as error:
            return GlueOperationResult(
                success=False,
                message=self._format_aws_error(
                    "creating Glue database",
                    error,
                ),
            )

    def ensure_database(
        self,
        database_name: str,
        description: str,
    ) -> GlueOperationResult:
        """
        Ensure that a Glue database exists.
        """

        existing = self.database_exists(
            database_name,
        )

        if not existing.success:
            return existing

        if existing.data is not None:
            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue database '{database_name}' "
                    "is ready."
                ),
                data=existing.data,
            )

        return self.create_database(
            database_name,
            description,
        )

    # ------------------------------------------------------------------
    # Crawler Operations
    # ------------------------------------------------------------------

    def crawler_exists(
        self,
        crawler_name: str,
    ) -> GlueOperationResult:
        """
        Check whether a Glue crawler exists.

        Args:
            crawler_name: Glue crawler name.

        Returns:
            GlueOperationResult containing crawler metadata.
        """

        try:
            response = self._client.get_crawler(
                Name=crawler_name,
            )

            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue crawler '{crawler_name}' exists."
                ),
                data=response.get("Crawler"),
            )

        except self._client.exceptions.EntityNotFoundException:
            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue crawler '{crawler_name}' "
                    "does not exist."
                ),
                data=None,
            )

        except (ClientError, BotoCoreError) as error:
            return GlueOperationResult(
                success=False,
                message=self._format_aws_error(
                    "checking Glue crawler",
                    error,
                ),
            )

    def create_crawler(
        self,
        crawler_name: str,
        database_name: str,
        role_arn: str,
        s3_path: str,
    ) -> GlueOperationResult:
        """
        Create a Glue crawler targeting an S3 location.

        Args:
            crawler_name: Glue crawler name.
            database_name: Target Glue database.
            role_arn: IAM role assumed by Glue.
            s3_path: S3 location containing source datasets.

        Returns:
            GlueOperationResult containing crawler metadata.
        """

        try:
            response = self._client.create_crawler(
                Name=crawler_name,
                Role=role_arn,
                DatabaseName=database_name,
                Targets={
                    "S3Targets": [
                        {
                            "Path": s3_path,
                        }
                    ]
                },
                Description=(
                    "Project 028 metadata discovery crawler."
                ),
                SchemaChangePolicy={
                    "UpdateBehavior": "UPDATE_IN_DATABASE",
                    "DeleteBehavior": "DEPRECATE_IN_DATABASE",
                },
            )

            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue crawler '{crawler_name}' "
                    "created successfully."
                ),
                data=response,
            )

        except self._client.exceptions.AlreadyExistsException:
            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue crawler '{crawler_name}' "
                    "already exists."
                ),
            )

        except (ClientError, BotoCoreError) as error:
            return GlueOperationResult(
                success=False,
                message=self._format_aws_error(
                    "creating Glue crawler",
                    error,
                ),
            )

    def start_crawler(
        self,
        crawler_name: str,
    ) -> GlueOperationResult:
        """
        Start a Glue crawler.
        """

        try:
            self._client.start_crawler(
                Name=crawler_name,
            )

            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue crawler '{crawler_name}' "
                    "started successfully."
                ),
            )

        except self._client.exceptions.CrawlerRunningException:
            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue crawler '{crawler_name}' "
                    "is already running."
                ),
            )

        except (ClientError, BotoCoreError) as error:
            return GlueOperationResult(
                success=False,
                message=self._format_aws_error(
                    "starting Glue crawler",
                    error,
                ),
            )

    def get_crawler_status(
        self,
        crawler_name: str,
    ) -> GlueOperationResult:
        """
        Retrieve the current Glue crawler state.
        """

        try:
            response = self._client.get_crawler(
                Name=crawler_name,
            )

            crawler = response.get("Crawler", {})
            state = crawler.get("State")

            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue crawler '{crawler_name}' "
                    f"state: {state}."
                ),
                data={
                    "name": crawler.get("Name"),
                    "state": state,
                    "last_crawl": crawler.get("LastCrawl"),
                },
            )

        except (ClientError, BotoCoreError) as error:
            return GlueOperationResult(
                success=False,
                message=self._format_aws_error(
                    "retrieving Glue crawler status",
                    error,
                ),
            )

    # ------------------------------------------------------------------
    # Catalog Table Operations
    # ------------------------------------------------------------------

    def get_tables(
        self,
        database_name: str,
    ) -> GlueOperationResult:
        """
        Retrieve tables registered in a Glue database.
        """

        try:
            response = self._client.get_tables(
                DatabaseName=database_name,
            )

            return GlueOperationResult(
                success=True,
                message=(
                    f"Retrieved Glue tables from "
                    f"database '{database_name}'."
                ),
                data=response.get("TableList", []),
            )

        except (ClientError, BotoCoreError) as error:
            return GlueOperationResult(
                success=False,
                message=self._format_aws_error(
                    "retrieving Glue tables",
                    error,
                ),
            )

    def get_table(
        self,
        database_name: str,
        table_name: str,
    ) -> GlueOperationResult:
        """
        Retrieve one Glue Data Catalog table.
        """

        try:
            response = self._client.get_table(
                DatabaseName=database_name,
                Name=table_name,
            )

            return GlueOperationResult(
                success=True,
                message=(
                    f"Retrieved Glue table "
                    f"'{table_name}'."
                ),
                data=response.get("Table"),
            )

        except self._client.exceptions.EntityNotFoundException:
            return GlueOperationResult(
                success=True,
                message=(
                    f"Glue table '{table_name}' "
                    "was not found."
                ),
                data=None,
            )

        except (ClientError, BotoCoreError) as error:
            return GlueOperationResult(
                success=False,
                message=self._format_aws_error(
                    "retrieving Glue table",
                    error,
                ),
            )

    # ------------------------------------------------------------------
    # Client Access
    # ------------------------------------------------------------------

    @property
    def client(self) -> Any:
        """
        Return the underlying boto3 Glue client.
        """

        return self._client

    # ------------------------------------------------------------------
    # Error Handling
    # ------------------------------------------------------------------

    @staticmethod
    def _format_aws_error(
        operation: str,
        error: Exception,
    ) -> str:
        """
        Convert an AWS exception into a useful project message.
        """

        if isinstance(error, ClientError):
            error_code = error.response.get(
                "Error",
                {},
            ).get(
                "Code",
                "Unknown",
            )

            error_message = error.response.get(
                "Error",
                {},
            ).get(
                "Message",
                str(error),
            )

            return (
                f"Error {operation}: "
                f"{error_code} - {error_message}"
            )

        return (
            f"Error {operation}: "
            f"{error}"
        )


# ---------------------------------------------------------------------------
# Public Interface
# ---------------------------------------------------------------------------
#
# Public class:
#   GlueService
#
# Public data model:
#   GlueOperationResult
#
# GlueService public methods:
#   database_exists()
#   create_database()
#   ensure_database()
#   crawler_exists()
#   create_crawler()
#   start_crawler()
#   get_crawler_status()
#   get_tables()
#   get_table()
#
# Public property:
#   client
#
# External dependency:
#   boto3
# ---------------------------------------------------------------------------