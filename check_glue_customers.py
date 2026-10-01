"""
Project : AWS Glue Data Catalog & Crawler Platform
Project ID : 028

Glue Customers Baseline Checker
"""

import boto3


def main() -> None:
    client = boto3.client("glue", region_name="ap-south-1")

    table = client.get_table(
        DatabaseName="project_028_catalog",
        Name="customers",
    )["Table"]

    print("Location:", table["StorageDescriptor"]["Location"])
    print("Columns:")

    for column in table["StorageDescriptor"]["Columns"]:
        print(f"  {column['Name']}: {column['Type']}")


if __name__ == "__main__":
    main()