#pip install "pyiceberg[s3,aws]" polars mypy-boto3-glue s3fs pyarrow boto3
from pyiceberg.catalog.glue import GlueCatalog
import os
import polars as pl
env="sandbox"
GLUE_DATABASE=f"platform_services_catalog_{env}"
ICEBERG_TABLE="client_division_organizations_stream"
S3_WAREHOUSE_PATH=f"s3://platform-services-catalog-{env}/platform_services_catalog_{env}/"
catalog = GlueCatalog(name="glue_catalog", warehouse=S3_WAREHOUSE_PATH)
print(f"Connecting to Glue Catalog: {catalog.name}...")
table_name = f"{GLUE_DATABASE}.{ICEBERG_TABLE}"
table = catalog.load_table(table_name)
print(f"Successfully loaded table: {table_name}")
df = pl.scan_iceberg(table).collect()

GLUE_DATABASE=f"ravemimic{env}"
#GLUE_DATABASE=f"platform_services_catalog_{env}"
ICEBERG_TABLE="edc_messages"
S3_WAREHOUSE_PATH=f"s3://ravemimic-{env}-iceberg/ravemimic{env}/"
catalog = GlueCatalog(name="glue_catalog", warehouse=S3_WAREHOUSE_PATH)
print(f"Connecting to Glue Catalog: {catalog.name}...")
table_name = f"{GLUE_DATABASE}.{ICEBERG_TABLE}"
table = catalog.load_table(table_name)
print(f"Successfully loaded table: {table_name}")
df = pl.scan_iceberg(table).collect()

