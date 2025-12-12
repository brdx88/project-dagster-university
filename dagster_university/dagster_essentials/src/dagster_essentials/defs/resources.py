# src/dagster_essentials/defs/resources.py
from dagster_duckdb import DuckDBResource


database_resource = DuckDBResource(
    database="data/staging/data.duckdb"
)


# import dagster as dg


# @dg.definitions
# def resources() -> dg.Definitions:
#     return dg.Definitions(resources={})
