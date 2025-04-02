# This code snippet imports a resource called DuckDBResource from Dagster’s dagster_duckdb integration library. 
# Next, it creates an instance of that resource and stores it in database_resource.

import dagster as dg
from dagster_duckdb import DuckDBResource

path = "data/staging/data.duckdb"

database_resource = DuckDBResource(
    database = path
)

# replacing argument "data/staging/data.duckdb" with environment variable defined in the .env file (this does not seem to work)
# database_resource = DuckDBResource(
#     database=dg.EnvVar("DUCKDB_DATABASE")
# )
