import requests
from . import constants
import dagster as dg

@dg.asset
def taxi_trips_file() -> None:
    """The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal."""
    month_to_fetch = "2023-03"
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as output_file:
        output_file.write(raw_trips.content)



@dg.asset
def taxi_zones_file() -> None:
    taxi_zones = requests.get(f'https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv')
    with open(constants.TAXI_ZONES_FILE_PATH, 'wb') as output_file:
        output_file.write(taxi_zones.content)

import duckdb
import os
from dagster._utils.backoff import backoff
# or:
from dagster_duckdb import DuckDBResource

@dg.asset(deps=['taxi_trips_file'])
def taxi_trips(database: DuckDBResource) -> None:
    # 1. define the query to create the table named trips
    query = """create or replace table trips as (
          SELECT
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          FROM 'data/raw/taxi_trips_2023-03.parquet'
        );"""
    
    # 2. connect to the database without using the resource
    # conn = backoff(
    #     fn=duckdb.connect,
    #     max_retries=10,
    #     retry_on=(RuntimeError, duckdb.IOException),
    #     kwargs={"database": os.getenv("DUCKDB_DATABASE")}
    # )
    # 3. run query through the connection
    # conn.execute(query)
    
    # 2+3. connect to the database and run the query using the defined resource
    with database.get_connection() as conn:
        conn.execute(query)

    

@dg.asset(
    deps=["taxi_zones_file"]
)
def taxi_zones(database: DuckDBResource) -> None:
    # 1. define the query to create the table named zones
    query = f"""
        create or replace table zones as (
            SELECT
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            FROM '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    # # 2. connect to the database
    # conn = backoff(
    #     fn=duckdb.connect,
    #     retry_on=(RuntimeError, duckdb.IOException),
    #     kwargs={
    #         "database": os.getenv("DUCKDB_DATABASE"),
    #     },
    #     max_retries=10,
    # )

    # # 3. run query through the connection
    # conn.execute(query)
    
    # 2+3. connect to the database and run the query using the defined resource
    with database.get_connection() as conn:
        conn.execute(query)

print('Trips created')