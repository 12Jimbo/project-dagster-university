from . import constants
import dagster as dg
from dagster import asset

import matplotlib.pyplot as plt
import geopandas as gpd

import duckdb
import os
from dagster._utils.backoff import backoff

@dg.asset(deps=['taxi_trips', 'taxi_zones'])
def manhattan_stats() -> None:
    query = """
            select
                zones.zone,
                zones.borough,
                zones.geometry,
                count(*) as trip_count
            from trips left join zones on trips.pickup_zone_id = zones.zone_id
            where borough = 'Manhattan' and geometry is not null
            group by zone, borough, geometry
    """

    conn = backoff(
        fn=duckdb.connect,
        max_retries=10,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={"database": os.getenv("DUCKDB_DATABASE")}
    )
    trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())


@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="trip_count", cmap="PRGn", legend=True, ax=ax, edgecolor="green")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # Adjust longitude range
    ax.set_ylim([40.70, 40.82])  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)


@dg.asset(deps = ['taxi_trips'])
def trips_by_week() -> None:
    query = """
            SELECT
                DATE_TRUNC('week', pickup_datetime) as period,
                COUNT(*) as num_trips,
                SUM(passenger_count) as passenger_count,
                SUM(total_amount) as total_amount,
                SUM(trip_distance) as trip_distance,
            FROM trips
            WHERE pickup_datetime >= '2023-01-01' AND pickup_datetime < '2023-04-01'
            GROUP BY DATE_TRUNC('week', pickup_datetime)
            ORDER BY period  
            """
    
    # 2. connect to the database
    conn = backoff(
        fn=duckdb.connect,
        max_retries=10,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={"database": os.getenv("DUCKDB_DATABASE")}
    )
    
    # 3. run query through the connection
    df = conn.execute(query).fetch_df()
    
    # Write to file
    with open(constants.TRIPS_BY_WEEK_FILE_PATH, 'w') as output_file:
        output_file.write(df.to_csv(index=False))


print('Metrics created')

# prova push commit

# prova push sync

# 2 prova push sync
# prova push sync 2
