# src/dagster_essentials/defs/assets/metrics.py

import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd
from datetime import datetime, timedelta
import pandas as pd

from dagster_essentials.defs.assets import constants
from dagster_duckdb import DuckDBResource

from dagster_essentials.defs.partitions import weekly_partition, monthly_partition

@dg.asset(
    deps = ['taxi_trips', 'taxi_zones']
)
def manhattan_stats(database: DuckDBResource) -> None:
    """
    Generate a GeoJSON file containing trip counts by zone in Manhattan.
    
    1. Makes a SQL query that joins the trips and zones tables, filters down to just trips in Manhattan, and then aggregates the data to get the number of trips per neighborhood.
    2. Executes that query against the same DuckDB database that you ingested data into in the other assets.
    3. Leverages the GeoPandas library to turn the messy coordinates into a format other libraries can use.
    4. Saves the transformed data to a GeoJSON file.
    """

    query = """
        SELECT
            zone,
            borough,
            geometry,
            count(1) as num_trips
        FROM trips
        LEFT JOIN zones
            ON trips.pickup_zone_id = zones.zone_id
        WHERE borough = 'Manhattan'
            AND geometry IS NOT NULL
        GROUP BY 
            zone,
            borough,
            geometry
    """

    # conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    with database.get_connection() as conn:        
        trips_by_zone = conn.execute(query).fetchdf()

    trips_by_zone['geometry'] = gpd.GeoSeries.from_wkt(trips_by_zone['geometry'])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@dg.asset(
    deps = ['manhattan_stats']
)
def manhattan_map() -> None:
    """
    Generate a map visualization of trip counts by zone in Manhattan.

    1. Defines a new asset called manhattan_map, which is dependent on manhattan_stats.
    2. Reads the GeoJSON file back into memory.
    3. Creates a map as a data visualization using the Matplotlib visualization library.
    4. Saves the visualization as a PNG.
    """

    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(
        column = "num_trips",
        cmap = "plasma",
        legend = True,
        ax = ax,
        edgecolor = 'black'
    )
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])       # adjust longitude range
    ax.set_ylim([40.70, 40.82])         # adjust latitude range

    # save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format = 'png', bbox_inches = 'tight')
    plt.close(fig)

@dg.asset(
        deps = ['taxi_trips'],
        partitions_def = monthly_partition
)
def trips_by_week(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
      The number of trips per week, aggregated by week.
    """

    period_to_fetch = context.partition_key

    # get all trips for the week
    query = f"""
        select vendor_id, total_amount, trip_distance, passenger_count
        from trips
        where pickup_datetime >= '{period_to_fetch}'
            and pickup_datetime < '{period_to_fetch}'::date + interval '1 week'
    """

    with database.get_connection() as conn:
        data_for_month = conn.execute(query).fetch_df()

    aggregate = data_for_month.agg({
        "vendor_id": "count",
        "total_amount": "sum",
        "trip_distance": "sum",
        "passenger_count": "sum"
    }).rename({"vendor_id": "num_trips"}).to_frame().T # type: ignore

    # clean up the formatting of the dataframe
    aggregate["period"] = period_to_fetch
    aggregate['num_trips'] = aggregate['num_trips'].astype(int)
    aggregate['passenger_count'] = aggregate['passenger_count'].astype(int)
    aggregate['total_amount'] = aggregate['total_amount'].round(2).astype(float)
    aggregate['trip_distance'] = aggregate['trip_distance'].round(2).astype(float)
    aggregate = aggregate[["period", "num_trips", "total_amount", "trip_distance", "passenger_count"]]

    try:
        # If the file already exists, append to it, but replace the existing month's data
        existing = pd.read_csv(constants.TRIPS_BY_WEEK_FILE_PATH)
        existing = existing[existing["period"] != period_to_fetch]
        existing = pd.concat([existing, aggregate]).sort_values(by="period")
        existing.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
    except FileNotFoundError:
        aggregate.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)