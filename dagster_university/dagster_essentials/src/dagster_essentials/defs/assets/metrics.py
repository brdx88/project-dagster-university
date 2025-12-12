# src/dagster_essentials/defs/assets/metrics.py

import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd
from datetime import datetime, timedelta
import pandas as pd

import duckdb
import os

from dagster_essentials.defs.assets import constants
from dagster._utils.backoff import backoff

@dg.asset(
    deps = ['taxi_trips', 'taxi_zones']
)
def manhattan_stats() -> None:
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

    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
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
        deps = ['taxi_trips']
)
def trips_by_week() -> None:
    conn = backoff(
        fn = duckdb.connect,
        retry_on = (RuntimeError, duckdb.IOException),
        kwargs = {
            'database': os.getenv("DUCKDB_DATABASE")
        },
        max_retries = 10
    )

    current_date = datetime.strptime("2023-03-01", constants.DATE_FORMAT)
    end_date = datetime.strptime("2023-04-01", constants.DATE_FORMAT)
    
    result = pd.DataFrame()
    while current_date < end_date:
        current_date_str = current_date.strftime(constants.DATE_FORMAT)
        query = f"""
        SELECT
            vendor_id,
            total_amount,
            trip_distance,
            passenger_count
        FROM trips
        WHERE date_trunc('week', pickup_datetime) = date_trunc('week', '{current_date_str}'::date)
        """
        
        date_for_week = conn.execute(query).fetch_df()

        aggregate = date_for_week.agg({
            'vendor_id': 'count',
            'total_amount':'sum',
            'trip_distance': 'sum',
            'passenger_count': 'sum'
        }).rename({'vendor_id':'num_trips'}).to_frame().T # type: ignore

        aggregate['period'] = current_date

        result = pd.concat([result,aggregate])

        current_date += timedelta(days = 7)

    # clean up the formatting of the dataframe
    result['num_trips'] = result['num_trips'].astype(int)
    result['passenger_count'] = result['passenger_count'].astype(int)
    result['total_amount'] = result['total_amount'].round(2).astype(float)
    result['trip_distance'] = result['trip_distance'].round(2).astype(float)
    result = result[['period', 'num_trips', 'total_amount', 'trip_distance', 'passenger_count']]
    result = result.sort_values(by = 'period')

    result.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index = False)