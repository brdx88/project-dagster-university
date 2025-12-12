# src/dagster_essentials/defs/assets/trips.py

import requests
import dagster as dg
from dagster_essentials.defs.assets import constants
from dagster_duckdb import DuckDBResource

# 1. A description of the asset is added using a docstring ("""), which will display in the Dagster UI.
# 2. Next, a variable named month_to_fetch is defined. The value is 2023-03, or March 2023.
# 3. A second variable named raw_trips is defined. This variable uses the get function from the requests library (requests.get) to retrieve a parquet file from the NYC Open Data portal website.
# 4. Using the month_to_fetch variable, the URL to retrieve the file from becomes: https://.../trip-data/yellow_tripdata_2023-03.parquet
# 5. Next, the path where the file will be stored is constructed. The value of TAXI_TRIPS_TEMPLATE_FILE_PATH, stored in your project’s assets/constants.py file, is retrieved: data/raw/taxi_trips_{}.parquet
# 6. The parquet file is created and saved at data/raw/taxi_trips_2023-03.parquet
# 7. The asset function’s execution completes successfully. This completion indicates to Dagster that an asset has been materialized, and Dagster will update the UI to reflect that asset materialized successfully.

@dg.asset
def taxi_trips_file() -> None:
    """
    The raw parquet file for the taxi trips dataset. Sourced from NYC Open Data portal.
    """

    month_to_fetch = '2023-03'

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)

@dg.asset
def taxi_zones_file() -> None:
    """
    The raw CSV file for the taxi zones dataset. Sourced from NYC Open Data portal.
    """

    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)

@dg.asset(
    deps = ['taxi_trips_file']
)
def taxi_trips(database: DuckDBResource) -> None:
    """
    The raw taxi trips data loaded into a DuckDB database.

    To check the result:
    1. open a Python REPL (make sure in the virtual environment for this project)
    2. type the following commands:
        >>> import duckdb
        >>> conn = duckdb.connect(database="data/staging/data.duckdb") # assumes you're writing to the same destination as specified in .env.example
        >>> conn.execute("select count(*) from trips").fetchall()
    3. the result might:
        [(3403766,)]
    """
    # 1. Using the @dg.asset decorator, an asset named taxi_trips is created.
    # 2. The taxi_trips_file asset is defined as a dependency of taxi_trips through the deps argument.
    # 3. Next, a variable named query is created. This variable contains a SQL query that creates a table named trips, which sources its data from the data/raw/taxi_trips_2023-03.parquet file. This is the file created by the taxi_trips_file asset.
    # 4. A variable named conn is created, which defines the connection to the DuckDB database in the project. To do this, we first wrap everything with the Dagster utility function backoff. Using the backoff function ensures that multiple assets can use DuckDB safely without locking resources. The backoff function takes in the function we want to call (in this case the .connect method from the duckdb library), any errors to retry on (RuntimeError and duckdb.IOException), the max number of retries, and finally, the arguments to supply to the .connect DuckDB method. Here we are passing in the DUCKDB_DATABASE environment variable to tell DuckDB where the database is located.
    # 5. The DUCKDB_DATABASE environment variable, sourced from your project’s .env file, resolves to data/staging/data.duckdb. Note: We set up this file in Lesson 2 - refer to this lesson if you need a refresher. If this file isn’t set up correctly, the materialization will result in an error.
    # 6. Finally, conn is paired with the DuckDB execute method, where our SQL query (query) is passed in as an argument. This tells the asset that, when materializing, to connect to the DuckDB database and execute the query in query.

    query = """
    CREATE OR REPLACE TABLE trips AS 
    (
        SELECT
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_cod_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
        FROM 'data/raw/taxi_trips_2023-03.parquet'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)

@dg.asset(
        deps = ['taxi_zones_file']
)
def taxi_zones(database: DuckDBResource) -> None:
    """
    Docstring for taxi_zones
    """

    query = f"""
        CREATE OR REPLACE TABLE zones AS 
        (
            SELECT
                LocationID as zone_id,
                zone as zone,
                borough as borough, 
                the_geom as geometry
            FROM '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)