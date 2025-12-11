# src/dagster_essentials/defs/assets/trips.py

import requests
from dagster_essentials.defs.assets import constants
import dagster as dg

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