"""

The models.py file contains a Bauplan pipeline:
- trips_and_zones runs an S3 two scans in Python against the data lake and joins them using PyArrow
- top_pickup_locations uses Pandas to do aggregation and ordering over trips_and_zones
and returns a table with all NY Zones ordered by total number of trips pickups.

Note that collecting models in a single file called models.py is not required, but we find it useful
to keep the pipeline's code together.

"""

import bauplan


@bauplan.model()
@bauplan.python('3.11')
def trips_and_zones(
        trips=bauplan.Model(
            'taxi_fhvhv',
            # this function does an S3 scan directly in Python, so we can specify the columns and the filter pushdown
            # by pushing the filters down to S3 we make the system considerably more performant
            columns=[
                'pickup_datetime',
                'dropoff_datetime',
                'PULocationID',
                'DOLocationID',
                'trip_miles',
                'trip_time',
                'base_passenger_fare',
                'tips',
            ],
            filter="pickup_datetime >= '2023-01-01T00:00:00-05:00' AND pickup_datetime < '2023-02-02T00:00:00-05:00'"
        ),
        zones=bauplan.Model(
            'taxi_zones',
        ),
):
    """

       this function does an S3 scan over two tables - taxi_fhvhv and zones - filtering by pickup_datetime
       it then joins them over PULocationID and LocationID using Pyarrow https://arrow.apache.org/docs/python/index.html
       the output is a table with the taxi trip the taxi trips in the relevant period and the corresponding pickup Zones

    """

    import math

    # the following code is PyArrow
    # because Bauplan speaks Arrow natively you don't need to import PyArrow explicitly
    # join 'trips' with 'zones' on 'PULocationID' and 'LocationID'
    pickup_location_table = (trips
                             .join(zones, 'PULocationID', 'LocationID')
                             .combine_chunks()
                             )
    # print the size of the resulting table
    size_in_gb = round(pickup_location_table.nbytes / math.pow(1024, 3), 3)
    print(f"\nThis table is {size_in_gb} GB and has {pickup_location_table.num_rows} rows\n")

    return pickup_location_table


# this function explicitly requires that its output is materialized in the data catalog as an Iceberg table
@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'pandas': '2.2.0'})
def top_pickup_locations(data=bauplan.Model('trips_and_zones')):
    """

    this function takes the parent table with the taxi trips and the corresponding pickup zones
    and groups the taxi trips by PULocationID, Borough and Zone sorting them in descending order
    the output is the table of the top pickup locations by number of trips

    """

    import pandas as pd

    # convert the input Arrow table into a Pandas dataframe
    df = data.to_pandas()

    # group the taxi trips by PULocationID, Borough and Zone and sort in descending order
    # the result will be a Pandas dataframe with all the pickup locations sorted by number of trips
    top_pickup_table = (
        df
        .groupby(['PULocationID', 'Borough', 'Zone'])
        .agg(number_of_trips=('pickup_datetime', 'count'))
        .reset_index()
        .sort_values(by='number_of_trips', ascending=False)
    )
    # we can return a Pandas dataframe
    return top_pickup_table
