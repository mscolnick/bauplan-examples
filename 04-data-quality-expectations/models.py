"""

This pipeline computes a table with the zones of NY ordered by how long it takes to get a taxi cab on average.

"""

import bauplan


@bauplan.model()
@bauplan.python('3.11', pip={'pandas': '2.2.0'})
def normalized_taxi_trips(
        trips=bauplan.Model(
            'taxi_fhvhv',
            columns=[
                'PULocationID',
                'request_datetime',
                'on_scene_datetime',
                'pickup_datetime',
                'dropoff_datetime',
            ],
            filter="pickup_datetime >= '2022-12-01T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'"
        ),
        zones=bauplan.Model(
            'taxi_zones'
        ),
):
    """

    this function runs a Python S3 scan over the tables 'taxi_fhvhv' and 'taxi_zones' in the data catalog
    joins them and return a new table using PyArrow https://arrow.apache.org/docs/python/index.html

    """

    import math
    import pyarrow.compute as pc

    # print some debug info - like the size and the number of rows of the table obtained from taxi_fhvhv
    size_in_gb = round(trips.nbytes / math.pow(1024, 3), 3)
    print(f"\nTaxi trips table is {size_in_gb} GB and has {trips.num_rows} rows\n")

    # join 'trips' with 'zones' on 'PULocationID' so get all the Zones and the Boroughs associated to each pickup location
    pickup_location_table = trips.join(zones, 'PULocationID', 'LocationID').combine_chunks()
    pickup_location_table = pc.drop_null(pickup_location_table)

    # return an arrow table
    return pickup_location_table


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'pandas': '2.2.0'})
def taxi_trip_waiting_times(
        data=bauplan.Model(
            'normalized_taxi_trips',
        )
):
    """

    This function computes the waiting time for a taxi ride as the time interval between:
    - request_datetime - the request time for a ride
    - on_scene_datetime - the time at which the taxi showed up at the pickup location

    this function uses PyArrow https://arrow.apache.org/docs/python/index.html

    """

    import pyarrow.compute as pc

    # compute the difference between request_datetime and on_scene_datetime
    # which tells us the waiting time between the call and the taxi arrival on site
    waiting_time_min = pc.minutes_between(data['request_datetime'], data['on_scene_datetime'])
    # append the new columns to the table
    data = data.append_column('waiting_time_minutes', waiting_time_min)

    # return an arrow table
    return data


@bauplan.model(
    materialization_strategy='REPLACE'
)
@bauplan.python('3.11', pip={'duckdb': '0.10.3'})
def zone_avg_waiting_times(
        taxi_trip_waiting_times=bauplan.Model(
            'taxi_trip_waiting_times'
        )
):
    """

    this function computes the mean waiting time for pickup zones using DuckDB https://duckdb.org/docs/
    the output is a table with all the NY zones ordered by how long it takes to get a taxi cab on average

    """

    import duckdb

    # the following code uses DuckDB
    # because DuckDB can query directly Arrow tables we do not need to do anything and can query directly the input tables
    sql_query="""
    SELECT
        Borough,
        Zone,
        AVG(waiting_time_minutes) AS avg_waiting_time
    FROM taxi_trip_waiting_times
    GROUP BY Borough, Zone
    ORDER BY avg_waiting_time DESC;
    """

    # run the query and return the results as an Arrow Table
    data = duckdb.sql(sql_query).arrow()

    # return an Arrow table
    return data
