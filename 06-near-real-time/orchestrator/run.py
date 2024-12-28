"""

This script simulates an ingestion, transformation and dashboard using Bauplan, Prefect, and Streamlit:
check the README.md for more information on how to run this script and the context for the example.

To run this script, make sure you have the packages in the requirements.txt installed, then just run:

python run.py --username bauplan_username --namespace your_newnamespace --dev_branch my_branch

(namespace is optional, but you should provide your own if you're using a shared Bauplan environment)

Check the script parameters at the end of the file to see how to run this script from the command line
with more parameters.

"""

### imports

import datetime
import time
import bauplan
from prefect import flow, task
from prefect.cache_policies import NONE
import boto3
import pandas as pd
from utils import one_off_setup, get_random_events_from_source_table
import tempfile
import uuid
from os.path import dirname, abspath


@task(cache_policy=NONE)
def update_dashboard_tables(
    client: bauplan.Client,
    dev_branch: str,
    namespace: str
):
    """

    This runs the bauplan project in the orchestrator/pipeline_analytics/ directory.

    Generate synthetic data and append it to the ecommerce_stream table in the ingest_branch.
    Then, rebuild the analytics models based on the updated ecommerce_stream table.

    """
    # we need to point the SDK to the directory containing the pipeline
    d = dirname(dirname(abspath(__file__)))
    run_state = client.run(
        project_dir=f"{d}/pipeline_analytics",
        ref=dev_branch,
        namespace=namespace
    )
    if run_state.job_status != "SUCCESS":
        raise Exception("Error during analytics pipeline!")

    return


@task(cache_policy=NONE)
def create_data_in_ingestion_bucket(
    s3_client: boto3.client,
    bauplan_client: bauplan.Client,
    namespace: str,
    dev_branch: str,
    bucket_name: str,
    flow_timestamp: int
):
    """

    Simulate the ingestion of new events by creating synthetic data in the S3 bucket as a parquet
    file in the folder bucket_name/flow_timestamp/....parquet
    
    NOTE: this is not the most efficient code, but it's only a mock function to generate constantly "new" rows
    in the Iceberg source table for the analytics DAG.

    """
    # get some random events from the source table
    df = get_random_events_from_source_table(bauplan_client, namespace, branch=dev_branch)
    # add one day to the max_hour field, generating a new max event_hour basically
    df['event_hour'] = pd.to_datetime(df['max_hour'])
    df['event_hour'] += pd.Timedelta(days=1)
    # drop the max_hour column
    df = df.drop(columns=['max_hour'])
    # write the data to a parquet file and upload it to S3
    with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
        tmp_file_name = tmp_file.name
        df.to_parquet(tmp_file_name, engine='pyarrow', index=False)
        s3_client.upload_file(tmp_file_name, bucket_name, f"{flow_timestamp}/{uuid.uuid4()}.parquet")

    return


@task(cache_policy=NONE)
def ingest_on_a_branch(
    client: bauplan.Client,
    namespace: str,
    bucket_name: str,
    dev_branch: str,
    username: str,
    flow_run_ingestion_timestamp: int,
    merge_on_success: bool = True
):
    """

    Run the ingestion pipeline on a branch. If no errors are raised, merge the branch into main
    if merge_on_success is True.

    For a longer discussion on ingestion best practices with Bauplan (e.g. Write-Audit-Publish),
    check our blog: https://www.prefect.io/blog/prefect-on-the-lakehouse-write-audit-publish-pattern-with-bauplan

    """
    ingest_branch = f"{username}.ingest_{flow_run_ingestion_timestamp}"
    # create the branch
    client.create_branch(branch=ingest_branch, from_ref=dev_branch)
    # ingest the data
    import_state = client.import_data(
        table='ecommerce_clean',
        search_uri=f's3://{bucket_name}/{flow_run_ingestion_timestamp}/*.parquet',
        branch=ingest_branch,
        namespace=namespace
    )
    if import_state.error:
        raise Exception(f"Error during ingestion: {import_state.error}")
    # finally, delete the branch if the merge was successful
    if merge_on_success:
        client.merge_branch(source_ref=ingest_branch, into_branch=dev_branch)
        client.delete_branch(branch=ingest_branch)

    return ingest_branch


@flow(log_prints=True)
def analytics_with_bauplan(
    bucket_name: str,
    username: str,
    namespace: str,
    dev_branch: str
) -> None:
    """

    Run the ingestion and analytics pipeline using Bauplan in a Prefect flow.

    """
    print(f"Starting realtime analytics pipeline at {datetime.datetime.now()}!")

    # instantiate the clients
    s3_client = boto3.client('s3')
    client = bauplan.Client()

    # some flow level vars
    flow_run_ingestion_timestamp = int(time.time())

    # 1: simulate a stream of new events by creating synthetic data in the S3 bucket with the timestamp
    # NOTE: in the real world, this happens OUTSIDE of the flow thanks to some ingestion process!
    create_data_in_ingestion_bucket(
        s3_client,
        client,
        namespace,
        dev_branch,
        bucket_name,
        flow_run_ingestion_timestamp
    )
    print(f"New data created in S3 bucket {bucket_name}")

    # 2: get the fresh data from S3 and append it to the bauplan table in a temporary ingest branch
    ingest_branch = ingest_on_a_branch(
        client,
        namespace,
        bucket_name,
        dev_branch,
        username,
        flow_run_ingestion_timestamp
    )
    print(f"Data ingested successfully on temporary branch {ingest_branch}")

    # 3: finally, we update the dashboard tables with a bauplan DAG run
    update_dashboard_tables(
        client,
        dev_branch,
        namespace
    )
    print("Updated dashboard tables successfully!")

    # say goodbye
    print(f"DAG completed {datetime.datetime.now()}. See you, space cowboy!")

    return


if __name__ == "__main__":
    # Define the parameters for the script
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", type=str, required=True)
    parser.add_argument("--namespace", type=str, default="examples")
    parser.add_argument("--bucket_name", type=str, default=None)
    parser.add_argument("--dev_branch", type=str, default='analytics_dev')
    args = parser.parse_args()

    # Parse the args when the script is run from the command line
    username = args.username
    namespace = args.namespace
    bucket_name = "alpha-hello-bauplan" if args.bucket_name is None else args.bucket_name
    # append the user name to the branch if the user forgot to do so
    dev_branch = args.dev_branch if args.dev_branch.startswith(username) else f"{username}.{args.dev_branch}"
    print(
        "Starting scheduled Prefect + Bauplan flow with the following parameters:"
        f"\n    username={username}"
        f"\n    namespace={namespace}"
        f"\n    bucket_name={bucket_name}"
    )
    # run the one-off setup
    is_setup_done = one_off_setup(namespace, bucket_name, dev_branch)

    # Run ingestion + building tables on a schedule with Prefect
    analytics_with_bauplan.serve(
        name="analytics_with_bauplan",
        interval=60 * 5,
        parameters={
            'username': username,
            'namespace': namespace,
            'bucket_name': bucket_name,
            'dev_branch': dev_branch
        }
    )
