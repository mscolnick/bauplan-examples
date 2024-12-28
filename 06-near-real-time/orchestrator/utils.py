import json
import bauplan
from os.path import dirname, abspath
from botocore.exceptions import ClientError
import boto3


### Utility functions


def one_off_setup(
    namespace: str,
    bucket_name: str,
    dev_branch: str,
):
    # Initialize the Bauplan client
    bauplan_client = bauplan.Client()
    # Instantiate the S3 client
    s3_client = boto3.client('s3')
    # Create branch for this project (a dev branch) if it doesn't exist
    if not bauplan_client.has_branch(dev_branch):
        bauplan_client.create_branch(branch=dev_branch, from_ref='main')
    # Before running the ingestion + analytics pipeline, we need to make sure the source table for our
    # DAG - a cleaned version of the Kaggle ecommerce dataset - exists in the target namespace.
    if not bauplan_client.has_table(f"{namespace}.ecommerce_clean", dev_branch):
        print("Building initial cleaned dataset through its own Bauplan pipeline")
        build_initial_cleaned_dataset(bauplan_client, namespace, dev_branch)

    # Create a public S3 bucket to host synthetic data simulating the ingestion of new events
    create_public_bucket(s3_client, bucket_name)

    return True


def get_random_events_from_source_table(
    client: bauplan.Client,
    namespace: str,
    branch: str,
    n: int = 2_000_000
):
    """

    Return a random sample of n events from the source table and the original Arrow schema
    to guarantee that the data is in the expected format when appending to the table.

    """
    result = client.query(
            """
            SELECT
                * EXCLUDE (event_hour),
                (SELECT MAX(event_hour) from ecommerce_clean) as max_hour
            FROM ecommerce_clean
            WHERE event_hour
            BETWEEN
                '2020-01-19 00:00:00'
            AND
                '2020-01-21 00:00:00'
            """,
            namespace=namespace,
            ref=branch
        ).to_pandas()

    tot_rows = len(result)

    return result.sample(min(n, tot_rows))


def does_bucket_exist(
    s3_client,
    bucket_name: str
):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        print(f"Bucket '{bucket_name}' does not exist. Creating it now.")

    return False


def create_public_bucket(
    s3_client,
    bucket_name: str,
):
    """
    Create an S3 bucket and apply public read-only permissions to all objects in it.

    """
    # check if the bucket already exists
    if not does_bucket_exist(s3_client, bucket_name):
        response = s3_client.create_bucket(Bucket=bucket_name)

    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/*"
            },
            # add list object
            {
                "Sid": "PublicListBucket",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{bucket_name}"
            }
        ]
    }
    bucket_policy_str = json.dumps(bucket_policy)
    s3_client.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy_str)
    print(f"Public read-only policy applied to bucket '{bucket_name}'.")

    return True


def build_initial_cleaned_dataset(
    client: bauplan.Client,
    namespace: str,
    dev_branch: str
):
    """

    Build a cleaned, smaller dataset from the "public.ecommerce" table. This operation
    is done once before the ingestion + table DAG gets on a Prefect schedule.

    """
    # we need to point the SDK to the directory containing the pipeline
    d = dirname(dirname(abspath(__file__)))
    client.run(
        project_dir=f"{d}/pipeline_initial",
        ref=dev_branch,
        namespace=namespace,
        client_timeout=60*50
    )
    # make sure the cleaned dataset was built
    assert client.has_table(f"{namespace}.ecommerce_clean", dev_branch), "Failed to build initial cleaned dataset"

    return True
