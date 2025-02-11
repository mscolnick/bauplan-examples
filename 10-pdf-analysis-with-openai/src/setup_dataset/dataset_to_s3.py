"""

This is a script that takes the files of the Sec10 dataset and upload them to S3
in a bucket, as well as creating a parquet files mapping the individual financial reports
to some metadata. Once the parquet file is in S3, we can creat an Iceberg table using bauplan
built-in functions and abstractions - the table will then be used as a pointer to the
actual PDF during the DAG execution.

This code does NOT need to run as part of the reference implementation, as the dataset is already
available in bauplan's data catalog whenever a new user join the public sandbox. However, it is useful
both as a reference to the original processing and upload choices, as well as a template for users who
wish to upload their own datasets to S3 and use bauplan SDK to quickly get versionable, branchable,
and queryable tables out of files.

To run:

python dataset_to_s3.py

Check the code for the arguments you can pass to the script.

"""

import bauplan
import boto3
import os
from datetime import datetime
import pyarrow.parquet as pq
import tempfile
import glob
import pyarrow as pa
from pathlib import Path


def add_files_to_bauplan_catalog(
        s3_bucket: str,
        s3_folder: str,
        file_name: str,
        table_name: str,
        ingestion_branch: str
):
    """

    We leverage the bauplan SDK to create a table from the S3 URI
    and add data to it in a pure Pythonic way.

    While not the focus of this, note the use of data branches to sandbox the upload
    and safely merge new tables into the production branch of the lakehouse.

    For a deeper dive into the Write-Audit-Publish pattern for ingestion, see our blog post:

    https://www.prefect.io/blog/prefect-on-the-lakehouse-write-audit-publish-pattern-with-bauplan

    """
    # instantiate the bauplan client
    bpln_client = bauplan.Client()
    # drop and recreate the branch
    if bpln_client.has_branch(ingestion_branch):
        bpln_client.delete_branch(ingestion_branch)

    bpln_client.create_branch(ingestion_branch, from_ref='main')
    # create table from S3 URI
    s3_uri = f's3://{s3_bucket}/{s3_folder}/{file_name}'
    bpln_table = bpln_client.create_table(
        table=table_name,
        search_uri=s3_uri,
        branch=ingestion_branch,
        # we use the public namespace, and assume it's already created
        namespace='public',
        replace=True
    )
    print(f"Table {table_name} created!")
    # add the data
    plan_state = bpln_client.import_data(
        table=table_name,
        search_uri=s3_uri,
        branch=ingestion_branch,
        namespace='public',
        client_timeout=60 * 60
    )
    if plan_state.error:
        raise Exception(f"Error importing data: {plan_state.error}")
    # merge the branch to main
    bpln_client.merge_branch(source_ref=ingestion_branch, into_branch='main')
    # drop the branch
    bpln_client.delete_branch(ingestion_branch)

    return


def download_sec_files_locally(local_folder: str):
    """

    We use the Llama index SDK to download the SEC 10Q Dataset files to avoid
    shipping files with the project...

    This function can be replaced by any function that gets the target PDF files
    locally to this process.

    """
    from llama_index.core.llama_dataset import download_llama_dataset
    rag_dataset, documents = download_llama_dataset("DocugamiKgRagSec10Q", local_folder)

    return documents


def upload_and_process(
        local_folder: str,
        s3_bucket: str,
        s3_folder: str,
        table_name: str,
        ingestion_branch: str
):
    # start processing
    print(f"\nStarting upload at {datetime.now()}...")

    # instantiate the s3 client
    # we assume the envs / local credentials are already set and working
    # with the target bucket
    s3_client = boto3.client('s3')
    # get the file name without the path and without the extension
    documents = download_sec_files_locally(local_folder)
    local_individual_files_folder = os.path.join(local_folder, 'source_files')
    pdf_files = glob.glob(f"{local_individual_files_folder}/*.pdf")
    print(f"Found {len(pdf_files)} PDF files.")
    # this list will contain the S3 paths of the files
    # which is used to do the table with the metadata for the
    # lookup during the DAG
    s3_file_paths = []
    for pdf_file in pdf_files:
        # get the file name without the path and without the extension
        # replace any whitespace with underscores, and make it lowercase
        _file_name = Path(pdf_file).stem.replace(' ', '_').lower()
        # upload the file to S3
        s3_file = f"{s3_folder}/{_file_name}.pdf"
        s3_client.upload_file(pdf_file, s3_bucket, s3_file)
        s3_file_paths.append(s3_file)
        print(f"File {_file_name} uploaded to S3.")

    # a conversion dictionary from the 4 letter company code to the full name
    code_to_company = {
        'aapl': 'Apple Inc.',
        'amzn': 'Amazon.com Inc.',
        'msft': 'Microsoft Corporation',
        'nvda': 'NVIDIA Corporation',
        'intc': 'Intel Corporation',
    }

    # create a parquet file with the metadata
    file_name = 'sec_10_q_metadata.parquet'
    pydict = {
        'id': [f"sec_10_q_{i}" for i in range(len(s3_file_paths))],
        'company': [code_to_company[Path(file).stem.replace(' ', '_').lower().split('_')[-1]] for file in pdf_files],
        'year': [int(Path(file).stem.replace(' ', '_').lower().split('_')[0]) for file in pdf_files],
        'quarter': [int(Path(file).stem.replace(' ', '_').lower().split('_')[1][1:]) for file in pdf_files],
        'bucket': [s3_bucket for _ in pdf_files],
        'pdf_path': s3_file_paths
    }
    table = pa.Table.from_pydict(pydict)
    with tempfile.NamedTemporaryFile() as tmp:
        pq.write_table(table, tmp.name)
        s3_client.upload_file(tmp.name, s3_bucket, f"{s3_folder}/{file_name}")
        print(f"Metadata file {file_name} uploaded to S3.")
    # now that the PDF files and the metadata file is in S3 , we can create the Iceberg table in bauplan
    add_files_to_bauplan_catalog(
        s3_bucket=s3_bucket,
        s3_folder=s3_folder,
        file_name=file_name,
        table_name=table_name,
        ingestion_branch=ingestion_branch
    )

    # say goodbye
    print(f"\nUploaded done at {datetime.now()}.\n\nSee you, Space Cowboy.")

    return


if __name__ == '__main__':
    import argparse

    # parse arguments from the command line
    parser = argparse.ArgumentParser()
    parser.add_argument('--local_folder', type=str, default='data')
    parser.add_argument('--s3_bucket', type=str, default='alpha-hello-bauplan')
    parser.add_argument('--s3_folder', type=str, default='SEC_10_Q_dataset')
    parser.add_argument('--table_name', type=str, default='sec_10_q_metadata')
    parser.add_argument('--ingestion-branch', type=str, default='jacopo.sec_10_q_ingestion')
    args = parser.parse_args()
    # run the upload and processing
    upload_and_process(
        local_folder=args.local_folder,
        s3_bucket=args.s3_bucket,
        s3_folder=args.s3_folder,
        table_name=args.table_name,
        ingestion_branch=args.ingestion_branch
    )