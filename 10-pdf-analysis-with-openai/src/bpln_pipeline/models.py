import bauplan


@bauplan.model(internet_access=True)
# we use boto3 to get the PDFs from S3 and markitdown to convert the PDFs to text
@bauplan.python('3.11', pip={'boto3': '1.35.86', 'markitdown': '0.0.1a3'})
def sec_10_q_markdown(
    data=bauplan.Model(
        'public.sec_10_q_metadata',
        # NOTE: apple pdfs are not well-supported by markitdown!
        filter="company != 'Apple Inc.'"
        # filter="company != 'Apple Inc.'"
    )
):
    """
    This function reads the metadata and the PDFs from S3 and converts them to markdown.
    The final table is therefore the same as the input table without bucket and path, but with an additional column:

    | id | company | year | quarter | markdown_text  |
    |----|---------|------|---------|----------------|
    | 1  | Amazon  | 2021 | 1       |  ...           |
    """
    import boto3
    import tempfile
    from markitdown import MarkItDown

    # we instantiate the client
    # since we are running in the bauplan alpha and reading from a connected bucket
    # we don't need to specify credentials - of course, you could instantiate
    # the client with your own credentials using bauplan secrets
    s3 = boto3.client('s3')
    # get lists from the Arrow columns, to iterate over them
    bucket_name = data['bucket'].to_pylist()
    object_key = data['pdf_path'].to_pylist()
    # we will store the Markdown text in a list
    markdown_text = []
    md = MarkItDown()
    # NOTE: we just run a few PDFs here, to keep the runtime low
    for bucket, pdf_path in zip(bucket_name, object_key):
        with tempfile.NamedTemporaryFile() as tmp_file:
            print(f"\n>>>> Downloading {pdf_path.split('/')[-1]} from {bucket}\n")
            s3.download_fileobj(bucket, pdf_path, tmp_file)
            # markitdown can be a bit slow, so if it takes a few seconds per line, don't panic.
            result = md.convert(tmp_file.name)
            markdown_text.append(result.text_content)

    # we add the Markdown text to the data
    data = data.append_column('markdown_text', [markdown_text])
    # remove the bucket and path columns
    data = data.drop_columns(['bucket', 'pdf_path'])

    return data


# we are going to materialize the output of this function in the data lake as an Iceberg table named sec_10_q_analysis_openai
@bauplan.model(internet_access=True, materialization_strategy='REPLACE')
# we use the openai Python client to call ChatGPT and analyze the text we saved as Markdown in the previous step
@bauplan.python('3.11', pip={'openai': '1.57.2'})
def sec_10_q_analysis_openai(
        data=bauplan.Model('sec_10_q_markdown'),
        # add openai apikey as a parameter for this function
        openai_api_key=bauplan.Parameter('openai_api_key'),
):
    """
    This function reads the Markdown text of each document and uses the LLM to predict the investment sentiment.
    The final table is therefore the same as the input table, minus markdown_text, and with an additional column
    containing the investment sentiment based on the LLM analysis:

    | id | company | year | quarter | investment_sentiment |
    |----|---------|------|---------| -------------------- |
    | 1  | Amazon  | 2021 | 1       | positive             |

    """
    from openai import OpenAI
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from gpt_utils import process_row

    # get the data as lists from the Arrow columns, to iterate over them
    companies = data['company'].to_pylist()
    years = data['year'].to_pylist()
    quarters = data['quarter'].to_pylist()
    markdown_texts = data['markdown_text'].to_pylist()

    # now we will use a function defined in the file gpt_utils.py to call ChatGPT for each row of our dataset
    # we will use multiple threads to speed up the calls to the OpenAI
    print("\n\n=====> Start the LLM loop...\n")
    investment_sentiment = []
    max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {
            executor.submit(
                process_row,
                openai_api_key,
                company,
                year,
                quarter,
                markdown
            ): _i for _i, (company, year, quarter, markdown)
            in enumerate(zip(companies, years, quarters, markdown_texts))
        }
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                response_text = future.result()
                print(f"Processed row {index}: {response_text}")
            except Exception as e:
                response_text = f"Error: {str(e)}"
            investment_sentiment.append(response_text)

    # save the results in a Arrow table that can now write back into the data lake as an Iceberg table
    return data.append_column('sentiment', [investment_sentiment])
