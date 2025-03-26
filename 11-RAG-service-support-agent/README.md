# RAG with bauplan and pinecone


## Overview
This is a reference implementation of a RAG pipeline (and LLM app) built using [bauplan](https://www.bauplanlabs.com/) on Iceberg for data preparation and training, and [pinecone](https://www.pinecone.io/) for serving.

## Use case

Given historical support tickets (StackOverflow-type questions and answers), how do we build a self-service app where users can ask questions and get answers in real-time?

We use bauplan to build a data pipeline for RAG: we start from historical data in tabular formats, do some data joining, wrangling and exploration, then leverage Pinecone's APIs to get text embeddings and store them in a powerfull vector database.

We then build a Streamlit app to explore the embedding space, and provide a simple UI for users to get answers based on natural language queries (powered by Pinecone and OpenAI APIs).

_Credits_:

 * Q&A data come from the _StackSample Dataset_ (originally from [Kaggle](https://www.kaggle.com/datasets/stackoverflow/stacksample), available as sample dataset in the bauplan sandbox).

### Data flow

In the end-to-end example, the data flow as follows between tools and environments:

1. the original dataset is stored in three bauplan-backed Iceberg tables. When joining bauplan, the dataset is already available in the `public` namespace;
2. the pipeline in `src/bpln_pipeline` contains the data modelling and embedding-for-rag code, in simple decorated Python functions; running the pipeline in bauplan will execute these functions and store the embeddings both as an Iceberg table and in Pinecone;
3. the streamlit app in `src/app` showcases how to get back the embeddings from bauplan (high latency / high throughput) for analysis and visualization, and how to interact with a live q&a system, powered by the vector index in Pinecone and an off-the-shelf LLM for text generation (e.g. OpenAI).

Note: both the pipeline and the app code are heavily commented and written with pedagogical purposes in mind. However, do not hesitate to reach out to the bauplan team for any questions or clarifications.

## Setup

### Python environment

To run the project, you need to have Python 3.10 (or later) installed. We recommend using a virtual environment to manage dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### bauplan

* [Join](https://www.bauplanlabs.com/#join) the bauplan sandbox, sign in, create your username and API key;
* complete do the 3-min [tutorial](https://docs.bauplanlabs.com/en/latest/tutorial/index.html) to get familiar with the platform;
* when you gain access, public datasets (including the one used in this project) will be available for you to start building pipelines.

### Pinecone

* Create a cluster on [Pinecone](https://docs.pinecone.io/guides/get-started/quickstart);
* get your API key from the Pinecone dashboard.

## Run

### Check out the dataset

Using bauplan, it is trivial to get acquainted with the dataset and its schema. We have three tables for this use case, questions, answers and tags. You can query the table structure directly from the CLI:

```bash
bauplan table get public.stack_overflow_questions
bauplan table get public.stack_overflow_answers
bauplan table get public.stack_overflow_tags
```

You can find out the distribution of tags in the dataset, for example, by directly querying the table in the CLI:

```bash
bauplan query "SELECT tag, COUNT(*) AS _C FROM public.stack_overflow_tags  GROUP BY 1 ORDER BY _C DESC"
```

### Running the data pipeline with bauplan

To run the pipeline - i.e. the DAG going from the source tables to embeddings and a fully built index in Pinecone -- you just need to create a [data branch](https://docs.bauplanlabs.com/en/latest/tutorial/02_catalog.html) to develop safely in the cloud:

```bash
cd src/bpln_pipeline
bauplan branch create <YOUR_USER_NAME>.soflow_rag
bauplan branch checkout <YOUR_USER_NAME>.soflow_rag
```

Now, add the Pinecone API key as a secret to the project (note: don't commit the yml to a public repo!): this will allow bauplan to connect to the cluster securely through asymmetric encryption:

```bash
bauplan parameter set --name pinecone_key --value aaabbbccc --type secret
```

If you inspect your `bauplan_project.yml` file, the new parameter will be found:

```yaml
parameters:
    pinecone_key:
        type: secret
        default: kUg6q4141413...
        key: awskms:///arn:aws:kms:us-...
```

You can now run the DAG with a command:

```bash
bauplan run
```

You can check that we successfully created the table with the embedding from the CLI:

```bash
bauplan table get one_big_qa_table_with_embeddings
```

You could also query the intermediate table, to get (for example) the number of answers per question:

```bash
bauplan query "SELECT question_id, COUNT(*) as _C FROM one_big_qa_table GROUP BY question_id ORDER BY 2 DESC LIMIT 10"
```

### Answering questions with Pinecone (and OpenAI)

We can visualize the structure of the embedding space using the Streamlit app, and then do the runtime RAG to simulate a user asking support questions. The RAG flow leverages the index we built running bauplan, together with a LLM model for the answer generation (in our case, we use OpenAI).

To run the app, make sure to pass a valid OpenAI API key as an environment variable, together with your Pinecone API key:

```bash
cd src/app
OPENAI_API_KEY=$$$ PINECONE_KEY=$$$ streamlit run explore_and_answer.py
```

The app will open in your browser, and you can start exploring the embedding space and asking questions. Note how easy is to interact with both bauplan and Pinecone from any Python process!

A video walkthrough of the app is available [here](https://www.youtube.com/watch?v=3Q6J9Q1Z9ZQ).

## Where to go from here?

TBD

## License

The code in this repository is released under the MIT License and provided as is.