"""

This is a simple app reading the artifacts prepared by the data pipeline and saved in both
bauplan and Pinecone. We first retrieve emebddings from bauplan, and display the 2-D TSNE vectors in a scatterplot 
(color-coded by tag!).

We then provide a simple UI to leverage Pinecone + OpenAI for a user-facing Q&A system: the user
can ask a question, and we will use the pre-computed vectors to find the nearest neighbors in the space, and feed those
as context to OpenAI's LLM to generate an answer.

To run the app, simply execute:

streamlit run explore_and_answer.py -- --bauplan_username <YOUR_USERNAME>

The app assumes you have run the pipeline in a branch: please select the right branch to query the data
and get started!

Check the code for the arguments you can pass to the script. Note that streamlit run parses parameters
slightly differently, so you need to use the -- separator to pass arguments to the script:

streamlit run explore_and_recommend.py -- --bauplan_user_name foo

"""


import streamlit as st
import sys
import pandas as pd
import matplotlib.pyplot as plt
import bauplan
from pinecone import Pinecone
from openai import OpenAI
import os


### GLOBAL CLIENTS ###
# we instantiate them once

LLM_CONTEXT = (
    "You are a friendly AI, expert in all things computer."
    "Help me with this question, please: '{}'\n"
    "As useful context, please consider the following interactions occured on this topic on StackOverflow: {}"
)

pc = Pinecone(api_key=os.environ['PINECONE_KEY'])
# this defaults to the OPEN AI key in the environment vars
oai_client = OpenAI() 
# now instantiate the bauplan client
bauplan_client = bauplan.Client()


### UTILITY FUNCTIONS ###

@st.cache_data()
def query_as_arrow(
    _client: bauplan.Client,
    sql: str,
    branch: str,
):
    """
    This function uses the query method to query a table in bauplan. This is 
    handy as a separate function because we can cache the results and avoid
    querying the same data multiple times.
    
    It returns None if the query fails.
    """

    try:
        return _client.query(sql, ref=branch)
    except Exception as e:
        print(e)
        
    return None


def plot_scatterplot_with_lookup(
    title: str, 
    items: list, 
    items_to_target_cat: dict,
    vectors: list
):
    """
    Plot the 2-D vectors in the space, and use the mapping items_to_target_cat
    to color-code the points for convenience
    """
    groups = {}
    for item in items:
        item_idx = items.index(item)
        target_cat = items_to_target_cat[item]
        x = vectors[item_idx][0]
        y = vectors[item_idx][1]
        if target_cat in groups:
            groups[target_cat]['x'].append(x)
            groups[target_cat]['y'].append(y)
        else:
            groups[target_cat] = {
                'x': [x], 'y': [y]
                }
    
    fig, ax = plt.subplots(figsize=(10, 10))
    for group, data in groups.items():
        ax.scatter(data['x'], data['y'], 
                   alpha=0.05 if group == 'unknown' else 0.9, 
                   edgecolors='none', 
                   s=25, 
                   marker='o',
                   label=group)

    plt.title(title)
    plt.legend(loc=2)
    st.pyplot(plt)
    
    return


def vector_search(
    pinecone_client: Pinecone,
    query: str,
    k: int=3
):
    # NOTE: index name and namespace are hardcoded here, but they should be
    # changed if the corresponding code changes in the bauplan pipeline!
    index = pinecone_client.Index("so-qa-index")
    results = index.search_records(
        namespace="bauplan-default", 
        query={
            "inputs": {"text": query},
            "top_k": k
        },
        fields=["chunk_text"]
    )
    # retrieve the closest neighbors
    rows = []
    for r in results['result']['hits']:
        rows.append({
            "id": r['_id'],
            'score': r['_score'],
            "text": r['fields']['chunk_text']
        })
        
    return rows
    
    
### THE STREAMLIT APP BEGINS HERE ###

def main(
    bauplan_user_name: str,
    one_big_table_name: str
):
    st.title('Explore the vector space and ask AI about your programming issue!')
    # debug line to ensure correct Python interpreter
    print(sys.executable)
    all_branches = list(_.name for _ in bauplan_client.get_branches(user=bauplan_user_name))
    target_branch = st.selectbox(f'Pick the branch with {one_big_table_name}:', all_branches, index=None)
    st.write(f'You selected: {target_branch}')
    if target_branch is None or not bauplan_client.has_table(one_big_table_name, target_branch):
        st.write(f'Please select a branch containing the {one_big_table_name} table!')
        st.stop()
    sql_query = f"""
    SELECT 
        question_id, two_d_vectors, tags
    FROM 
        {one_big_table_name}
    """
    table = query_as_arrow(bauplan_client, sql_query, target_branch)
    if table is  None:
        st.write('Something went wrong! Please check your branch and try again!')
        st.stop()
        
    st.dataframe(table.slice(length=3).to_pandas(), width=1200)
    all_items = table['question_id'].to_pylist()
    question_to_tag = dict()
    # associate to each question its tag (pick the first one if multiple)
    for q, t in zip(table['question_id'].to_pylist(), table['tags'].to_pylist()):
        question_to_tag[q] = t[0] if len(t) > 0 else 'unknown'
    # we highlight a few languages for the scatterplot
    target_languages = [ 
        'javascript', 
        'python', 
        'c++'
    ]
    # we mark as unknown interactions outside the target languages, so that 
    # the visualization is more readable
    for t, a in question_to_tag.items():
        if a not in target_languages:
            question_to_tag[t] = 'unknown'
    # plot the embeddings, color-coded by author
    plot_scatterplot_with_lookup(
        title='StackOverflow in (vector) space',
        items=all_items,
        items_to_target_cat=question_to_tag,
        vectors=table['two_d_vectors'].to_pylist()
    )
    
    # now we provide a simple UI to interact with the AI
    user_question = st.text_input('Ask a question:', value='how can I open a Json file in Python?')
    if user_question == '':
        st.stop()
    # retrieval first
    results = vector_search(pc, user_question, k=10)
    st.dataframe(pd.DataFrame(results[:3]), width=1200)
    # now time to do augmented generation!
    # fill the prompt with the user question and the top K retrieved answers
    prompt = LLM_CONTEXT.format(user_question, '\n'.join([r['text'] for r in results]))
    completion = oai_client.chat.completions.create(
       model="gpt-4",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    message_content = completion.choices[0].message.content.strip()
    # print the answer
    st.write('AI response:')
    st.write(message_content)
    
    return
        

if __name__ == "__main__":
    # parse the arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--bauplan_username', type=str)
    parser.add_argument('--one_big_table_name', type=str, default='one_big_qa_table_with_embeddings')
    args = parser.parse_args()
    # start the app
    main(
        bauplan_user_name=args.bauplan_username,
        one_big_table_name=args.one_big_table_name
    )
