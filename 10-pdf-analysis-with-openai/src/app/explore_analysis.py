"""

This is a simple app reading the artifacts prepared by the data pipeline and saved in
bauplan.

To run the app, simply execute:

streamlit run explore_analysis.py

The app assumes you have run the pipeline in a branch: please select the right branch to query the data
and get started!

Check the code for the arguments you can pass to the script. Note that streamlit run parses parameters
slightly differently, so you need to use the -- separator to pass arguments to the script, e.g.:

streamlit run explore_analysis.py -- --bauplan_user_name foo

"""
import streamlit as st
import sys
import pandas as pd
import matplotlib.pyplot as plt
import bauplan


### GLOBAL CLIENTS ###
# we instantiate them once
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


def plot_bar_chart(
    brands: list,
    counts: list
):
    """
    Plot a bar chart!
    """
    fig, ax = plt.subplots()
    ax.bar(brands, counts)
    ax.set_title('# of report by sentiment')
    st.pyplot(plt)
    
    return


### THE STREAMLIT APP BEGINS HERE ###
def main(
    bauplan_user_name: str,
    one_big_table_name: str
):
    """
    Streamlit-based interactive dashboard to explore investment sentiment recommendations made by an LLM.

    This function allows users to:
    - Select a Bauplan data branch containing investment recommendations.
    - Query and display the sentiment distribution from the specified table.
    - Visualize sentiment data in a bar chart.

    :param bauplan_user_name: str - The Bauplan username used to fetch available branches.
    :param one_big_table_name: str - The name of the table containing investment sentiment data.
    :return None - This function runs the Streamlit UI and does not return a value.
    """

    st.title('Explore the investment recommendations made by the LLM!')
    # debug line to ensure correct Python interpreter
    print(sys.executable)
    all_branches = list(_.name for _ in bauplan_client.get_branches(user=bauplan_user_name))
    target_branch = st.selectbox(f'Pick the branch with {one_big_table_name}:', all_branches, index=None)
    st.write(f'You selected: {target_branch}')
    if target_branch is None:
        st.write('Please select a branch to continue!')
        st.stop()
    # plot a bar chart with the sentiment distribution
    st.write('Sentiment distribution:')
    sql_query = f"""
    SELECT
        sentiment AS investment_sentiment, count(*) as _C
    FROM
        {one_big_table_name}
    GROUP BY
        1
    ORDER BY
        2 DESC
    """
    brand_table = query_as_arrow(bauplan_client, sql_query, target_branch)
    if brand_table is None:
        st.write('Something went wrong! Please try again!')
        st.stop()
        
    plot_bar_chart(
        brands=brand_table['investment_sentiment'].to_pylist(),
        counts=brand_table['_C'].to_pylist()  
    )
    
    return
        

if __name__ == "__main__":
    # parse the arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--bauplan_username', type=str)
    parser.add_argument('--one_big_table_name', type=str, default='bauplan.sec_10_q_analysis_openai')
    args = parser.parse_args()
    # start the app
    main(
        bauplan_user_name=args.bauplan_username,
        one_big_table_name=args.one_big_table_name
    )
