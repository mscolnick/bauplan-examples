"""
This file contains a Streamlit app to visualize the table 'top_pickup_location' created with a Bauplan pipeline
contained in this project. This simple script shows how to use bauplan Python SDK to embed querying functionalities in a webapp.
To run this script run in your terminal:
    streamlit run viz_app.py -- --branch <YOUR_BRANCH_NAME>
"""

import streamlit as st
import sys
from argparse import ArgumentParser
import plotly.express as px
import bauplan
import pandas as pd


@st.cache_data()
def query_as_dataframe(
        _client: bauplan.Client,
        sql: str,
        branch: str
) -> pd.DataFrame:
    """
    Runs a query with bauplan and put the table in a Pandas dataframe
    """
    try:
        df = _client.query(query=sql, ref=branch).to_pandas()
        return df
    except bauplan.exceptions.BauplanError as e:
        print(f"Error: {e}")
        return None


def plot_interactive_chart(df: pd.DataFrame) -> None:
    """
    Creates an interactive bar chart using Plotly Express
    """
    # Define the figure to display in the app
    fig = px.bar(
        df,
        y='Zone',
        x='number_of_trips',
        orientation='h',
        title='Number of Trips per Zone',
        labels={'number_of_trips': 'Number of Trips', 'Zone': 'Zone'},
        height=800
    )

    # Customize the layout
    fig.update_layout(
        showlegend=False,
        xaxis_title="Number of Trips",
        yaxis_title="Zone",
        hoverlabel=dict(bgcolor="white"),
        margin=dict(l=20, r=20, t=40, b=20)
    )

    # Display the plot in Streamlit
    st.plotly_chart(fig, use_container_width=True)


def main():
    # Set up command line argument parsing
    parser = ArgumentParser()
    parser.add_argument('--branch', type=str, required=True, help='Branch name to query data from')
    args = parser.parse_args()

    # set up the table name as a global
    table_name = 'top_pickup_locations'

    st.title('A simple data app to visualize taxi rides and locations in NY')

    # instantiate a bauplan client
    client = bauplan.Client()

    # Using the branch from command line argument
    branch = args.branch

    # Query the table top_pickup_locations using bauplan
    df = query_as_dataframe(
        _client=client,
        sql=f"SELECT * FROM {table_name}",
        branch=branch
    )

    if df is not None and not df.empty:
        # Add a toggle for viewing raw data
        if st.checkbox('Show raw data'):
            st.dataframe(df.head(50), width=1200)

        # Display the interactive plot
        plot_interactive_chart(df=df.head(50))
    else:
        st.error('Error retrieving data. Please check your branch name and try again.')


if __name__ == "__main__":
    main()
