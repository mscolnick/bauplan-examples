# Import required libraries
import streamlit as st
import pandas as pd
import bauplan
import plotly.express as px


# NOTE: change here if you run analytics pipeline in a different namespace
NAMESPACE = "examples"
TABLE_NAME = "ecommerce_metrics_base"


def main():
    # Initialize Bauplan client for data access
    client = bauplan.Client()

    # --- USER AND BRANCH SELECTION SECTION ---
    # Get all branches from Bauplan
    branches = [branch.name for branch in client.get_branches()]
    # Get unique users from branch names (assuming format: username.branchname)
    users = sorted(set(branch.split('.')[0] for branch in branches if '.' in branch))

    # Create user dropdown in sidebar
    selected_user = st.sidebar.selectbox("Select bauplan user", ['None'] + users)

    # Only proceed if a user is selected
    if selected_user != 'None':
        # Filter branches for selected user
        user_branches = [
            branch.split('.')[1]
            for branch in branches
            if branch.startswith(f"{selected_user}.")
        ]
        # Create branch dropdown
        selected_branch_name = st.sidebar.selectbox("Select Branch", user_branches)
        # Construct full branch name
        full_branch = f"{selected_user}.{selected_branch_name}"
        # Check if namespace AND table exist in selected branch
        if not client.has_table(f"{NAMESPACE}.{TABLE_NAME}", full_branch):
            st.write(f"Table {TABLE_NAME} not found in branch {full_branch}")
            st.write(f"Please pick a branch containing the table {TABLE_NAME} in the namespace {NAMESPACE}")
            st.stop()

        # --- DATA FETCHING SECTION ---
        try:
            # SQL query to get main KPIs by date
            query = f"""
                SELECT
                    event_hour::DATE as date,
                    SUM(revenue)::FLOAT as total_revenue,
                    SUM(orders)::INT as total_orders,
                    SUM(unique_sessions)::INT as unique_visitors,
                    ROUND(CAST(SUM(orders) AS FLOAT) / CAST(SUM(unique_sessions) AS FLOAT) * 100, 2) as conversion_rate
                FROM {TABLE_NAME}
                GROUP BY 1
                ORDER BY 1 DESC
            """

            # Execute query and get data
            df = client.query(query, ref=full_branch, namespace=NAMESPACE).to_pandas()

            # --- VISUALIZATION SECTION ---
            # Display KPI table
            st.header("📊 Daily KPI Summary")
            st.dataframe(df, use_container_width=True)

            st.header("📈 Daily Revenue Trend")
            # Create simple line chart for revenue
            fig = px.line(
                df,
                x='date',
                y='total_revenue',
                title='Daily Revenue'
            )
            # Display the chart
            st.plotly_chart(fig, use_container_width=True)



        except Exception as e:
            st.error(f"Error fetching data: {e}")

if __name__ == "__main__":
    main()
