# Required imports at the top level
import streamlit as st
import pandas as pd
import bauplan
import plotly.express as px
from datetime import datetime
import plotly.graph_objects as go

# Configure the page
st.set_page_config(
    page_title="Ecommerce Analytics Dashboard",
    page_icon="🛍️",
    layout="wide"
)

# Helper Functions
def query_as_dataframe(
    _client: bauplan.Client,
    sql: str,
    branch: str,
):
    """
    Query data from bauplan and return as DataFrame
    """
    try:
        # Add debug information
        st.write(f"Attempting to query with branch: {branch}")

        # Try to execute the query
        df = _client.query(sql, ref=branch).to_pandas()

        # If successful, show the shape of the data
        if not df.empty:
            st.write(f"Query successful! Retrieved {df.shape[0]} rows and {df.shape[1]} columns")

        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        st.error(f"SQL: {sql}")
        st.error(f"Branch: {branch}")
        return pd.DataFrame()

def format_branches(_client: bauplan.Client):
    """
    Formats the names of the branches to handle those with a special syntax, like 'main'
    """
    clean_branches = [branch.name for branch in _client.get_branches()
                     if len(branch.name.split('.')) == 2
                     or branch.name.split('.')[0] == 'main']
    return clean_branches

def get_user_branches(branches: list):
    """
    Group the branches corresponding to usernames from all the branches in the catalog
    """
    users = {branch.split('.')[0] for branch in branches}
    user_branches = [
        {user: [branch.split('.')[1] for branch in branches if branch.split('.')[0] == user]}
        if user != 'main' else {'main': 'main'}
        for user in users
    ]
    return user_branches

# Custom CSS for better styling
# Custom CSS for better styling
st.markdown("""
    <style>
    .stApp {
        background-color: #f8f9fa;
    }
    .metric-card {
        background-color: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
        color: #1f1f1f;
        margin: 10px 0;
    }
    .metric-label {
        color: #666;
        font-size: 14px;
    }
    .chart-container {
        background-color: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 10px 0;
    }
    /* Remove background from Plotly chart container and adjust padding */
    .stPlotlyChart {
        padding: 0;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    div[data-testid="stMetricValue"] {
        font-size: 24px;
    }
    </style>
""", unsafe_allow_html=True)

def create_metric_card(label, value, delta=None, prefix="", suffix=""):
    """Creates a styled metric card"""
    # Format the main value
    if isinstance(value, (int, float)):
        formatted_value = f"{prefix}{value:,.2f}{suffix}"
    else:
        formatted_value = f"{prefix}{value}{suffix}"

    # Handle delta
    delta_html = ""
    if delta is not None:
        color = "green" if delta >= 0 else "red"
        arrow = "↑" if delta >= 0 else "↓"
        delta_html = f'<div style="color: {color}; font-size: 14px; font-weight: bold;">{arrow} {abs(delta):.1f}%</div>'

    # Generate card HTML
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{formatted_value}</div>
            {delta_html}
            <div class="metric-label">{label}</div>
        </div>
    """, unsafe_allow_html=True)

def create_plotly_chart(data, x_col, y_col, title, chart_type='line'):
    """Creates a styled Plotly chart"""
    if chart_type == 'line':
        fig = px.line(data, x=x_col, y=y_col, title=title)
        fig.update_traces(line_color='#1f77b4')
    elif chart_type == 'bar':
        fig = px.bar(data, x=x_col, y=y_col, title=title)
        fig.update_traces(marker_color='#1f77b4')

    fig.update_layout(
        template='plotly_white',
        title_x=0.5,
        title_font_size=16,
        height=400,
        margin=dict(t=50, l=50, r=30, b=50),
        showlegend=True,
        legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
        xaxis_title=x_col.replace('_', ' ').title(),
        yaxis_title=y_col.replace('_', ' ').title()
    )

    return fig

def main():
    # Initialize Bauplan client
    client = bauplan.Client()

    # New branch selection logic
    st.title("🛍️ Ecommerce Analytics Dashboard")

    # Get branches and users
    branches = format_branches(client)
    user_branches = get_user_branches(branches)
    users = sorted([key for d in user_branches for key in d.keys()])

    # Sidebar for branch selection
    st.sidebar.markdown('# Branch Selection')
    selected_user = st.sidebar.selectbox("Select a user", ['None'] + users, key="user_select")

    # Initialize selected_branch
    selected_branch = None

    # Handle branch selection
    if selected_user == 'None':
        st.warning("Please select a user to begin")
        st.stop()

    if selected_user == 'main':
        selected_branch = 'main'
    else:
        drop_down_branches = next((d[selected_user] for d in user_branches if selected_user in d), None)
        selected_branch_name = st.sidebar.selectbox("Select a branch", ['None'] + drop_down_branches, key="branch_select")
        if selected_branch_name == 'None':
            st.warning(f"Hi {selected_user}. Please select a branch to continue.")
            st.stop()
        selected_branch = f"{selected_user}.{selected_branch_name}"

    if not selected_branch:
        st.warning("Please select a valid branch to continue")
        st.stop()

    st.markdown("""---""")

    # Debug info to verify branch selection
    st.sidebar.write(f"Current branch: {selected_branch}")

    # Rest of your dashboard code using selected_branch
    with st.spinner('Loading dashboard data...'):
        try:
            sql_query = """
                SELECT
                    event_hour,
                    SUM(views)::INT AS views,
                    SUM(purchased_products)::INT as purchased_products,
                    SUM(revenue)::FLOAT AS revenue,
                    sum(unique_sessions)::INT as unique_sessions,
                    sum(total_sessions)::INT as total_sessions,
                    sum(orders)::INT as orders,
                    CAST(SUM(orders) AS FLOAT) / CAST(SUM(unique_sessions) AS FLOAT) AS click_through_rate
                FROM examples.ecommerce_metrics_base
                GROUP BY 1
                ORDER BY 1 desc
            """
            # Use the dynamically selected branch
            data = query_as_dataframe(client, sql_query, selected_branch)

            if not data.empty:
                # Calculate period-over-period metrics
                latest_revenue = data['revenue'].iloc[0]
                prev_revenue = data['revenue'].iloc[1]
                revenue_delta = ((latest_revenue - prev_revenue) / prev_revenue) * 100

                # Key Metrics Section
                st.subheader("📊 Key Metrics")
                col1, col2, col3, col4 = st.columns(4)

                # Calculate deltas for each metric
                latest_sessions = data['total_sessions'].iloc[0]
                prev_sessions = data['total_sessions'].iloc[1]
                sessions_delta = ((latest_sessions - prev_sessions) / prev_sessions) * 100

                latest_orders = data['orders'].iloc[0]
                prev_orders = data['orders'].iloc[1]
                orders_delta = ((latest_orders - prev_orders) / prev_orders) * 100

                latest_ctr = data['click_through_rate'].iloc[0] * 100
                prev_ctr = data['click_through_rate'].iloc[1] * 100
                ctr_delta = ((latest_ctr - prev_ctr) / prev_ctr) * 100

                with col1:
                    st.metric(
                        label="Revenue",
                        value=f"${latest_revenue:,.2f}",
                        delta=f"{revenue_delta:.1f}%" if revenue_delta else None
                    )

                with col2:
                    st.metric(
                        label="Total Sessions",
                        value=f"{int(latest_sessions):,}",
                        delta=f"{sessions_delta:.1f}%" if sessions_delta else None
                    )

                with col3:
                    st.metric(
                        label="Orders",
                        value=f"{int(latest_orders):,}",
                        delta=f"{orders_delta:.1f}%" if orders_delta else None
                    )

                with col4:
                    st.metric(
                        label="CTR",
                        value=f"{latest_ctr:.2f}%",
                        delta=f"{ctr_delta:.1f}%" if ctr_delta else None
                    )

                # Charts Section
                st.markdown("---")
                st.subheader("📈 Performance Metrics")

                tab1, tab2, tab3 = st.tabs(["Sessions", "Revenue", "Conversion"])

                with tab1:
                    fig = create_plotly_chart(
                        data,
                        'event_hour',
                        'total_sessions',
                        'Hourly Session Trends',
                        'bar'
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with tab2:
                    fig = create_plotly_chart(
                        data,
                        'event_hour',
                        'revenue',
                        'Hourly Revenue',
                        'line'
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with tab3:
                    fig = create_plotly_chart(
                        data,
                        'event_hour',
                        'click_through_rate',
                        'Hourly Click-Through Rate',
                        'line'
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # Brand Performance Section
                st.markdown("---")
                st.subheader("🏢 Brand Performance")

                with st.spinner('Loading brand metrics...'):
                    order_sql_query = "SELECT * FROM examples.metrics_orders"
                    order_data = query_as_dataframe(client, order_sql_query, selected_branch)

                    if not order_data.empty:
                        # Style and display the dataframe
                        st.dataframe(
                            order_data.style.background_gradient(
                                cmap='Blues',
                                subset=['revenue']
                            ),
                            use_container_width=True,
                            height=400
                        )
                    else:
                        st.warning("No brand performance data available.")

            else:
                st.error(f"No data available for the selected branch: {selected_branch}")

        except Exception as e:
            st.error(f"Error loading dashboard for branch {selected_branch}: {str(e)}")

if __name__ == "__main__":
    main()
