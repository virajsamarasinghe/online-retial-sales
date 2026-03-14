import streamlit as st
import pandas as pd
import plotly.express as px
import os

# Set page configuration
st.set_page_config(
    page_title="Retail Sales Performance Dashboard",
    page_icon=None,
    layout="wide"
)

# Custom CSS for professional/corporate look
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    .main {
        background-color: #f8fafc;
    }
    
    .stMetric {
        background-color: #ffffff;
        padding: 24px;
        border-radius: 8px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06);
        min-height: 140px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    
    div[data-testid="stMetricValue"] {
        color: #1e3a8a;
        font-weight: 700;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }

    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
        color: #64748b;
    }

    .stTabs [aria-selected="true"] {
        color: #1e3a8a !important;
        border-bottom: 2px solid #1e3a8a !important;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

st.title("Retail Sales Performance Dashboard")
st.markdown("<p style='color: #64748b; font-size: 1.1rem;'>Hadoop MapReduce Analytical Results</p>", unsafe_allow_html=True)

# Data loading functions
DOCS_DIR = "docs"

@st.cache_data
def load_country_revenue():
    path = os.path.join(DOCS_DIR, "Task1_CountryRevenue.txt")
    df = pd.read_csv(path, sep='\t', names=['Country', 'Revenue'])
    return df.sort_values('Revenue', ascending=False)

@st.cache_data
def load_monthly_trends():
    path = os.path.join(DOCS_DIR, "Task2_MonthlyTrends.txt")
    df = pd.read_csv(path, sep='\t', names=['Month', 'Revenue'])
    return df

@st.cache_data
def load_country_monthly():
    path = os.path.join(DOCS_DIR, "Task3_CountryMonthly.txt")
    data = []
    with open(path, 'r') as f:
        for line in f:
            if line.strip():
                parts = line.strip().split('\t')
                if len(parts) == 2:
                    key, val = parts
                    # Key is Country_Year-Month
                    if '_' in key:
                        country, month = key.rsplit('_', 1)
                        data.append({'Country': country, 'Month': month, 'Revenue': float(val)})
    return pd.DataFrame(data)

@st.cache_data
def load_market_leaders():
    path = os.path.join(DOCS_DIR, "Task4_MarketLeaders.txt")
    df = pd.read_csv(path, sep='\t', names=['Month', 'Country', 'Revenue'])
    return df

# Sidebar
st.sidebar.header("Analysis Controls")
try:
    df_country = load_country_revenue()
    df_monthly = load_monthly_trends()
    df_country_monthly = load_country_monthly()
    df_market_leaders = load_market_leaders()

    # Professional color scale
    BLUE_PALETTE = ['#1e3a8a', '#1e40af', '#1d4ed8', '#2563eb', '#3b82f6', '#60a5fa', '#93c5fd']

    # Metrics
    total_revenue = df_country['Revenue'].sum()
    total_countries = df_country['Country'].nunique()
    top_country = df_country.iloc[0]['Country']
    top_revenue = df_country.iloc[0]['Revenue']

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Global Revenue", f"£{total_revenue:,.2f}")
    col2.metric("Market Reach", f"{total_countries} Countries")
    col3.metric("Leading Market", top_country, f"£{top_revenue:,.2f}")

    st.divider()

    # Tabs for different views
    tab1, tab2, tab3 = st.tabs(["Geographic Analysis", "Temporal Trends", "Market Leaders"])

    with tab1:
        st.subheader("Revenue Distribution by Country")
        fig_country = px.bar(
            df_country.head(15), 
            x='Country', 
            y='Revenue',
            color='Revenue',
            color_continuous_scale=BLUE_PALETTE,
            template='plotly_white',
            title="Top 15 Countries by Performance"
        )
        fig_country.update_layout(
            xaxis_title="Country",
            yaxis_title="Total Revenue (£)",
            font=dict(family="Inter, sans-serif")
        )
        st.plotly_chart(fig_country, use_container_width=True)
        
        # Show table
        with st.expander("Details View: Full Revenue Data"):
            st.dataframe(df_country.style.format({"Revenue": "£{:,.2f}"}), use_container_width=True)

    with tab2:
        st.subheader("Global Growth Trends")
        fig_monthly = px.line(
            df_monthly, 
            x='Month', 
            y='Revenue',
            markers=True,
            template='plotly_white',
            line_shape='spline',
            color_discrete_sequence=['#1e3a8a'],
            title="Revenue Trajectory (Dec 2010 - Dec 2011)"
        )
        fig_monthly.update_layout(
            xaxis_title="Month",
            yaxis_title="Monthly Revenue (£)",
            font=dict(family="Inter, sans-serif")
        )
        st.plotly_chart(fig_monthly, use_container_width=True)

        st.subheader("Regional Performance Comparison")
        selected_countries = st.multiselect(
            "Filter Countries for Comparison",
            options=df_country['Country'].unique(),
            default=['United Kingdom', 'Netherlands', 'EIRE']
        )
        
        if selected_countries:
            df_filtered = df_country_monthly[df_country_monthly['Country'].isin(selected_countries)]
            fig_compare = px.line(
                df_filtered,
                x='Month',
                y='Revenue',
                color='Country',
                markers=True,
                template='plotly_white',
                line_shape='spline',
                color_discrete_sequence=px.colors.qualitative.Prism,
                title="Cross-Country Monthly Revenue Analysis"
            )
            fig_compare.update_layout(
                xaxis_title="Month",
                yaxis_title="Revenue (£)",
                font=dict(family="Inter, sans-serif")
            )
            st.plotly_chart(fig_compare, use_container_width=True)

    with tab3:
        st.subheader("Leading Performers per Period")
        st.dataframe(df_market_leaders.style.format({"Revenue": "£{:,.2f}"}), use_container_width=True)
        
        fig_leaders = px.pie(
            df_market_leaders, 
            names='Country', 
            values='Revenue',
            template='plotly_white',
            color_discrete_sequence=px.colors.sequential.Blues_r,
            title="Market Share Concentration of Monthly Leaders"
        )
        fig_leaders.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_leaders, use_container_width=True)

except Exception as e:
    st.error(f"Error loading system metrics: {e}")
    st.info("System Status: Awaiting Hadoop task results in the documentation directory.")

st.sidebar.markdown("---")
st.sidebar.markdown("**System Information**")
st.sidebar.caption("This interface visualizes the high-scale analytical output from a 5-stage Hadoop MapReduce pipeline. Designed for executive-level business intelligence.")
