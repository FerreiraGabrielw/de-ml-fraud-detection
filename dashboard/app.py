# ============================================================
# FRAUD DETECTION MONITORING PLATFORM
# FULL ENTERPRISE DASHBOARD
# ============================================================

# ============================================================
# IMPORTS
# ============================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from sqlalchemy import create_engine

import os

from dotenv import load_dotenv


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(

    page_title="Fraud Detection Monitoring Platform",

    page_icon="🚨",

    layout="wide"

)


# ============================================================
# DATABASE VARIABLES
# ============================================================

DB_HOST = os.getenv("RDS_HOST")
DB_PORT = os.getenv("RDS_PORT")
DB_NAME = os.getenv("RDS_DATABASE")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")


# ============================================================
# DATABASE CONNECTION
# ============================================================

@st.cache_resource
def get_engine():

    engine = create_engine(

        f"postgresql+psycopg2://{DB_USER}:"
        f"{DB_PASSWORD}@"
        f"{DB_HOST}:{DB_PORT}/"
        f"{DB_NAME}"

    )

    return engine


engine = get_engine()


# ============================================================
# LOAD DATA
# ============================================================

@st.cache_data(ttl=60)
def load_data():

    query = """

    SELECT *
    FROM fraud_predictions

    """

    df = pd.read_sql(
        query,
        engine
    )

    return df


df = load_data()


# ============================================================
# PAGE TITLE
# ============================================================

st.title("🚨 Fraud Detection Monitoring Platform")

st.markdown(
    """
    Real-time monitoring platform for fraud detection,
    machine learning inference and operational analytics.
    """
)


# ============================================================
# SIDEBAR FILTERS
# ============================================================

st.sidebar.header("Filters")


fraud_filter = st.sidebar.selectbox(

    "Fraud Prediction",

    ["All", "Fraud Only", "Non Fraud Only"]

)


if fraud_filter == "Fraud Only":

    df = df[
        df["fraud_prediction"] == 1
    ]

elif fraud_filter == "Non Fraud Only":

    df = df[
        df["fraud_prediction"] == 0
    ]


# ============================================================
# KPI CALCULATIONS
# ============================================================

total_transactions = len(df)

fraud_transactions = len(
    df[df["fraud_prediction"] == 1]
)

fraud_rate = (
    fraud_transactions / total_transactions
) * 100

fraud_amount = df[
    df["fraud_prediction"] == 1
]["amt"].sum()

avg_probability = df[
    "fraud_probability"
].mean()


# ============================================================
# KPI SECTION
# ============================================================

st.subheader("Executive Overview")


col1, col2, col3, col4, col5 = st.columns(5)


col1.metric(
    "Transactions",
    f"{total_transactions:,}"
)

col2.metric(
    "Frauds",
    f"{fraud_transactions:,}"
)

col3.metric(
    "Fraud Rate",
    f"{fraud_rate:.2f}%"
)

col4.metric(
    "Fraud Amount",
    f"${fraud_amount:,.2f}"
)

col5.metric(
    "Avg Fraud Score",
    f"{avg_probability:.4f}"
)


st.divider()


# ============================================================
# FRAUD DISTRIBUTION
# ============================================================

col_a, col_b = st.columns(2)


with col_a:

    fraud_dist = (

        df["fraud_prediction"]

        .value_counts()

        .reset_index()

    )

    fraud_dist.columns = [
        "Fraud",
        "Count"
    ]

    fraud_dist["Fraud"] = fraud_dist[
        "Fraud"
    ].replace({

        0: "Non Fraud",
        1: "Fraud"

    })


    fig_pie = px.pie(

        fraud_dist,

        names="Fraud",

        values="Count",

        title="Fraud Distribution",

        color_discrete_sequence=[
            "#4CAF50",
            "#F44336"
        ]

    )

    fig_pie.update_traces(
        textposition='inside',
        textinfo='percent+label'
    )

    st.plotly_chart(
        fig_pie,
        use_container_width=True
    )


with col_b:

    fig_hist = px.histogram(

        df,

        x="fraud_probability",

        nbins=40,

        title="Fraud Probability Distribution",

        color_discrete_sequence=[
            "#1E88E5"
        ]

    )

    fig_hist.update_layout(
        bargap=0.05
    )

    st.plotly_chart(
        fig_hist,
        use_container_width=True
    )


st.divider()


# ============================================================
# FRAUDS BY HOUR
# ============================================================

st.subheader("Fraud Activity by Hour")


fraud_hour = (

    df[df["fraud_prediction"] == 1]

    .groupby("transaction_hour")

    .size()

    .reset_index(name="count")

)


fig_hour = px.line(

    fraud_hour,

    x="transaction_hour",

    y="count",

    markers=True,

    title="Fraud Transactions by Hour"

)

fig_hour.update_traces(
    line=dict(width=3)
)

st.plotly_chart(
    fig_hour,
    use_container_width=True
)


st.divider()


# ============================================================
# HIGH AMOUNT VS NIGHT FRAUD
# ============================================================

col_c, col_d = st.columns(2)


with col_c:

    high_amount = (

        df.groupby("high_amount_flag")

        .size()

        .reset_index(name="count")

    )

    high_amount["high_amount_flag"] = high_amount[
        "high_amount_flag"
    ].replace({

        0: "Normal Amount",
        1: "High Amount"

    })


    fig_high = px.bar(

        high_amount,

        x="high_amount_flag",

        y="count",

        title="High Amount Transactions",

        color="high_amount_flag",

        color_discrete_sequence=[
            "#42A5F5",
            "#EF5350"
        ]

    )

    st.plotly_chart(
        fig_high,
        use_container_width=True
    )


with col_d:

    night_fraud = (

        df.groupby("night_transaction_flag")

        .size()

        .reset_index(name="count")

    )

    night_fraud["night_transaction_flag"] = night_fraud[
        "night_transaction_flag"
    ].replace({

        0: "Day",
        1: "Night"

    })


    fig_night = px.bar(

        night_fraud,

        x="night_transaction_flag",

        y="count",

        title="Night Transactions",

        color="night_transaction_flag",

        color_discrete_sequence=[
            "#64B5F6",
            "#283593"
        ]

    )

    st.plotly_chart(
        fig_night,
        use_container_width=True
    )


st.divider()


# ============================================================
# GEO FRAUD MAP
# ============================================================

st.subheader("Fraud Geographic Monitoring")


fraud_map = df[
    df["fraud_prediction"] == 1
].copy()


# ============================================================
# DATA CLEANING
# ============================================================

fraud_map = fraud_map[
    (fraud_map["fraud_probability"] < 0.90)
    &
    (fraud_map["amt"] < 5000)
]

fraud_map = fraud_map.dropna(
    subset=["lat", "long"]
)


# ============================================================
# OPTIONAL PERFORMANCE SAMPLE
# ============================================================

if len(fraud_map) > 3000:

    fraud_map = fraud_map.sample(
        3000,
        random_state=42
    )


# ============================================================
# MAP CREATION
# ============================================================

fig_map = px.scatter_mapbox(

    fraud_map,

    lat="lat",

    lon="long",

    color="fraud_probability",

    size="amt",

    hover_data={

        "amt": ":.2f",

        "customer_age": True,

        "transaction_hour": True,

        "customer_merchant_distance": ":.2f",

        "fraud_probability": ":.3f",

        "lat": False,

        "long": False

    },

    color_continuous_scale="Blues",

    opacity=0.55,

    size_max=12,

    zoom=3,

    height=700,

    title="Fraud Transactions Geospatial Distribution",

    mapbox_style="carto-positron"

)


# ============================================================
# MAP LAYOUT
# ============================================================

fig_map.update_layout(

    paper_bgcolor="#0E1117",

    plot_bgcolor="#0E1117",

    font=dict(

        color="white",

        size=12

    ),

    margin=dict(

        l=0,
        r=0,
        t=50,
        b=0

    ),

    title_x=0.5,

    coloraxis_colorbar=dict(
        title="Fraud<br>Probability"
    )

)


st.plotly_chart(
    fig_map,
    use_container_width=True
)


st.divider()


# ============================================================
# TOP HIGH RISK TRANSACTIONS
# ============================================================

st.subheader("Top High Risk Transactions")


top_risk = (

    df.sort_values(

        "fraud_probability",

        ascending=False

    )

    [[

        "trans_num",

        "amt",

        "customer_age",

        "transaction_hour",

        "customer_merchant_distance",

        "fraud_probability",

        "fraud_prediction"

    ]]

    .head(20)

)


st.dataframe(

    top_risk,

    use_container_width=True,

    hide_index=True

)


st.divider()


# ============================================================
# DISTANCE ANALYSIS
# ============================================================

st.subheader("Customer vs Merchant Distance")


fig_distance = px.histogram(

    df,

    x="customer_merchant_distance",

    nbins=50,

    title="Transaction Distance Distribution",

    color_discrete_sequence=["#00ACC1"]

)

st.plotly_chart(
    fig_distance,
    use_container_width=True
)


st.divider()


# ============================================================
# PIPELINE MONITORING
# ============================================================

st.subheader("Pipeline Monitoring")


pipe1, pipe2, pipe3 = st.columns(3)


pipe1.metric(
    "Rows Processed",
    f"{len(df):,}"
)

pipe2.metric(
    "Fraud Alerts",
    f"{fraud_transactions:,}"
)

pipe3.metric(

    "Last Inference",

    pd.to_datetime(
        df["inference_timestamp"].max()
    ).strftime("%Y-%m-%d %H:%M")

)


# ============================================================
# RAW DATA
# ============================================================

with st.expander("View Raw Data"):

    st.dataframe(
        df.head(100),
        use_container_width=True
    )