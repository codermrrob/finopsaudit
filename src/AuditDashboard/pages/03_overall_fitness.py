import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from Configuration import AuditReportColumns

st.set_page_config(page_title="Overall Corpus Fitness", layout="wide")

st.title("Overall Corpus Fitness")
st.write("This page provides high-level visualizations of the entire resource corpus from the initial audit phase (Phase 1).")
# --- Load Data ---
@st.cache_data
def load_data(output_base, tenant, year, month):
    # Note: This page uses the Phase 1 output file
    data_path = Path(output_base) / tenant / "audit"
    file_path = data_path / f"audit_readiness.{year}_{month}.p1.csv"
    if not file_path.exists():
        return None
    df = pd.read_csv(file_path)
    return df
    st.warning("Please launch the dashboard via the main app.py with the correct CLI arguments.")
    st.stop()

df = load_data(
    st.session_state['output_base'],
    st.session_state['tenant'],
    st.session_state['year'],
    st.session_state['month']
)

if df is None:
    st.error(f"Could not find `audit_readiness.p1.{st.session_state['year']}_{st.session_state['month']}.csv`. Please ensure the Phase 1 audit file exists.")
    st.stop()

# --- Distribution Plots ---
st.header("Distribution of Key Name Metrics")
col1, col2 = st.columns(2)

with col1:
    st.subheader("Distribution of 'Percent Removed'")
    fig1 = px.histogram(df, x=AuditReportColumns.PCT_REMOVED, nbins=50, title="Explainability of Names (Higher is Better)")
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("Distribution of 'Residual Length'")
    fig2 = px.histogram(df, x=AuditReportColumns.RESIDUAL_LEN, nbins=50, title="Length of Unexplained Part of Name")
    st.plotly_chart(fig2, use_container_width=True)

# --- 2D Density Plot ---
st.header("Correlation between Explainability and Residual Length")
fig_density = px.density_heatmap(
    df,
    x=AuditReportColumns.PCT_REMOVED,
    y=AuditReportColumns.RESIDUAL_LEN,
    marginal_x="histogram",
    marginal_y="histogram",
    title="Density of Resources by Pct Removed vs. Residual Length"
)
st.plotly_chart(fig_density, use_container_width=True)

# --- Violin Plots ---
st.header("Flag Analysis with Violin Plots")

col3, col4 = st.columns(2)

with col3:
    cat_flag = st.selectbox(
        "Select Categorical Flag",
        options=[
            AuditReportColumns.IS_GLUED, 
            AuditReportColumns.OVERSTRIP_FLAG, 
            AuditReportColumns.ACRONYM_ONLY_RESIDUAL, 
            AuditReportColumns.HEAVY_SCAFFOLD, 
            AuditReportColumns.ENV_CONFLICT
        ],
        index=0
    )

with col4:
    cont_metric = st.selectbox(
        "Select Continuous Metric",
        options=[
            AuditReportColumns.ENTROPY_ORIG, 
            AuditReportColumns.PCT_REMOVED, 
            AuditReportColumns.RESIDUAL_LEN, 
            AuditReportColumns.MASK_HITS_TOTAL
        ],
        index=0
    )

fig_violin = px.violin(
    df,
    x=cat_flag,
    y=cont_metric,
    box=True,
    points="all", # Set to 'all' for smaller datasets if needed
    title=f"Distribution of '{cont_metric}' by '{cat_flag}' Status"
)
st.plotly_chart(fig_violin, use_container_width=True)
