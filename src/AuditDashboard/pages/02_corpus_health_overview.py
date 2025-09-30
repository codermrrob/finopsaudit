import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from Configuration import AuditConfig, ResourcesPerDayJsonColumns, CorpusRollupColumns

st.set_page_config(page_title="Corpus Health Overview", layout="wide")

st.title("Corpus Health Overview")
st.write("This page provides comparative views of naming hygiene across different scopes (e.g., Subscriptions or Resource Groups).")
# --- Load Data ---
@st.cache_data
def load_data(output_base, tenant, year, month):
    data_path = Path(output_base) / tenant / "audit"
    file_path = data_path / f"corpus_readiness_summary.{year}_{month}.p2.csv"
    if not file_path.exists():
        return None
    df = pd.read_csv(file_path)
    return df

if 'output_base' not in st.session_state:
    st.warning("Please launch the dashboard via the main app.py with the correct CLI arguments.")
    st.stop()

df = load_data(
    st.session_state['output_base'],
    st.session_state['tenant'],
    st.session_state['year'],
    st.session_state['month']
)

if df is None:
    st.error(f"Could not find `corpus_readiness_summary.{year}_{month}.p2.csv`. Please ensure the file exists.")
# --- Controls ---
scope = st.selectbox(
    "Select Scope for Analysis",
    options=[ResourcesPerDayJsonColumns.SUB_ACCOUNT_NAME, ResourcesPerDayJsonColumns.RESOURCE_GROUP]
)

# Defensively handle older data formats that may be missing the 'scope_type' column.
if CorpusRollupColumns.SCOPE_TYPE in df.columns:
    scope_df = df[df[CorpusRollupColumns.SCOPE_TYPE] == scope].copy()
else:
    # Fallback for older files: assume rows are relevant if the scope value is present.
    st.warning(f"Warning: The data file is from an older run and is missing the '{CorpusRollupColumns.SCOPE_TYPE}' column. Filtering may be incomplete.")
    scope_df = df[df[CorpusRollupColumns.SCOPE_VALUE].notna()].copy()

# --- Bar Charts for Hygiene Scores ---

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Highest Overstrip Rate")
    overstrip_df = scope_df.nlargest(15, CorpusRollupColumns.OVERSTRIP_FLAG_RATE)
    fig1 = px.bar(overstrip_df, x=CorpusRollupColumns.OVERSTRIP_FLAG_RATE, y=CorpusRollupColumns.SCOPE_VALUE, orientation='h', title="Overstrip Flag Rate (Higher is Worse)")
    fig1.update_yaxes(categoryorder="total ascending")
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("Lowest Glued Explained Rate")
    glued_df = scope_df[scope_df[CorpusRollupColumns.GLUED_EXPLAINED_RATE].notna()].nsmallest(15, CorpusRollupColumns.GLUED_EXPLAINED_RATE)
    fig2 = px.bar(glued_df, x=CorpusRollupColumns.GLUED_EXPLAINED_RATE, y=CorpusRollupColumns.SCOPE_VALUE, orientation='h', title="Glued Explained Rate (Lower is Worse)")
    fig2.update_yaxes(categoryorder="total descending")
    st.plotly_chart(fig2, use_container_width=True)

with col3:
    st.subheader("Highest Glued Name Rate")
    is_glued_df = scope_df.nlargest(15, CorpusRollupColumns.IS_GLUED_RATE)
    fig3 = px.bar(is_glued_df, x=CorpusRollupColumns.IS_GLUED_RATE, y=CorpusRollupColumns.SCOPE_VALUE, orientation='h', title="Is Glued Rate (Higher is Worse)")
    fig3.update_yaxes(categoryorder="total ascending")
    st.plotly_chart(fig3, use_container_width=True)

# --- Heatmap for Multi-Metric Health Check ---
st.header(f"Multi-Metric Health Check for Top 20 {scope.replace('_', ' ').title()}")

heatmap_df = scope_df.nlargest(20, CorpusRollupColumns.N_RESOURCES)
heatmap_metrics = [
    CorpusRollupColumns.OVERSTRIP_FLAG_RATE, 
    CorpusRollupColumns.GLUED_EXPLAINED_RATE, 
    CorpusRollupColumns.IS_GLUED_RATE, 
    CorpusRollupColumns.PCT_REMOVED_MEAN, 
    CorpusRollupColumns.ENTROPY_RESID_MEAN
]

fig_heatmap = px.imshow(
    heatmap_df[heatmap_metrics],
    y=heatmap_df[CorpusRollupColumns.SCOPE_VALUE],
    x=heatmap_metrics,
    color_continuous_scale='RdYlGn_r', # Red-Yellow-Green, reversed so green is high
    title=f"Health Metrics for Top 20 Scopes by Resource Count"
)
fig_heatmap.update_layout(height=800)
st.plotly_chart(fig_heatmap, use_container_width=True)
