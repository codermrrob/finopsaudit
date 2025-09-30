"""
Streamlit page for the Entity Analysis Executive Summary.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Correctly import from the parent directory's utils module
from AuditDashboard.utils.data_loader import load_entity_analysis_data
from Configuration import SimilarityConfig, AuditDashboardConfig

# Initialize config

st.set_page_config(layout="wide", page_title=AuditDashboardConfig.SUMMARY_PAGE_TITLE)

st.title(f"📊 {AuditDashboardConfig.SUMMARY_PAGE_TITLE}")
st.markdown("A high-level summary of naming consistency and its financial impact.")

# --- Load Data ---
if 'tenant' not in st.session_state:
    st.warning("Please launch the dashboard via the main app.py with the correct CLI arguments.")
    st.stop()

data = load_entity_analysis_data(
    st.session_state['tenant'], 
    st.session_state['year'], 
    st.session_state['month'], 
    st.session_state['output_base']
)

if not data:
    st.stop()

entity_data = data['entity_data']
cost_df = data['cost_df']

# --- Scorecard Metrics ---
st.header("Naming Hygiene Scorecard")

num_groups = len(entity_data.get('groups', []))
num_distinct = len(entity_data.get(SimilarityConfig.DISTINCT_ENTITIES_KEY, []))
total_entities = num_groups + num_distinct
num_members = sum(len(g.get('members', [])) for g in entity_data.get('groups', []))

# A more accurate ratio: (entities inside groups) / (total entities)
consolidation_ratio = (num_members + num_groups) / total_entities if total_entities > 0 else 0

# Calculate duplication vs. variation
link_types = [member['link_type'] for group in entity_data.get('groups', []) for member in group['members']]
duplicate_count = link_types.count('duplicate')
related_count = link_types.count('related')

# Calculate 'at-risk' cost
at_risk_cost = cost_df[cost_df['num_members'] > 0]['cost'].sum()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Consolidation Ratio", f"{consolidation_ratio / 100:.1%}", help="The percentage of your entity vocabulary that is redundant or inconsistent. Higher is worse.")
with col2:
    st.metric("Total 'At-Risk' Cost", f"${at_risk_cost:,.0f}", help="The total cost of resources associated with inconsistent entity groups.")

# --- Top N Coverage ---
with col3:
    top_n = st.slider(
        "Top N Entities", 
        min_value=5, 
        max_value=min(AuditDashboardConfig.COST_COVERAGE_SLIDER_MAX, len(cost_df)), 
        value=min(AuditDashboardConfig.COST_COVERAGE_SLIDER_DEFAULT, len(cost_df)), 
        step=5
    )
with col4:
    top_n_coverage = cost_df.head(top_n)['cost_pct_of_total'].sum()
    st.metric(f"Top {top_n} Cost Coverage", f"{top_n_coverage / 100:.1%}", help=f"The percentage of total cloud spend covered by the top {top_n} most costly suggested entities.")

# --- Duplication vs. Variation Chart ---
st.subheader("Breakdown of Inconsistencies")
if duplicate_count > 0 or related_count > 0:
    fig_pie = px.pie(
        names=['Duplicates (Typos)', 'Related (Variations)'], 
        values=[duplicate_count, related_count], 
        title='Nature of Inconsistent Groups',
        hole=0.3
    )
    st.plotly_chart(fig_pie, use_container_width=True)
else:
    st.info("No inconsistent groups found.")

# --- Cost Coverage Chart ---
st.header("Cost Coverage of Suggested Entities")
st.markdown("This chart shows how much of your total cloud spend can be explained by the suggested entities, ranked by cost.")

fig = go.Figure()

# Bar chart for individual cost
fig.add_trace(go.Bar(
    x=cost_df['entity_name'],
    y=cost_df['cost'],
    name='Monthly Cost',
    marker_color='blue'
))

# Line chart for cumulative percentage
fig.add_trace(go.Scatter(
    x=cost_df['entity_name'],
    y=cost_df['cumulative_cost_pct'],
    name='Cumulative Coverage %',
    yaxis='y2',
    mode='lines+markers',
    line=dict(color='red')
))

fig.update_layout(
    title='Cost Coverage by Suggested Entity',
    xaxis_title='Suggested Entity',
    yaxis=dict(
        title='Monthly Cost ($)',
        title_font=dict(color='blue'),
        tickfont=dict(color='blue')
    ),
    yaxis2=dict(
        title='Cumulative Coverage (%)',
        title_font=dict(color='red'),
        tickfont=dict(color='red'),
        overlaying='y',
        side='right',
        range=[0, 100]
    ),
    legend=dict(x=0.1, y=0.9)
)

st.plotly_chart(fig, use_container_width=True)
