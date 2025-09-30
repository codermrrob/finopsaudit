"""
Streamlit page for the Inconsistency Deep Dive.
"""

import streamlit as st
import plotly.express as px
from streamlit_agraph import agraph, Node, Edge, Config

import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Correctly import from the parent directory's utils module
from AuditDashboard.utils.data_loader import load_entity_analysis_data
from Configuration import SimilarityConfig, AuditDashboardConfig

# Initialize config

st.set_page_config(layout="wide", page_title=AuditDashboardConfig.DEEP_DIVE_PAGE_TITLE)

st.title(f"🔬 {AuditDashboardConfig.DEEP_DIVE_PAGE_TITLE}")
st.markdown("An interactive workbench for exploring inconsistent entity groups.")

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

# --- Treemap ---
st.header("Most Inconsistent Entities by Cost")
st.markdown("This treemap shows the entity groups with the highest associated cloud spend. The size of the rectangle is proportional to the total cost of all resources in that group.")

# First, filter for only the inconsistent groups, then sort by cost and take the top N
group_cost_df = cost_df[cost_df['num_members'] > 0].sort_values(by='cost', ascending=False).head(AuditDashboardConfig.TREEMAP_TOP_N)

if not group_cost_df.empty:
    fig_treemap = px.treemap(
        group_cost_df,
        path=[px.Constant("All Inconsistent Groups"), 'entity_name'],
        values='cost',
        color='num_members',
        color_continuous_scale='Reds',
        hover_data={'cost': ':.2f', 'num_members': True},
        title="Top Inconsistent Entity Groups by Associated Cost"
    )
    fig_treemap.update_layout(margin = dict(t=50, l=25, r=25, b=25))
    st.plotly_chart(fig_treemap, use_container_width=True)
else:
    st.info("No inconsistent groups with associated costs were found.")

# --- Interactive Graph ---
st.header("Interactive Entity Relationship Graph")
st.markdown("Explore the relationships within the discovered entity groups. Canonical entities are larger and blue.")

nodes = []
edges = []
seen_nodes = set()

groups = entity_data.get('groups', [])
if groups:
    for group in groups:
        canonical_name = group[SimilarityConfig.CANONICAL_KEY]
        if canonical_name not in seen_nodes:
            nodes.append(Node(id=canonical_name, label=canonical_name, size=25, color="#007bff"))
            seen_nodes.add(canonical_name)

        for member in group['members']:
            member_name = member[SimilarityConfig.ENTITY_NAME_KEY]
            if member_name not in seen_nodes:
                nodes.append(Node(id=member_name, label=member_name, size=15))
                seen_nodes.add(member_name)
            
            edges.append(Edge(
                source=canonical_name, 
                target=member_name, 
                label=f"{member['similarity']:.2f}",
                type="CURVE_SMOOTH",
                color="#ff4b4b" if member['link_type'] == 'duplicate' else "#a9a9a9",
                strokeWidth=3 if member['link_type'] == 'duplicate' else 1
            ))

    agraph_config = Config(
        width=1000,
        height=600,
        directed=False, 
        physics=True, 
        hierarchical=False,
    )

    agraph(nodes=nodes, edges=edges, config=agraph_config)
else:
    st.info("No groups to visualize.")
