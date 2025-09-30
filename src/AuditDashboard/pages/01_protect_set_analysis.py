import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from Configuration import AuditConfig, ProtectSetColumns

st.set_page_config(page_title="Protect Set Analysis", layout="wide")

st.title("Protect Set Analysis")
st.write("This page provides an interactive visualization of the `protect_set_combined` data, helping to identify high-priority entities based on frequency, cost, and spread.")
# --- Load Data ---
@st.cache_data
def load_data(output_base, tenant, year, month):
    data_path = Path(output_base) / tenant / "audit"
    file_path = data_path / f"protect_set_combined.{year}_{month}.p2.csv"
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
    st.error(f"Could not find `protect_set_combined.{year}_{month}.p2.csv`. Please ensure the file exists.")
    st.stop()

# --- Bubble Scatter Plot ---
st.header("Interactive Bubble Scatter Plot")
st.write("Analyze tokens by frequency (Support), cost, and spread. Use the legend to filter by category.")

col1, _ = st.columns([1, 3])
with col1:
    size_level = st.slider(
        "Bubble Size",
        min_value=0,
        max_value=2,
        value=0,
        format="%d",
        help="Increase bubble size. 0=Normal, 1=Large, 2=Largest."
    )


# Map slider level to multiplier
# Map slider level to a more impactful multiplier
# Map slider level to a more impactful multiplier
size_multipliers = {0: 1, 1: 5, 2: 10}
size_multiplier = size_multipliers[size_level]

# Create a color mapping column
def assign_color(row):
    if row[ProtectSetColumns.IN_FREQUENCY_SET] and row[ProtectSetColumns.IN_COST_SET]:
        return "Both (Freq & Cost)"
    elif row[ProtectSetColumns.IN_FREQUENCY_SET]:
        return "Frequency Only"
    elif row[ProtectSetColumns.IN_COST_SET]:
        return "Cost Only"
    return "Other"

df['category'] = df.apply(assign_color, axis=1)

# Handle potential zero values for log scale
df['log_support'] = pd.to_numeric(df[ProtectSetColumns.SUPPORT_NAMES], errors='coerce').fillna(0)
df['log_cost'] = pd.to_numeric(df[ProtectSetColumns.TOTAL_COST], errors='coerce').fillna(0)

# Add a small constant to avoid log(0)
df['log_support'] = df['log_support'].apply(lambda x: x if x > 0 else 1)
df['log_cost'] = df['log_cost'].apply(lambda x: x if x > 0 else 0.01) # Use a small value for cost
df['scaled_spread'] = df[ProtectSetColumns.SPREAD_SUBS] * size_multiplier

fig = px.scatter(
    df,
    size_max=60, # Set a fixed max size for consistent scaling
    x="log_support",
    y="log_cost",
    size="scaled_spread",
    color="category",
    hover_name=ProtectSetColumns.CHUNK,
    hover_data={
        ProtectSetColumns.SUPPORT_NAMES: True,
        ProtectSetColumns.TOTAL_COST: ':.2f',
        ProtectSetColumns.SPREAD_SUBS: True,
        'log_support': False, # Hide the log value
        'log_cost': False # Hide the log value
    },
    log_x=True,
    log_y=True,
    title="Protect Set Entities: Frequency vs. Cost vs. Spread",
    labels={
        "log_support": "Support (Frequency) - Log Scale",
        "log_cost": "Total Cost - Log Scale",
        "scaled_spread": "Subscription Spread",
        "category": "Category"
    },
    color_discrete_map={
        "Both (Freq & Cost)": "green",
        "Frequency Only": "blue",
        "Cost Only": "red",
        "Other": "grey"
    })

fig.update_layout(height=700)
st.plotly_chart(fig, use_container_width=True)

# --- Data Table ---
st.header("Detailed Data")
st.dataframe(df)
