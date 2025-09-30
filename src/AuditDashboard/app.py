import streamlit as st
import typer
from pathlib import Path

def main(
    output_base: str = typer.Option(..., "--output-base", help="The base directory where tenant data is stored."),
    tenant: str = typer.Option(..., "--tenant", help="The name of the tenant."),
    year: str = typer.Option(..., "--year", help="The audit year."),
    month: str = typer.Option(..., "--month", help="The audit month.")
):
    """
    Launches the Audit Readiness Dashboard for a specific tenant and period.
    """
    st.set_page_config(
        page_title="Audit Readiness Dashboard",
        layout="wide"
    )

    # --- Store CLI arguments in session state for other pages to use ---
    st.session_state['output_base'] = output_base
    st.session_state['tenant'] = tenant
    st.session_state['year'] = year
    st.session_state['month'] = month

    # --- Main App --- 
    st.title("Audit Dashboard")
    st.info(f"Displaying data for Tenant: **{tenant}**, Period: **{year}-{month}**")
    st.write("This dashboard provides visualizations for the outputs of the Audit Readiness process. Use the sidebar to navigate between different analysis pages.")

    st.sidebar.success("Analysis loaded successfully.")
    st.sidebar.markdown("---")
    st.sidebar.header("Selected Context")
    st.sidebar.markdown(f"**Tenant:** `{tenant}`")
    st.sidebar.markdown(f"**Period:** `{year}-{month}`")
    st.sidebar.markdown(f"**Data Path:** `{Path(output_base).resolve()}`")


if __name__ == "__main__":
    # To run this app, use the command:
    # streamlit run src/AuditDashboard/app.py -- --output-base /path/to/output --tenant your_tenant --year 2024 --month 6
    typer.run(main)