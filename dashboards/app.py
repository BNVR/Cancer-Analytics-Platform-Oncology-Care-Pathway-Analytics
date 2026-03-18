from __future__ import annotations

import html

import pandas as pd
import plotly.express as px
import streamlit as st
from pandas.tseries.offsets import DateOffset

from data_access import load_dashboard_bundle


st.set_page_config(
    page_title="Cancer Analytics Platform",
    layout="wide",
)


st.markdown(
    """
    <style>
    :root {
        --cap-sidebar-bg: linear-gradient(180deg, #e5f2ff 0%, #a8c9e8 48%, #5f8fbd 100%);
        --cap-main-bg: radial-gradient(circle at top, #153f7a 0%, #0b2142 48%, #061327 100%);
        --cap-panel-bg: rgba(255, 255, 255, 0.08);
        --cap-panel-border: rgba(181, 219, 255, 0.18);
        --cap-text: #eef6ff;
        --cap-text-muted: #b7cae6;
        --cap-accent: #62b7ff;
        --cap-accent-strong: #1f8fff;
    }

    [data-testid="stAppViewContainer"] {
        background: var(--cap-main-bg);
    }

    [data-testid="stSidebar"] {
        background: var(--cap-sidebar-bg);
        border-right: 1px solid rgba(25, 88, 150, 0.18);
    }

    [data-testid="stSidebarCollapsedControl"] button,
    [data-testid="stSidebarCollapsedControl"] svg,
    [data-testid="stSidebarCollapsedControl"] path,
    [data-testid="collapsedControl"] button,
    [data-testid="collapsedControl"] svg,
    [data-testid="collapsedControl"] path,
    button[kind="header"] svg,
    button[kind="header"] path {
        color: #f4fbff !important;
        fill: #f4fbff !important;
        stroke: #f4fbff !important;
        opacity: 1 !important;
    }

    [data-testid="stSidebar"] * {
        color: #12385e;
    }

    [data-testid="stSidebar"] [data-baseweb="select"],
    [data-testid="stSidebar"] [data-baseweb="input"],
    [data-testid="stSidebar"] .stDateInput > div > div {
        background: rgba(255, 255, 255, 0.9);
        border-radius: 14px;
    }

    [data-testid="stSidebar"] [data-baseweb="select"] span,
    [data-testid="stSidebar"] [data-baseweb="select"] div,
    [data-testid="stSidebar"] [data-baseweb="input"] input,
    [data-testid="stSidebar"] .stDateInput input {
        color: #12385e !important;
        -webkit-text-fill-color: #12385e !important;
    }

    [data-baseweb="popover"] {
        background: #f7fbff !important;
    }

    [data-baseweb="popover"] *,
    [data-baseweb="popover"] [role="option"],
    [data-baseweb="popover"] [role="listbox"],
    [data-baseweb="popover"] li,
    [data-baseweb="popover"] span,
    [data-baseweb="popover"] div {
        color: #12385e !important;
        -webkit-text-fill-color: #12385e !important;
    }

    [data-baseweb="popover"] [aria-selected="true"] {
        background: rgba(98, 183, 255, 0.18) !important;
    }

    [data-testid="stHeader"] {
        background: transparent;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    h1, h2, h3, h4, h5, h6, p, label, span, div {
        color: var(--cap-text);
    }

    [data-testid="stCaptionContainer"] p,
    .stMarkdown p {
        color: var(--cap-text-muted);
    }

    [data-testid="stMetric"] {
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.12), rgba(255, 255, 255, 0.06));
        border: 1px solid var(--cap-panel-border);
        border-radius: 20px;
        padding: 1rem 1.2rem;
        box-shadow: 0 18px 40px rgba(4, 15, 33, 0.28);
        backdrop-filter: blur(10px);
    }

    [data-testid="stMetricLabel"] p {
        color: #c8dcf7;
        font-weight: 600;
        letter-spacing: 0.02em;
    }

    [data-testid="stMetricValue"] {
        color: #ffffff;
        font-size: 2rem !important;
        line-height: 1.05 !important;
    }

    [data-baseweb="tab-list"] {
        gap: 0.5rem;
    }

    button[data-baseweb="tab"] {
        background: rgba(255, 255, 255, 0.06);
        border-radius: 999px;
        padding: 0.45rem 1rem;
    }

    button[data-baseweb="tab"][aria-selected="true"] {
        background: linear-gradient(135deg, var(--cap-accent-strong), var(--cap-accent));
    }

    button[data-baseweb="tab"]::after {
        display: none !important;
        background: transparent !important;
        border-bottom: none !important;
        box-shadow: none !important;
    }

    [data-baseweb="tab-highlight"] {
        display: none !important;
        background: transparent !important;
    }

    .stTabs [data-baseweb="tab-border"] {
        display: none !important;
    }

    [data-testid="stPlotlyChart"],
    [data-testid="stDataFrame"],
    .cap-table-shell {
        background: var(--cap-panel-bg);
        border: 1px solid var(--cap-panel-border);
        border-radius: 22px;
        padding: 0.5rem;
        box-shadow: 0 18px 40px rgba(4, 15, 33, 0.22);
        backdrop-filter: blur(8px);
    }

    [data-testid="stDataFrame"] [data-testid="stDataFrameResizable"],
    [data-testid="stDataFrame"] [role="grid"] {
        background: rgba(12, 32, 62, 0.92) !important;
        color: #eef6ff !important;
    }

    [data-testid="stDataFrame"] [role="columnheader"],
    [data-testid="stDataFrame"] [role="gridcell"],
    [data-testid="stDataFrame"] div[data-stale="false"],
    [data-testid="stDataFrame"] .glideDataEditor,
    [data-testid="stDataFrame"] .gdg-cell,
    [data-testid="stDataFrame"] .gdg-header {
        background: rgba(12, 32, 62, 0.92) !important;
        color: #bfe4ff !important;
        border-color: rgba(160, 203, 244, 0.14) !important;
    }

    [data-testid="stDataFrame"] [role="columnheader"] {
        background: rgba(29, 67, 116, 0.96) !important;
        color: #ffffff !important;
        font-weight: 700 !important;
    }

    [data-testid="stDataFrame"] canvas {
        filter: hue-rotate(0deg) saturate(1.1);
    }

    .cap-table-shell {
        padding: 0.9rem;
        overflow-x: auto;
        width: 100%;
        box-sizing: border-box;
    }

    .cap-table {
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
        background: rgba(12, 32, 62, 0.92);
        color: #bfe4ff;
        border-radius: 18px;
        overflow: hidden;
        font-size: 0.95rem;
    }

    .cap-table thead th {
        background: rgba(29, 67, 116, 0.96);
        color: #ffffff;
        text-align: left;
        padding: 0.85rem 0.9rem;
        font-weight: 700;
        border-bottom: 1px solid rgba(160, 203, 244, 0.18);
        white-space: normal;
        word-break: break-word;
    }

    .cap-table tbody td {
        padding: 0.8rem 0.9rem;
        border-bottom: 1px solid rgba(160, 203, 244, 0.12);
        color: #bfe4ff;
        white-space: normal;
        word-break: break-word;
        overflow-wrap: anywhere;
        vertical-align: top;
    }

    .cap-table tbody tr:nth-child(even) td {
        background: rgba(19, 44, 82, 0.92);
    }

    .cap-table tbody tr:nth-child(odd) td {
        background: rgba(12, 32, 62, 0.92);
    }

    .cap-table.compact {
        font-size: 0.82rem;
    }

    .cap-table.compact thead th {
        padding: 0.55rem 0.65rem;
    }

    .cap-table.compact tbody td {
        padding: 0.45rem 0.65rem;
    }

    .stTabs [data-baseweb="tab-panel"] {
        padding-top: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def normalise_frame_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.copy()
    renamed.columns = [
        column.strip().lower().replace(" ", "_").replace("-", "_")
        for column in renamed.columns
    ]
    return renamed


def style_figure(fig, *, discrete_sequence: list[str] | None = None):
    if discrete_sequence is not None:
        fig.update_traces(marker=dict(line=dict(width=0)))
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(9,24,49,0.35)",
        font=dict(color="#eef6ff"),
        title=dict(font=dict(size=22, color="#ffffff")),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor="rgba(255,255,255,0)",
            font=dict(color="#d7e7fb"),
        ),
        margin=dict(l=24, r=24, t=60, b=24),
    )
    fig.update_xaxes(
        gridcolor="rgba(150,190,235,0.15)",
        zerolinecolor="rgba(150,190,235,0.15)",
        linecolor="rgba(150,190,235,0.18)",
    )
    fig.update_yaxes(
        gridcolor="rgba(150,190,235,0.15)",
        zerolinecolor="rgba(150,190,235,0.15)",
        linecolor="rgba(150,190,235,0.18)",
    )
    return fig


def render_styled_table(df: pd.DataFrame, *, max_rows: int = 25, compact: bool = False):
    preview_df = df.head(max_rows).copy()
    preview_df.columns = [column.replace("_", " ").title() for column in preview_df.columns]

    header_html = "".join(f"<th>{html.escape(str(column))}</th>" for column in preview_df.columns)
    body_rows = []
    for _, row in preview_df.iterrows():
        cells = "".join(f"<td>{html.escape(str(value))}</td>" for value in row.tolist())
        body_rows.append(f"<tr>{cells}</tr>")

    st.markdown(
        f"""
        <div class="cap-table-shell">
            <table class="cap-table{' compact' if compact else ''}">
                <thead><tr>{header_html}</tr></thead>
                <tbody>{''.join(body_rows)}</tbody>
            </table>
        </div>
        """,
        unsafe_allow_html=True,
    )


bundle, source = load_dashboard_bundle()
overview_df = normalise_frame_columns(bundle.encounter_overview)
financial_df = normalise_frame_columns(bundle.financial_analytics)
provider_df = normalise_frame_columns(bundle.provider_performance)
audit_df = normalise_frame_columns(bundle.audit_log)

if "encounter_date" in overview_df.columns:
    overview_df["encounter_date"] = pd.to_datetime(overview_df["encounter_date"])
elif "date" in overview_df.columns:
    overview_df["encounter_date"] = pd.to_datetime(overview_df["date"])

if "billing_amount" not in overview_df.columns and "amount" in overview_df.columns:
    overview_df["billing_amount"] = overview_df["amount"]

if "patient_name" not in overview_df.columns and "name" in overview_df.columns:
    overview_df["patient_name"] = overview_df["name"]

st.title("Cancer Analytics Platform")
st.caption(f"Data source: {source}")

min_date = overview_df["encounter_date"].min().date()
max_date = overview_df["encounter_date"].max().date()
date_preset_key = "encounter_date_preset"
date_start_key = "encounter_start_date_v1"
date_end_key = "encounter_end_date_v1"


def clamp_date_range(value: object, lower: object, upper: object) -> tuple[object, object]:
    if isinstance(value, tuple) and len(value) == 2:
        start_date, end_date = value
    elif isinstance(value, list) and len(value) == 2:
        start_date, end_date = value
    else:
        start_date, end_date = lower, upper

    start_date = max(lower, min(start_date, upper))
    end_date = max(start_date, min(end_date, upper))
    return start_date, end_date


def preset_to_range(preset: str, lower: object, upper: object) -> tuple[object, object]:
    upper_ts = pd.Timestamp(upper)
    if preset == "Past Week":
        lower_ts = upper_ts - DateOffset(days=7)
    elif preset == "Past Month":
        lower_ts = upper_ts - DateOffset(months=1)
    elif preset == "Past 3 Months":
        lower_ts = upper_ts - DateOffset(months=3)
    elif preset == "Past 6 Months":
        lower_ts = upper_ts - DateOffset(months=6)
    elif preset == "Past Year":
        lower_ts = upper_ts - DateOffset(years=1)
    elif preset == "Past 2 Years":
        lower_ts = upper_ts - DateOffset(years=2)
    else:
        lower_ts = pd.Timestamp(lower)
    return clamp_date_range((lower_ts.date(), upper), lower, upper)


st.session_state.setdefault(date_preset_key, "All Data")
default_start, default_end = preset_to_range(st.session_state[date_preset_key], min_date, max_date)
st.session_state[date_start_key], st.session_state[date_end_key] = clamp_date_range(
    (
        st.session_state.get(date_start_key, default_start),
        st.session_state.get(date_end_key, default_end),
    ),
    min_date,
    max_date,
)

with st.sidebar:
    st.header("Filters")
    selected_treatments = st.multiselect(
        "Treatment",
        sorted(overview_df["treatment"].dropna().unique().tolist()),
    )
    selected_stages = st.multiselect(
        "Cancer Stage",
        sorted(overview_df["stage"].dropna().unique().tolist()),
    )
    selected_payment_status = st.multiselect(
        "Payment Status",
        sorted(overview_df["payment_status"].dropna().unique().tolist()),
    )
    selected_date_preset = st.selectbox(
        "Date Preset",
        ["All Data", "Past Week", "Past Month", "Past 3 Months", "Past 6 Months", "Past Year", "Past 2 Years"],
        key=date_preset_key,
    )
    preset_start, preset_end = preset_to_range(selected_date_preset, min_date, max_date)
    if selected_date_preset != "All Data":
        st.session_state[date_start_key], st.session_state[date_end_key] = preset_start, preset_end

    selected_start_date = st.date_input(
        "Encounter Start Date",
        min_value=min_date,
        max_value=max_date,
        key=date_start_key,
    )
    selected_end_date = st.date_input(
        "Encounter End Date",
        min_value=min_date,
        max_value=max_date,
        key=date_end_key,
    )
    selected_start_date, selected_end_date = clamp_date_range(
        (selected_start_date, selected_end_date),
        min_date,
        max_date,
    )

filtered_df = overview_df.copy()

if selected_treatments:
    filtered_df = filtered_df[filtered_df["treatment"].isin(selected_treatments)]
if selected_stages:
    filtered_df = filtered_df[filtered_df["stage"].isin(selected_stages)]
if selected_payment_status:
    filtered_df = filtered_df[filtered_df["payment_status"].isin(selected_payment_status)]
filtered_df = filtered_df[
    filtered_df["encounter_date"].dt.date.between(selected_start_date, selected_end_date)
]

total_revenue = filtered_df["billing_amount"].fillna(0).sum()
avg_revenue = filtered_df["billing_amount"].fillna(0).mean()

metric_columns = st.columns(4)
metric_columns[0].metric("Encounters", f"{filtered_df['encounter_id'].nunique():,}")
metric_columns[1].metric("Patients", f"{filtered_df['patient_id'].nunique():,}")
metric_columns[2].metric("Revenue", f"${total_revenue:,.0f}")
metric_columns[3].metric("Avg Billing", f"${avg_revenue:,.0f}")

tab_overview, tab_clinical, tab_financial, tab_provider, tab_governance = st.tabs(
    ["Operational", "Clinical", "Financial", "Provider", "Governance"]
)

with tab_overview:
    encounter_trend = (
        filtered_df.assign(month=filtered_df["encounter_date"].dt.to_period("M").astype(str))
        .groupby("month", as_index=False)["encounter_id"]
        .nunique()
        .rename(columns={"encounter_id": "encounter_count"})
    )
    encounter_trend_fig = px.line(
        encounter_trend,
        x="month",
        y="encounter_count",
        markers=True,
        title="Encounter Volume Trend",
        color_discrete_sequence=["#62b7ff"],
    )
    encounter_trend_fig.update_traces(line=dict(width=3), marker=dict(size=7))
    st.plotly_chart(
        style_figure(encounter_trend_fig),
        use_container_width=True,
    )

    treatment_mix = (
        filtered_df.groupby("treatment", as_index=False)["encounter_id"]
        .nunique()
        .rename(columns={"encounter_id": "encounter_count"})
        .sort_values("encounter_count", ascending=False)
    )
    treatment_mix_fig = px.bar(
        treatment_mix,
        x="treatment",
        y="encounter_count",
        color="treatment",
        title="Treatment Mix",
        color_discrete_sequence=px.colors.sequential.Blues_r,
    )
    st.plotly_chart(
        style_figure(treatment_mix_fig),
        use_container_width=True,
    )

with tab_clinical:
    stage_distribution = (
        filtered_df.groupby("stage", as_index=False)["patient_id"]
        .nunique()
        .rename(columns={"patient_id": "patient_count"})
        .sort_values("patient_count", ascending=False)
    )
    stage_distribution_fig = px.pie(
        stage_distribution,
        values="patient_count",
        names="stage",
        title="Cancer Stage Distribution",
        color_discrete_sequence=["#8fd3ff", "#5dade2", "#2f98ff", "#1f6fd1", "#174a8b"],
    )
    stage_distribution_fig.update_traces(textfont=dict(color="white"), hole=0.4)
    st.plotly_chart(
        style_figure(stage_distribution_fig),
        use_container_width=True,
    )

    response_by_treatment = (
        filtered_df.groupby("treatment", as_index=False)["treatment_response_score"]
        .mean()
        .sort_values("treatment_response_score", ascending=False)
    )
    response_by_treatment_fig = px.bar(
        response_by_treatment,
        x="treatment",
        y="treatment_response_score",
        color="treatment",
        title="Average Treatment Response Score",
        color_discrete_sequence=px.colors.sequential.Tealgrn,
    )
    st.plotly_chart(
        style_figure(response_by_treatment_fig),
        use_container_width=True,
    )

with tab_financial:
    revenue_by_payer = (
        filtered_df.groupby("insurance", as_index=False)["billing_amount"]
        .sum()
        .sort_values("billing_amount", ascending=False)
    )
    revenue_by_payer_fig = px.bar(
        revenue_by_payer,
        x="insurance",
        y="billing_amount",
        color="insurance",
        title="Revenue by Insurance Type",
        color_discrete_sequence=["#a8ddff", "#74c0fc", "#4098ff", "#1c7ed6", "#174a8b"],
    )
    st.plotly_chart(
        style_figure(revenue_by_payer_fig),
        use_container_width=True,
    )

    payment_status_mix = (
        filtered_df.groupby("payment_status", as_index=False)["billing_amount"]
        .sum()
        .sort_values("billing_amount", ascending=False)
    )
    payment_status_mix_fig = px.funnel(
        payment_status_mix,
        x="billing_amount",
        y="payment_status",
        title="Billing Amount by Payment Status",
        color="payment_status",
        color_discrete_sequence=["#9ed3ff", "#62b7ff", "#2f98ff", "#0f6ed6"],
    )
    st.plotly_chart(
        style_figure(payment_status_mix_fig),
        use_container_width=True,
    )

with tab_provider:
    provider_encounter_fig = px.bar(
        provider_df.sort_values("encounter_count", ascending=False).head(15),
        x="provider_name",
        y="encounter_count",
        color="specialty",
        title="Top Providers by Encounter Count",
        color_discrete_sequence=["#7cc0ff", "#68d391", "#f6ad55", "#fc8181", "#b794f4"],
    )
    st.plotly_chart(
        style_figure(provider_encounter_fig),
        use_container_width=True,
    )
    provider_response_fig = px.bar(
        provider_df.sort_values("avg_treatment_response_score", ascending=False).head(15),
        x="provider_name",
        y="avg_treatment_response_score",
        color="specialty",
        title="Average Treatment Response by Provider",
        color_discrete_sequence=["#5dade2", "#48c9b0", "#f5b041", "#ec7063", "#af7ac5"],
    )
    st.plotly_chart(
        style_figure(provider_response_fig),
        use_container_width=True,
    )
    render_styled_table(provider_df, max_rows=10, compact=True)

with tab_governance:
    governance_df = filtered_df[
        ["patient_id", "patient_name", "gender", "age", "smoking_status", "cancer_type", "stage", "treatment", "billing_amount"]
    ].copy()
    governance_df["patient_id_masked"] = governance_df["patient_id"].astype(str).str[:3] + "****"
    governance_df = governance_df.drop(columns=["patient_id"]).rename(columns={"billing_amount": "amount"})

    st.subheader("Masked Patient Slice")
    render_styled_table(governance_df, max_rows=10, compact=True)

    dq_summary = pd.DataFrame(
        [
            {"check_name": "Missing billing amount", "rows": int(filtered_df["billing_amount"].isna().sum())},
            {"check_name": "Missing encounter date", "rows": int(filtered_df["encounter_date"].isna().sum())},
            {"check_name": "Duplicate encounter ids", "rows": int(filtered_df["encounter_id"].duplicated().sum())},
        ]
    )
    st.subheader("Data Quality Summary")
    render_styled_table(dq_summary, max_rows=10)

    st.subheader("Recent Pipeline Audit")
    render_styled_table(audit_df, max_rows=20)
