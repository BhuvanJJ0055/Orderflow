"""
Orderflow Analytics Dashboard
Real-time e-commerce data pipeline monitoring and analytics.
"""

import sys
import time
from datetime import datetime
from pathlib import Path

# ── Ensure repository root is on sys.path ─────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Orderflow Analytics",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  /* ── global ── */
  [data-testid="stAppViewContainer"] {
      background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
      color: #e8e8e8;
  }
  [data-testid="stSidebar"] {
      background: rgba(255,255,255,0.04);
      border-right: 1px solid rgba(255,255,255,0.08);
  }
  h1, h2, h3 { color: #ffffff; }
  /* ── metric cards ── */
  [data-testid="stMetric"] {
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 12px;
      padding: 16px 20px;
      backdrop-filter: blur(10px);
  }
  [data-testid="stMetricLabel"] { color: #a0aec0 !important; font-size: 13px !important; }
  [data-testid="stMetricValue"] { color: #ffffff !important; font-size: 28px !important; font-weight: 700 !important; }
  [data-testid="stMetricDelta"] { font-size: 12px !important; }
  /* ── section headers ── */
  .section-header {
      font-size: 1.1rem;
      font-weight: 600;
      color: #a78bfa;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      margin: 1.5rem 0 0.75rem;
      border-bottom: 1px solid rgba(167,139,250,0.3);
      padding-bottom: 6px;
  }
  /* ── status pill ── */
  .pill-green { background: #22c55e22; color: #22c55e; border: 1px solid #22c55e55;
      border-radius: 20px; padding: 2px 12px; font-size: 12px; display: inline-block; }
  .pill-red { background: #ef444422; color: #ef4444; border: 1px solid #ef444455;
      border-radius: 20px; padding: 2px 12px; font-size: 12px; display: inline-block; }
  /* ── dataframe ── */
  [data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ── DB connection ─────────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    from src.etl.config import DATABASE_URL
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def db_ok() -> tuple[bool, str]:
    try:
        run_query("SELECT 1")
        return True, ""
    except Exception as e:
        return False, str(e)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📦 Orderflow")
    st.markdown("---")

    connected, err_msg = db_ok()
    pill = (
        '<span class="pill-green">● Connected</span>'
        if connected
        else '<span class="pill-red">● Disconnected</span>'
    )
    st.markdown(f"**Database** {pill}", unsafe_allow_html=True)

    if not connected and err_msg:
        st.caption(f"Error: {err_msg}")

    st.markdown("---")
    refresh_interval = st.selectbox("Auto-refresh", [0, 10, 30, 60], index=0,
                                    format_func=lambda x: "Off" if x == 0 else f"{x}s")
    if st.button("🔄 Refresh now"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

if not connected:
    st.error(f"Cannot connect to PostgreSQL ({err_msg}). Start the database with `docker compose up -d postgres` and try again.")
    st.stop()

# ── Auto-refresh ──────────────────────────────────────────────────────────────
if refresh_interval > 0:
    time.sleep(refresh_interval)
    st.cache_data.clear()
    st.rerun()

# ── Title ─────────────────────────────────────────────────────────────────────
st.markdown("# 📦 Orderflow Analytics")
st.markdown("Real-time e-commerce data pipeline monitoring dashboard")

# ═══════════════════════════════════════════════════════════════════════════════
# 1. KPI row
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-header">Pipeline KPIs</div>', unsafe_allow_html=True)

kpi_sql = """
SELECT
    (SELECT COUNT(DISTINCT order_id) FROM analytics.fact_order_items)          AS total_orders,
    (SELECT COUNT(DISTINCT customer_key) FROM analytics.fact_order_items)      AS unique_customers,
    (SELECT COALESCE(SUM(price), 0) FROM analytics.fact_order_items)           AS total_revenue,
    (SELECT COUNT(*) FROM analytics.agg_daily_category_revenue)                AS spark_agg_rows,
    (SELECT COUNT(*) FROM analytics.live_orders)                               AS streamed_events,
    (SELECT COUNT(*) FROM staging.orders)                                      AS staging_rows
"""

try:
    kpi = run_query(kpi_sql).iloc[0]
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Orders", f"{int(kpi.total_orders):,}")
    c2.metric("Unique Customers", f"{int(kpi.unique_customers):,}")
    c3.metric("Total Revenue", f"R$ {float(kpi.total_revenue):,.0f}")
    c4.metric("Spark Agg Rows", f"{int(kpi.spark_agg_rows):,}")
    c5.metric("Streamed Events", f"{int(kpi.streamed_events):,}")
    c6.metric("Staging Rows", f"{int(kpi.staging_rows):,}")
except Exception as e:
    st.warning(f"KPI query failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 2. Revenue over time
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-header">Daily Revenue by Category</div>', unsafe_allow_html=True)

rev_sql = """
SELECT order_date, category, revenue, orders
FROM analytics.agg_daily_category_revenue
ORDER BY order_date, category
"""
try:
    rev_df = run_query(rev_sql)
    if rev_df.empty:
        st.info("No aggregated revenue data yet. Run the PySpark job first.")
    else:
        rev_df["order_date"] = pd.to_datetime(rev_df["order_date"])

        # Category filter
        all_cats = sorted(rev_df["category"].dropna().unique())
        selected = st.multiselect("Filter categories", all_cats,
                                  default=all_cats[:6] if len(all_cats) >= 6 else all_cats)
        filtered = rev_df[rev_df["category"].isin(selected)] if selected else rev_df

        daily_total = filtered.groupby("order_date")["revenue"].sum().reset_index()
        daily_total.columns = ["order_date", "total_revenue"]

        fig_rev = px.area(
            daily_total, x="order_date", y="total_revenue",
            title="Aggregated Daily Revenue (all selected categories)",
            color_discrete_sequence=["#a78bfa"],
            template="plotly_dark",
        )
        fig_rev.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e8e8e8", title_font_size=15,
            xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.07)"),
        )
        st.plotly_chart(fig_rev, use_container_width=True)

        # Top 10 categories bar
        top_cat = rev_df.groupby("category")["revenue"].sum().nlargest(10).reset_index()
        fig_cat = px.bar(
            top_cat, x="revenue", y="category", orientation="h",
            title="Top 10 Categories by Total Revenue",
            color="revenue", color_continuous_scale="Purples",
            template="plotly_dark",
        )
        fig_cat.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e8e8e8", title_font_size=15, showlegend=False,
            yaxis=dict(autorange="reversed", showgrid=False),
            xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.07)"),
        )
        st.plotly_chart(fig_cat, use_container_width=True)

except Exception as e:
    st.warning(f"Revenue query failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 3. Live stream monitor
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-header">Live Kafka Stream Monitor</div>', unsafe_allow_html=True)

stream_sql = """
SELECT
    date_trunc('minute', ingested_ts) AS minute,
    COUNT(*) AS events
FROM analytics.live_orders
GROUP BY 1
ORDER BY 1 DESC
LIMIT 60
"""
try:
    stream_df = run_query(stream_sql)
    if stream_df.empty:
        st.info("No live events yet. Start the Kafka producer and consumer.")
    else:
        stream_df["minute"] = pd.to_datetime(stream_df["minute"])
        stream_df = stream_df.sort_values("minute")

        fig_stream = px.bar(
            stream_df, x="minute", y="events",
            title="Events per Minute (last 60 minutes)",
            color_discrete_sequence=["#34d399"],
            template="plotly_dark",
        )
        fig_stream.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e8e8e8", title_font_size=15,
            xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.07)"),
        )
        st.plotly_chart(fig_stream, use_container_width=True)

        recent_sql = """
            SELECT event_id, order_id, product_id, amount,
                   to_char(event_ts AT TIME ZONE 'UTC', 'HH24:MI:SS') AS event_ts,
                   to_char(ingested_ts AT TIME ZONE 'UTC', 'HH24:MI:SS') AS ingested_ts
            FROM analytics.live_orders
            ORDER BY ingested_ts DESC LIMIT 20
        """
        recent = run_query(recent_sql)
        st.dataframe(recent, use_container_width=True, hide_index=True)

except Exception as e:
    st.warning(f"Stream query failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 4. ETL pipeline health
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-header">ETL Pipeline Runs</div>', unsafe_allow_html=True)

runs_sql = """
SELECT
    run_id,
    mode AS run_mode,
    status,
    to_char(started_at, 'YYYY-MM-DD HH24:MI:SS') AS started_at,
    to_char(finished_at, 'YYYY-MM-DD HH24:MI:SS') AS finished_at,
    rows_in,
    rows_loaded,
    rows_rejected
FROM ops.pipeline_runs
ORDER BY started_at DESC
LIMIT 20
"""
try:
    runs_df = run_query(runs_sql)
    if runs_df.empty:
        st.info("No pipeline runs recorded yet.")
    else:
        def _color_status(val):
            if val == "success":
                return "color: #22c55e"
            if val == "failed":
                return "color: #ef4444"
            return "color: #fbbf24"

        styler = runs_df.style
        if hasattr(styler, "map"):
            styled = styler.map(_color_status, subset=["status"])
        else:
            styled = styler.applymap(_color_status, subset=["status"])
        st.dataframe(styled, use_container_width=True, hide_index=True)
except Exception as e:
    st.warning(f"Pipeline runs query failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 5. Star schema row counts
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-header">Star Schema Row Counts</div>', unsafe_allow_html=True)

schema_sql = """
SELECT 'dim_customers' AS table_name, COUNT(*) AS rows FROM analytics.dim_customers
UNION ALL SELECT 'dim_products', COUNT(*) FROM analytics.dim_products
UNION ALL SELECT 'dim_date', COUNT(*) FROM analytics.dim_date
UNION ALL SELECT 'fact_order_items', COUNT(*) FROM analytics.fact_order_items
ORDER BY rows DESC
"""
try:
    schema_df = run_query(schema_sql)
    fig_schema = px.bar(
        schema_df, x="table_name", y="rows",
        title="Analytics Schema Row Counts",
        color="rows", color_continuous_scale="Blues",
        template="plotly_dark",
        text_auto=True,
    )
    fig_schema.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color="#e8e8e8", title_font_size=15, showlegend=False,
        xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.07)"),
    )
    st.plotly_chart(fig_schema, use_container_width=True)
except Exception as e:
    st.warning(f"Schema counts query failed: {e}")

st.markdown("---")
st.caption("Orderflow Analytics Dashboard · Built with Streamlit + Plotly")
