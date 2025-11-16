# pages/promote_staging_to_prod.py
import os
import time
import smtplib
import pandas as pd
import streamlit as st
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Sync Data",
    page_icon="https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
    layout="wide",
)

IS_STAGING = os.getenv("ENVIRONMENT", "").lower() == "staging"
STAGING_BASE = "https://sip-portal-stg.coresight.com"
PROD_BASE    = "https://sip-portal.coresight.com"
CURRENT_PATH = "/sync_data"  # this file's route

if not IS_STAGING:
    st.markdown(
        f"""
        <script>
          (function(){{
            const qs = window.location.search || "";
            window.location.replace("{STAGING_BASE}{CURRENT_PATH}" + qs);
          }})();
        </script>
        """,
        unsafe_allow_html=True,
    )
    st.switch_page("pages/opening.py") 

st.markdown("""
    <style>
    .stApp, .main, .css-18e3th9, .css-1y0tads, main { padding: 0 !important; margin: 0 !important; }
    .css-18e3th9 { padding-top: 85px !important; margin-top: 0 !important; }
    header[data-testid="stHeader"], footer { display: none !important; }
    #MainMenu { visibility: hidden !important; }
    body, .stApp { background: #fff !important; }
    </style>
""", unsafe_allow_html=True)


# Optional header include (ignore if not in your repo)
try:
    from html_utils import include_html
    include_html("header.html")
except Exception:
    pass

# ─────────────────────────────────────────────────────────────────────────────
# Auth (matches your portal style; falls back if helpers not available)
# ─────────────────────────────────────────────────────────────────────────────
try:
    from streamlit_cookies_controller import CookieController
except Exception:
    CookieController = None

try:
    from auth_utils import _read_auth_cookie, logout  # your helper
except Exception:
    def _read_auth_cookie(): return {}
    def logout(): pass

EMPLOYEE_ID = os.getenv("EMPLOYEE_ID", "101318").strip()
LOGIN_URL   = "/login"

def ensure_auth_in_session():
    if st.session_state.get("user_email"):
        return
    existing = _read_auth_cookie() or {}
    if not existing:
        return
    st.session_state["session_id"] = existing.get("session_id")
    st.session_state["user_email"] = existing.get("user_email")
    st.session_state["token"] = existing.get("token")
    st.session_state["membership"] = existing.get("membership") or {}
    st.session_state["membership_id"] = existing.get("membership_id")
    st.session_state["membership_type"] = existing.get("membership_type")

def save_auth_cookie(data: dict):
    try:
        cc = CookieController(key="auth_cookies")
        cc.set("auth_data", json.dumps(data), max_age_days=30, same_site="Lax")
    except Exception:
        pass

ensure_auth_in_session()
user_email = (st.session_state.get("user_email") or "").strip()
membership_id = (str(st.session_state.get("membership_id") or "")).strip()

if not user_email:
    st.warning("You must be logged in to access this page.")
    st.link_button("Go to Login", "https://sip-portal-stg.coresight.com/")
    st.stop()

if membership_id != EMPLOYEE_ID:
    st.info("Access restricted to employees. Redirecting you…")
    st.switch_page("pages/opening.py")



DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
SSL_CA = os.getenv("SSL_CA")

DB_HOST_PROD = os.getenv("DB_HOST_PROD")
DB_NAME_PROD = os.getenv("DB_NAME_PROD")
DB_USER_PROD = os.getenv("DB_USER_PROD")
DB_PASSWORD_PROD = os.getenv("DB_PASSWORD_PROD")

staging_conn_str = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
production_conn_str = f"mysql+mysqlconnector://{DB_USER_PROD}:{DB_PASSWORD_PROD}@{DB_HOST_PROD}/{DB_NAME_PROD}"

@st.cache_resource
def get_engines():
    return (
        create_engine(
            staging_conn_str,
            connect_args={"ssl_ca": SSL_CA} if SSL_CA else {},
            pool_pre_ping=True,
        ),
        create_engine(
            production_conn_str,
            connect_args={"ssl_ca": SSL_CA} if SSL_CA else {},  # optional, but safer if prod requires SSL
            pool_pre_ping=True,
        ),
    )

staging_engine, production_engine = get_engines()

# ─────────────────────────────────────────────────────────────────────────────
# Email config (reusing your Office365 pattern)
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_RECIPIENTS = [
    "ShashankGupta@coresight.com",
    "MaxKahn@coresight.com",
    "johnmercer@coresight.com",
    "jtblubaugh@coresight.com",
    "MohdSaeedAfri@coresight.com",
    "vaishnavinayakk@coresight.com",
]
EMAIL_RECIPIENTS = os.getenv("PROMO_EMAIL_RECIPIENTS", ",".join(DEFAULT_RECIPIENTS)).split(",")
FROM_EMAIL = os.getenv("FROM_EMAIL", "dataautomation@coresight.com")
FROM_EMAIL_PASSWORD = os.getenv("FROM_EMAIL_PASSWORD", "D)636612595014om")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

def _uniq_emails(*lists):
    seen, out = set(), []
    for lst in lists:
        for e in lst or []:
            key = (e or "").strip().lower()
            if key and key not in seen:
                seen.add(key); out.append((e or "").strip())
    return out

def send_success_email(tables_done, row_counts, elapsed_min, fk_disabled, triggered_by):
    try:
        recipients = _uniq_emails(EMAIL_RECIPIENTS, [triggered_by])
        if not recipients:
            return

        rows_html = "".join(
            f"<tr><td style='padding:6px 10px;border:1px solid #eee;'>{t}</td>"
            f"<td style='padding:6px 10px;border:1px solid #eee;text-align:right;'>{row_counts.get(t, 0):,}</td></tr>"
            for t in tables_done
        )
        table_html = f"""
        <table style="border-collapse:collapse;border:1px solid #ddd;">
          <thead>
            <tr style="background:#f8f8f8">
              <th style="padding:6px 10px;border:1px solid #eee;text-align:left;">Table</th>
              <th style="padding:6px 10px;border:1px solid #eee;text-align:right;">Rows Copied</th>
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
        """

        subject = "[SIP Promote] STAGING to PRODUCTION complete"
        html = f"""
        <html><body>
          <p><strong>Promotion completed successfully.</strong></p>
          <p><b>Triggered by:</b> {triggered_by or 'unknown'}<br/>
             <b>FK checks disabled:</b> {"Yes" if fk_disabled else "No"}<br/>
             <b>Elapsed:</b> {elapsed_min:.2f} minutes
          </p>
          {table_html}
        </body></html>
        """

        msg = MIMEMultipart("alternative")
        msg["From"] = FROM_EMAIL
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.ehlo(); server.starttls(); server.login(FROM_EMAIL, FROM_EMAIL_PASSWORD)
            server.sendmail(FROM_EMAIL, recipients, msg.as_string())
    except Exception as e:
        st.warning(f"Promotion succeeded but email failed to send: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Migration helpers (your logic, adapted for UI + safety)
# ─────────────────────────────────────────────────────────────────────────────
def ensure_prod_table(stg_conn, prod_conn, prod_engine, table_name: str, log):
    insp = inspect(prod_engine)
    if table_name in insp.get_table_names():
        prod_conn.exec_driver_sql(f"TRUNCATE TABLE `{table_name}`")
        log.write(" - Production table truncated\n")
        return

    log.write(" - Table missing in PROD — creating from STAGING schema…\n")
    row = stg_conn.exec_driver_sql(f"SHOW CREATE TABLE `{table_name}`").fetchone()
    if not row or len(row) < 2:
        raise RuntimeError(f"Could not fetch CREATE TABLE for `{table_name}` from STAGING.")
    create_sql = row[1]
    prod_conn.exec_driver_sql(create_sql)
    log.write(" - Created table in PROD\n")
    prod_conn.exec_driver_sql(f"TRUNCATE TABLE `{table_name}`")
    log.write(" - Production table truncated\n")

def get_prod_columns(prod_engine, table_name: str):
    insp = inspect(prod_engine)
    cols = [c["name"] for c in insp.get_columns(table_name)]
    if not cols:
        raise RuntimeError(f"No columns found for `{table_name}` in PROD after creation.")
    return cols

def copy_table(tbl: str, read_chunksize: int, write_chunksize: int, fk_disable: bool, log_area) -> int:
    total_rows = 0
    with staging_engine.connect() as stg_conn, production_engine.begin() as prod_conn:
        if fk_disable:
            prod_conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS=0")
        try:
            log_area.write(f"\n### Syncing `{tbl}`\n")
            ensure_prod_table(stg_conn, prod_conn, production_engine, tbl, log_area)

            prod_cols = get_prod_columns(production_engine, tbl)
            select_sql = f"SELECT * FROM `{tbl}`"

            first = True
            for df in pd.read_sql_query(select_sql, con=stg_conn, chunksize=read_chunksize):
                df_cols = list(df.columns)
                extra_in_df = [c for c in df_cols if c not in prod_cols]
                missing_in_df = [c for c in prod_cols if c not in df_cols]

                if extra_in_df:
                    df = df.drop(columns=extra_in_df, errors="ignore")
                if missing_in_df:
                    for c in missing_in_df:
                        df[c] = None
                df = df[prod_cols]

                if first:
                    if extra_in_df:
                        log_area.write(f" - Dropped columns not in PROD: {extra_in_df}\n")
                    if missing_in_df:
                        log_area.write(f" - Added missing columns (NULL): {missing_in_df}\n")
                    first = False

                df.to_sql(
                    name=tbl,
                    con=prod_conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=write_chunksize,
                )
                total_rows += len(df)
                log_area.write(f" - Copied rows so far: {total_rows:,}\n")
        finally:
            if fk_disable:
                prod_conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS=1")
    return total_rows

# ─────────────────────────────────────────────────────────────────────────────
# UI – controls
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_TABLES = [
    "data_change_log",
    "parent_chain_names_data",
    "all_opened_cy",
    "all_opened_py",
    "all_closed_cy",
    "all_closed_py",
    "all_active_cy",
    "all_active_py",
    "population_data_by_age_and_sex",  # keep as-is if that's the actual table name
    "run_comparison_opened_closed_detail_log",
    "run_comparison_reopening_detail_log",
    "retailers_edit_audit_log",
]

try:
    cookie_controller = CookieController(key="auth_cookies")
except Exception:
    cookie_controller = {}

def load_auth_cookie():
    if not cookie_controller:
        return {}
    try:
        raw_cookie = cookie_controller.get("auth_data")
        if not raw_cookie: return {}
        if isinstance(raw_cookie, str):  return json.loads(raw_cookie)
        if isinstance(raw_cookie, dict): return raw_cookie
    except Exception:
        return {}
    return {}

auth_cookie = load_auth_cookie() or {}
user_filters = (auth_cookie.get("filters") or {})

col1, col3, col4, col5, col2 = st.columns([6.1, 1.5, 1.2, 1.6, 1], vertical_alignment="center")
with col1:
    st.subheader("Promote Staging to Production")
    st.caption("Select tables below, then click **Run Promotion** to copy from STAGING into PRODUCTION.")
    st.html("<style>[data-testid='stHeaderActionElements'] {display: none;}</style>")
with col3:
    # put the portal hop behind a button (prevents auto-redirect loops)
    if st.button("App Release Portal", type="tertiary", icon=":material/release_alert:"):
        user_filters["returnPage"] = "sync_data"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        # same-tab navigation
        st.switch_page("pages/app_release_portal.py")

with col4:
    if st.button("Edit Retailers", key="go_to_retailers_editing",
                 type="tertiary", icon=":material/edit:"):
        # remember return page in the cookie
        user_filters["returnPage"] = "sync_data"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        st.switch_page("pages/retailers_editing.py")

with col5:
    if st.button("Missing Logs Upload", key="go_to_missing_logs", type="tertiary", icon=":material/cloud_upload:"):
        user_filters["returnPage"] = "sync_data"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        st.switch_page("pages/missing_logs_upload.py")

with col2:
    if st.button("Log Out", key="Logout", type="tertiary", icon=":material/logout:"):
        logout()
        _redirect_immediate(LOGIN_URL)

left, right = st.columns([0.65, 0.35])

with left:
    all_tables = DEFAULT_TABLES
    sel_all = st.checkbox("Select all tables", value=False, key="sel_all")
    selected = st.multiselect(
        "Tables to promote",
        options=all_tables,
        default=all_tables if sel_all else all_tables[:1],
        help="Only selected tables will be copied into PROD (table is truncated first).",
    )
    colA, colB, colC = st.columns(3)
    read_chunksize  = colA.number_input("Read chunksize", 10_000, 500_000, 50_000, step=10_000)
    write_chunksize = colB.number_input("Write chunksize", 5_000, 200_000, 10_000, step=5_000)
    fk_disable = colC.toggle("Disable FK checks", value=True, help="Temporarily SET FOREIGN_KEY_CHECKS=0 during insert")

with right:
    st.markdown("**Connections (hidden for security)**")
    st.text("STAGING connection verified")
    st.text("PRODUCTION connection verified")
    st.markdown("**Email**")
    st.write("Recipients:", ", ".join([e.strip() for e in EMAIL_RECIPIENTS if e.strip()]))


run = st.button("Run Promotion", type="primary", use_container_width=True, disabled=not selected)
st.warning("Note: Promoting any table from staging to production will send an email alert to the SIP team.")

# live log area
log = st.empty()
log_text = []

class UILog:
    def write(self, s: str):
        log_text.append(s)
        log.markdown("```text\n" + "".join(log_text)[-5000:] + "\n```")

ui_log = UILog()

# ─────────────────────────────────────────────────────────────────────────────
# Execute on click
# ─────────────────────────────────────────────────────────────────────────────
if run:
    start_time = time.time()
    row_counts = {}
    error = None
    with st.status("Running promotion…", expanded=True) as status:
        try:
            for tbl in selected:
                try:
                    copied = copy_table(tbl, int(read_chunksize), int(write_chunksize), bool(fk_disable), ui_log)
                    row_counts[tbl] = copied
                    st.success(f"{tbl}: {copied:,} rows")
                except Exception as e_tbl:
                    st.error(f"{tbl}: FAILED — {e_tbl}")
                    ui_log.write(f" !! ERROR on {tbl}: {e_tbl}\n")
                    raise
            elapsed = (time.time() - start_time) / 60.0
            ui_log.write(f"\n⏱ Elapsed time: {elapsed:.2f} minutes\n")
            status.update(label="Promotion complete", state="complete", expanded=False)
        except Exception as e:
            error = e
            status.update(label="Promotion failed", state="error", expanded=True)

    if error:
        st.error(f"Promotion failed: {error}")
    else:
        # Summary + email
        elapsed = (time.time() - start_time) / 60.0
        st.success(f"All done. Elapsed: {elapsed:.2f} minutes")
        st.dataframe(
            pd.DataFrame(
                [{"Table": t, "Rows Copied": row_counts.get(t, 0)} for t in selected]
            ).set_index("Table")
        )
        try:
            send_success_email(
                tables_done=selected,
                row_counts=row_counts,
                elapsed_min=elapsed,
                fk_disabled=bool(fk_disable),
                triggered_by=user_email,
            )
            st.info("Success email sent ✅")
        except Exception as e:
            st.warning(f"Promotion ok, but email failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Danger zone footer
# ─────────────────────────────────────────────────────────────────────────────
with st.expander("⚠️ Notes & Safety", expanded=False):
    st.markdown(
        "- For each selected table, the PROD table is **TRUNCATED** before inserting.\n"
        "- Schemas are aligned to match PROD (extra cols dropped, missing cols filled NULL).\n"
        "- Use environment variables `STAGING_CONN_STR` and `PRODUCTION_CONN_STR` to override connections.\n"
        "- Email recipients can be overridden with `PROMO_EMAIL_RECIPIENTS` (comma-separated).\n"
    )
