import os
import smtplib
import pandas as pd
import streamlit as st
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Optional

from sqlalchemy import create_engine, text
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="User Activity",  # Title of the app
    layout="wide",                 # Use the 'wide' layout
    page_icon = "https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
    initial_sidebar_state="collapsed"  # Sidebar state (optional)
)

RECIPIENTS_TO  = [
    "shashankgupta@coresight.com",
    "deborahweinswig@coresight.com",
    "MaxKahn@coresight.com",
    "johnmercer@coresight.com",
    "MohdSaeedAfri@coresight.com",
    "vaishnavinayakk@coresight.com"
]

RECIPIENTS_CC  = [
    "dataautomation@coresight.com",
]
RECIPIENTS_BCC = [
    # "audit@coresight.com",
]

DB_HOST = os.getenv("DB_HOST_PROD")
DB_NAME = os.getenv("DB_NAME_PROD")
DB_USER = os.getenv("DB_USER_PROD")
DB_PASS = os.getenv("DB_PASSWORD_PROD")
SSL_CA  = os.getenv("SSL_CA_PROD")

EMAIL_FROM = os.getenv("EMAIL_FROM", "dataautomation@coresight.com")
EMAIL_SMTP = os.getenv("EMAIL_SMTP", "smtp.office365.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_PASS = os.getenv("EMAIL_PASSWORD","D)636612595014om")

# Scheduler timezone (job trigger time). Runs at 08:00 ET every Monday.
TZ_ET = ZoneInfo("America/New_York")

# ------------------ DB Engine ------------------
def get_engine():
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS]):
        raise RuntimeError("Missing DB env vars: DB_HOST_PROD, DB_NAME_PROD, DB_USER_PROD, DB_PASSWORD_PROD")
    conn_str = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    connect_args = {"ssl_ca": SSL_CA} if SSL_CA else {}
    return create_engine(conn_str, connect_args=connect_args, pool_pre_ping=True)

# ------------------ Time window (UTC) ------------------
def last_full_week_window_utc():
    """
    Returns [start_utc, end_utc, label] for the last full calendar week in UTC:
    Monday 00:00 UTC (inclusive) to next Monday 00:00 UTC (exclusive).
    """
    now_utc = datetime.now(timezone.utc)
    today_utc = now_utc.date()
    weekday = today_utc.weekday()  # Monday=0 ... Sunday=6
    this_monday_utc = today_utc - timedelta(days=weekday)
    last_monday_utc = this_monday_utc - timedelta(days=7)

    start_utc = datetime.combine(last_monday_utc, time(0, 0, 0, tzinfo=timezone.utc))
    end_utc   = start_utc + timedelta(days=7)

    label = f"{start_utc.strftime('%b %d, %Y')} – {(end_utc - timedelta(seconds=1)).strftime('%b %d, %Y')} UTC"
    return start_utc, end_utc, label

# ------------------ Query + format ------------------
SQL_LAST_WEEK = text("""
SELECT
  session_id,
  user_email,
  user_display_name,
  login_at,
  last_activity,
  membership_id,
  membership_type
FROM user_login_sessions
WHERE last_activity >= :start_utc
  AND last_activity <  :end_utc
  AND COALESCE(NULLIF(TRIM(user_email), ''), NULL) IS NOT NULL
""")

def fetch_last_week_logins(engine):
    start_utc, end_utc, label = last_full_week_window_utc()
    with engine.connect() as conn:
        df = pd.read_sql(SQL_LAST_WEEK, conn, params={"start_utc": start_utc, "end_utc": end_utc})
    for col in ("login_at", "last_activity"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df, label

def dedupe_latest_by_user(df: pd.DataFrame) -> pd.DataFrame:
    """Keep each user's latest activity within the window."""
    if df.empty:
        return df
    df = df.sort_values("last_activity").copy()
    latest = df.groupby("user_email", as_index=False).tail(1)
    return latest.sort_values(["last_activity", "user_email"], ascending=[False, True])

def _fmt_dt_utc_series(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s, utc=True, errors="coerce")
    return s.dt.strftime("%Y-%m-%d %H:%M UTC")  # 24h UTC

def _df_for_email(latest: pd.DataFrame) -> pd.DataFrame:
    """Select columns + prettify headers & datetime formatting for email."""
    cols_order = [
        "user_email", "user_display_name",
        "membership_type", "membership_id",
        "session_id", "login_at", "last_activity",
    ]
    df = latest.copy()
    cols = [c for c in cols_order if c in df.columns]
    df = df[cols]
    if "login_at" in df.columns:
        df["login_at"] = _fmt_dt_utc_series(df["login_at"])
    if "last_activity" in df.columns:
        df["last_activity"] = _fmt_dt_utc_series(df["last_activity"])
    df.rename(columns={
        "user_email": "User Email",
        "user_display_name": "Name",
        "membership_type": "Membership Type",
        "membership_id": "Membership ID",
        "session_id": "Session ID",
        "login_at": "Login At (UTC)",
        "last_activity": "Last Activity (UTC)",
    }, inplace=True)
    return df

def _html_table(df: pd.DataFrame, title: str = "", max_rows: int = 1000) -> str:
    """Inline-styled HTML table (Outlook/Gmail friendly)."""
    truncated = False
    if len(df) > max_rows:
        df = df.head(max_rows).copy()
        truncated = True

    df = df.fillna("")
    table_style = "border-collapse:collapse;width:100%;font-family:Arial,Helvetica,sans-serif;font-size:12px;"
    th_style = "text-align:left;padding:6px 8px;border:1px solid #dcdfe3;background:#f3f4f6;color:#111;"
    td_style = "padding:6px 8px;border:1px solid #eef0f3;color:#111;"

    ths = "".join(f"<th style='{th_style}'>{col}</th>" for col in df.columns)
    rows_html = []
    for _, row in df.iterrows():
        tds = "".join(f"<td style='{td_style}'>{row[col]}</td>" for col in df.columns)
        rows_html.append(f"<tr>{tds}</tr>")
    rows = "\n".join(rows_html)

    note = f"<p style='font-size:12px;color:#555;'>Showing first {max_rows} rows (truncated).</p>" if truncated else ""
    title_html = f"<h3 style='font-family:Arial; font-weight:600; color:#111;'>{title}</h3>" if title else ""

    return f"""
    {title_html}
    <table role="grid" style="{table_style}">
      <thead><tr>{ths}</tr></thead>
      <tbody>{rows}</tbody>
    </table>
    {note}
    """

# ------------------ Email send ------------------
def send_email_table(subject: str, plain_intro: str, html_intro: str, df: pd.DataFrame, week_label: str,
                     to: List[str], cc: Optional[List[str]] = None, bcc: Optional[List[str]] = None):
    cc = cc or []
    bcc = bcc or []

    msg = MIMEMultipart("alternative")
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject

    # Plain text part (fallback)
    plain_body = f"{plain_intro}\n\nReporting window: {week_label}\n\nRows: {len(df)}"
    msg.attach(MIMEText(plain_body, "plain"))

    # HTML part with table
    tbl = _html_table(_df_for_email(df), title=f"Weekly Login Activity ({week_label})", max_rows=1000)
    html_body = f"""
    <div style="font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#111;">
      <p>{html_intro}</p>
      <p><strong>Reporting window:</strong> {week_label}</p>
      {tbl}
    </div>
    """
    msg.attach(MIMEText(html_body, "html"))

    # SMTP envelope recipients (TO + CC + BCC)
    all_rcpts = list(dict.fromkeys([*to, *cc, *bcc]))

    with smtplib.SMTP(EMAIL_SMTP, EMAIL_PORT) as server:
        server.starttls()
        if EMAIL_PASS:
            server.login(EMAIL_FROM, EMAIL_PASS)
        server.sendmail(EMAIL_FROM, all_rcpts, msg.as_string())

# ------------------ Job ------------------
def run_weekly_login_report():
    eng = get_engine()
    df, week_label = fetch_last_week_logins(eng)

    # keep this only for the unique user count in the header
    latest = dedupe_latest_by_user(df)

    total_sessions = len(df)
    unique_users = latest["user_email"].nunique() if not latest.empty else 0

    subject = f"Weekly SIP Report – {week_label}"
    plain_intro = (
        f"Weekly login activity for {week_label}\n"
        f"Unique users: {unique_users}\n"
        f"Total sessions: {total_sessions}"
    )
    html_intro = (
        f"Weekly login activity summary.<br>"
        f"<b>Unique users:</b> {unique_users} &nbsp; | &nbsp; "
        f"<b>Total sessions:</b> {total_sessions}"
    )

    # >>> Use ALL sessions in the email table (sorted newest first)
    # Use ALL sessions in the email table (sorted newest first)
    if not df.empty:
        sort_cols = [c for c in ("last_activity", "login_at") if c in df.columns]
        if sort_cols:
            table_df = df.sort_values(by=sort_cols, ascending=[False] * len(sort_cols))
        else:
            table_df = df  # no known datetime cols; leave as-is
    else:
        table_df = df


    send_email_table(
        subject=subject,
        plain_intro=plain_intro,
        html_intro=html_intro,
        df=table_df,            # <<< send all sessions
        week_label=week_label,
        to=RECIPIENTS_TO,
        cc=RECIPIENTS_CC,
        bcc=RECIPIENTS_BCC,
    )

    print(f"[{datetime.now(timezone.utc).isoformat()}] Weekly SIP report emailed "
          f"to {len(RECIPIENTS_TO)} TO, {len(RECIPIENTS_CC)} CC, {len(RECIPIENTS_BCC)} BCC "
          f"— {unique_users} users, {total_sessions} sessions (UTC window).")


# ------------------ Streamlit-safe scheduler ------------------
@st.cache_resource
def start_scheduler() -> BackgroundScheduler:
    """
    Start a single BackgroundScheduler per Streamlit process.
    Uses a fixed job id to avoid duplicates on reruns.
    """
    sched = BackgroundScheduler(timezone=TZ_ET)
    sched.add_job(
        run_weekly_login_report,
        CronTrigger(day_of_week="mon", hour=8, minute=0),
        id="user_activity",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=3600,  # run if missed by < 1h
        # Uncomment to also fire once immediately on first load:
        # next_run_time=datetime.now(TZ_ET),
    )
    sched.start()
    return sched

# Initialize scheduler once
scheduler = start_scheduler()

# Optional: trigger a one-off run now for testing via URL param ?run_now=1
run_now = (st.query_params.get("run_now", "0") or "0").lower() in ("1", "true", "yes")
if run_now:
    run_weekly_login_report()
    st.success("Triggered weekly login report now.")

# Tiny status indicator (optional)
jobs = scheduler.get_jobs()
if jobs:
    nxt = jobs[0].next_run_time
    if nxt:
        st.caption(f"Weekly login report scheduled — next run: {nxt.astimezone(TZ_ET).strftime('%a %b %d, %Y %I:%M %p %Z')}")
