# pages/app_release_portal.py
import json
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from streamlit_cookies_controller import CookieController

from html_utils import include_html
import streamlit as st
from datetime import date, datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import calendar

st.set_page_config(
    page_title="Store Intelligence Platform: Monthly Release Portal",
    page_icon="https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
    layout="wide"
)

IS_STAGING = os.getenv("ENVIRONMENT", "").lower() == "staging"
STAGING_BASE = "https://sip-portal-stg.coresight.com"
PROD_BASE    = "https://sip-portal.coresight.com"
CURRENT_PATH = "/app_release_portal"  # this file's route

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


# Basic layout polish
st.markdown("""
    <style>
    .block-container { max-width: 1300px !important; margin: 0 auto !important; padding-left: 0 !important; padding-right: 0 !important; }
    .main { padding-left: 0 !important; padding-right: 0 !important; }
    </style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
#MainMenu {visibility: hidden;}
header {padding-top: 0rem;}
.css-1y0tads, .block-container { padding-top: 2.1rem !important; }
</style>
""", unsafe_allow_html=True)

st.markdown(
    '<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" crossorigin="anonymous">',
    unsafe_allow_html=True,
)
st.markdown("""
    <style>
    .stApp, .main, .css-18e3th9, .css-1y0tads, main { padding: 0 !important; margin: 0 !important; }
    .css-18e3th9 { padding-top: 85px !important; margin-top: 0 !important; }
    header[data-testid="stHeader"], footer { display: none !important; }
    #MainMenu { visibility: hidden !important; }
    body, .stApp { background: #fff !important; }
    </style>
""", unsafe_allow_html=True)

include_html("header.html")

# ──────────────────────────── Auth Restore ────────────────────────────
try:
    from auth_utils import _read_auth_cookie, logout
except Exception:
    # Fallbacks if helpers aren't available
    def _read_auth_cookie():
        return {}
    def logout():
        # no-op
        pass

# Keep sensitive things like employee id in env
EMPLOYEE_ID = os.getenv("EMPLOYEE_ID", "101318").strip()

# Non-secret URLs: hard-code them
LOGIN_URL    = "/login"              # where to send users who aren't authorized
OPENINGS_URL = "/pages/opening.py"     # where to send authenticated non-employees

def _redirect_immediate(url: str):
    """Client-side redirect now, stop Streamlit execution."""
    st.markdown(
        f"""
        <script>
          window.location.replace("{url}");
        </script>
        """,
        unsafe_allow_html=True,
    )
    st.stop()

def _redirect_after(url: str, ms: int = 1000):
    """Client-side redirect after a brief delay, stop Streamlit execution."""
    st.markdown(
        f"""
        <script>
          setTimeout(function() {{
            window.location.replace("{url}");
          }}, {int(ms)});
        </script>
        """,
        unsafe_allow_html=True,
    )
    st.stop()

def ensure_auth_in_session():
    """Restore cookie-auth into session_state if present."""
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

ensure_auth_in_session()

user_email = (st.session_state.get("user_email") or "").strip()
membership_id = st.session_state.get("membership_id")
membership_id = (str(membership_id).strip() if membership_id is not None else "")

# Not logged in → bounce to login with ?next=<current>
if not user_email:
    # Build next= (current path + search)
    st.warning("You must be logged in to access this page. Click below to login again.")
    is_staging = os.getenv("ENVIRONMENT", "").lower() == "staging"
        
    if is_staging:
        login_url = "https://sip-portal-stg.coresight.com/"
    else:
        login_url = "https://sip-portal.coresight.com/"
    
        # Use st.link_button to open in new tab
    st.link_button(
            "Login",
            login_url,
            # type="tertiary",
            # icon=":material/help:"
        )

    st.markdown(
        """
        <script>
          (function(){
            const next = encodeURIComponent(window.location.pathname + window.location.search);
            window.location.replace("%s?next=" + next);
          })();
        </script>
        """ % LOGIN_URL,
        unsafe_allow_html=True,
    )
    st.stop()

# Logged in but not an employee → bounce to Openings after 1s
if membership_id != EMPLOYEE_ID:
    st.info("Access restricted to employees. Redirecting you…")
    # _redirect_after(OPENINGS_URL, ms=1000)
    st.switch_page("pages/opening.py")

# ──────────────────────────── Database Bootstrap ────────────────────────────
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
SSL_CA = os.getenv("SSL_CA")

def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    q = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = :t
          AND column_name = :c
        LIMIT 1
    """)
    return conn.execute(q, {"t": table_name, "c": column_name}).first() is not None

def _add_column_if_missing(conn, table: str, col_def_sql: str, col_name: str):
    if not _table_has_column(conn, table, col_name):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col_def_sql}"))

@st.cache_resource
def get_engine() -> Engine:
    eng = create_engine(
        f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}",
        connect_args={"ssl_ca": SSL_CA} if SSL_CA else {},
        pool_pre_ping=True,
    )
    with eng.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS release_cycle(
            id INT AUTO_INCREMENT PRIMARY KEY,
            year INT NOT NULL,
            month INT NOT NULL,
            kickoff_dt DATE,
            ingest_start_dt DATE,
            ingest_end_dt DATE,
            validation_start_dt DATE,
            validation_end_dt DATE,
            release_dt DATE,
            downtime_hours INT DEFAULT 2,
            status VARCHAR(32) DEFAULT 'planning',
            go_no_go VARCHAR(16) DEFAULT 'PENDING',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_ym (year, month)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS checklist_item(
            id INT AUTO_INCREMENT PRIMARY KEY,
            cycle_id INT NOT NULL,
            section VARCHAR(128) NOT NULL,
            title VARCHAR(255) NOT NULL,
            owner VARCHAR(255),
            due_dt DATE,
            done_bool TINYINT DEFAULT 0,
            done_at DATETIME NULL,
            notes TEXT,
            CONSTRAINT fk_ci_cycle FOREIGN KEY (cycle_id) REFERENCES release_cycle(id) ON DELETE CASCADE,
            KEY idx_ci_cycle_section (cycle_id, section)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS release_notes(
            id INT AUTO_INCREMENT PRIMARY KEY,
            cycle_id INT NOT NULL,
            section VARCHAR(128) NOT NULL,
            combined_notes LONGTEXT NOT NULL,
            updated_at DATETIME NOT NULL,
            CONSTRAINT fk_rn_cycle FOREIGN KEY (cycle_id) REFERENCES release_cycle(id) ON DELETE CASCADE,
            UNIQUE KEY uniq_cycle_section (cycle_id, section)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS section_contact(
            id INT AUTO_INCREMENT PRIMARY KEY,
            cycle_id INT NOT NULL,
            section VARCHAR(128) NOT NULL,
            contact_email VARCHAR(255) NOT NULL,
            updated_at DATETIME NOT NULL,
            CONSTRAINT fk_sc_cycle FOREIGN KEY (cycle_id) REFERENCES release_cycle(id) ON DELETE CASCADE,
            UNIQUE KEY uniq_sc (cycle_id, section)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))

        # additions
        _add_column_if_missing(conn, "release_cycle", "completed_bool TINYINT DEFAULT 0", "completed_bool")
        _add_column_if_missing(conn, "release_cycle", "completed_at DATETIME NULL", "completed_at")
    return eng

engine = get_engine()

# ──────────────────────────── Email (on save) ────────────────────────────
EMAIL_RECIPIENTS = ['ShashankGupta@coresight.com','MaxKahn@coresight.com','johnmercer@coresight.com', 'jtblubaugh@coresight.com','MohdSaeedAfri@coresight.com','vaishnavinayakk@coresight.com']
FROM_EMAIL = "dataautomation@coresight.com"
FROM_EMAIL_PASSWORD = "D)636612595014om"
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

def _uniq_emails(*lists):
    seen, out = set(), []
    for lst in lists:
        for e in lst or []:
            if not e:
                continue
            key = e.strip().lower()
            if key and key not in seen:
                seen.add(key); out.append(e.strip())
    return out

def send_step_email(year:int, month:int, section:str, contact_email:str, user_email:str, checked_titles:list, notes:str):
    try:
        recipients = _uniq_emails([contact_email], EMAIL_RECIPIENTS)
        if not recipients:
            return
        ym = f"{year}-{str(month).zfill(2)}"
        subject = f"[SIP Release] {ym} — '{section}' saved by {user_email}"
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        items_block = "<ul>" + "".join(f"<li>{t}</li>" for t in (checked_titles or [])) + "</ul>" if checked_titles else "<em>No items checked</em>"
        html = f"""
        <html><body>
          <p><strong>Release Cycle:</strong> {ym}<br/>
             <strong>Section:</strong> {section}<br/>
             <strong>Saved by:</strong> {user_email}<br/>
             <strong>When:</strong> {now}
          </p>
          <p><strong>Checked Items:</strong></p>
          {items_block}
          <p><strong>Combined Notes:</strong><br/>
          <pre style="white-space:pre-wrap;border:1px solid #ddd;padding:10px;border-radius:6px;">{notes}</pre></p>
        </body></html>
        """
        msg = MIMEMultipart("alternative")
        msg["From"] = FROM_EMAIL
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
            server.ehlo(); server.starttls(); server.login(FROM_EMAIL, FROM_EMAIL_PASSWORD)
            server.sendmail(FROM_EMAIL, recipients, msg.as_string())
    except Exception as e:
        st.warning(f"Saved, but email failed to send: {e}")

# ──────────────────────────── Date Helpers ────────────────────────────
def next_biz_day(d: date) -> date:
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d

def prev_biz_day(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d

def compute_dates(y: int, m: int):
    kickoff = next_biz_day(date(y, m, 15))
    ingest_start = next_biz_day(kickoff + timedelta(days=1))
    ingest_end = next_biz_day(ingest_start + timedelta(days=1))
    validation_start = next_biz_day(ingest_end + timedelta(days=1))
    release_dt = next_biz_day(date(y, m, 24))
    validation_end = prev_biz_day(release_dt - timedelta(days=1))
    return dict(
        kickoff=kickoff, ingest_start=ingest_start, ingest_end=ingest_end,
        validation_start=validation_start, validation_end=validation_end,
        release_dt=release_dt
    )

def fmt_d(d: date) -> str:
    return d.strftime("%b %d")

def section_single_date(section_key: str, dmap: dict) -> date:
    if section_key == "Step 1 (15th): Information Gathering":
        return dmap["kickoff"]
    if section_key == "Step 2 (16th-17th): Data Update":
        return dmap["ingest_end"]
    if section_key == "Step 3 (18th-23rd): Data Validation & App Testing":
        return dmap["validation_end"]
    if section_key == "Step 4 (24th) (next biz day): Release":
        return dmap["release_dt"]
    if section_key == "Step 5: Post-Release":
        return next_biz_day(dmap["release_dt"] + timedelta(days=1))
    return dmap["release_dt"]

# ──────────────────────────── Query Params / Cycle Picker ────────────────────────────
def qp_get_all():
    try:
        return dict(st.query_params)
    except Exception:
        return st.experimental_get_query_params()

def qp_set(**kwargs):
    try:
        for k, v in kwargs.items():
            st.query_params[k] = str(v)
    except Exception:
        st.experimental_set_query_params(**{k: str(v) for k, v in kwargs.items()})
try:
    cookie_controller = CookieController(key="auth_cookies")
except Exception:
    cookie_controller = {}

print(cookie_controller,"cookie_controller")
def load_auth_cookie():
        if not cookie_controller:
            return {}
        try:
            raw_cookie = cookie_controller.get("auth_data")
            if not raw_cookie:
                return {}
            if isinstance(raw_cookie, str):
                return json.loads(raw_cookie)
            if isinstance(raw_cookie, dict):
                return raw_cookie
        except Exception:
            return {}
        return {}

# --- add below your load_auth_cookie() ---
def save_auth_cookie(data: dict):
    """Persist auth_data back to the cookie."""
    try:
        cc = CookieController(key="auth_cookies")
        cc.set("auth_data", json.dumps(data), max_age_days=30, same_site="Lax")
    except Exception:
        pass

# Initialize cookie + filters dicts used by the buttons
auth_cookie = load_auth_cookie() or {}
user_filters = (auth_cookie.get("filters") or {})

col1,col3, col4, col5, col2 = st.columns([6.4, 1.5, 1.1,1,1], vertical_alignment="bottom")
with col1:
    st.subheader("Store Intelligence Platform - Monthly Release Portal")
    st.html("<style>[data-testid='stHeaderActionElements'] {display: none;}</style>")
# with col3:
#     if st.button("Back", key="back", type="tertiary", icon=":material/arrow_back:"):
#         from time import sleep
#         sleep(1)

#         try:
#             # Parse the cookie into a dict (handles str/dict/None safely)
#             auth_cookie = load_auth_cookie()
#         except Exception:
#             auth_cookie = {}
#         return_page = ((auth_cookie or {}).get("filters") or {}).get("returnPage")
#         if return_page:
#             st.switch_page(f"pages/{return_page}.py")
#         else:
#             st.switch_page("pages/opening.py")
with col3:
    if st.button("Missing Logs Upload", key="go_to_missing_logs",
                 type="tertiary", icon=":material/cloud_upload:"):
        # remember return page in the cookie
        user_filters["returnPage"] = "app_release_portal"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        st.switch_page("pages/missing_logs_upload.py")

with col4:
    if st.button("Edit Retailers", key="go_to_retailers_editing",
                 type="tertiary", icon=":material/edit:"):
        # remember return page in the cookie
        user_filters["returnPage"] = "app_release_portal"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        st.switch_page("pages/retailers_editing.py")

with col5:
    if st.button("Sync Data", key="go_to_promote_stg_to_prod",
                 type="tertiary", icon=":material/sync:"):
        # remember return page in the cookie
        user_filters["returnPage"] = "app_release_portal"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        st.switch_page("pages/sync_data.py")

with col2:
    if st.button("Log Out", key="Logout", type="tertiary",icon=":material/logout:"):
        logout()
        _redirect_immediate(LOGIN_URL)

if "__saved_section__" in st.session_state:
    st.toast(f"Saved: {st.session_state['__saved_section__']}")
    del st.session_state["__saved_section__"]

today = date.today()
qp = qp_get_all()

def _parse_qp_int(val, default):
    try:
        if isinstance(val, list):
            val = val[0] if val else None
        return int(val) if val is not None else default
    except Exception:
        return default

qp_year = _parse_qp_int(qp.get("y"), today.year)
qp_month = _parse_qp_int(qp.get("m"), today.month)

if "sel_year" not in st.session_state:
    st.session_state.sel_year = qp_year
if "sel_month" not in st.session_state:
    st.session_state.sel_month = qp_month

def _sync_qp():
    qp_set(y=st.session_state.sel_year, m=st.session_state.sel_month)

colA, colB, colC = st.columns(3)
colA.number_input("Year", step=1, key="sel_year", on_change=_sync_qp)
colB.number_input("Month", min_value=1, max_value=12, step=1, key="sel_month", on_change=_sync_qp)
month_num = int(st.session_state.get("sel_month", 1))
month_name = calendar.month_name[month_num]

# Show the month name nicely beside the number box
colC.markdown(
    f"<div style='margin-top:28px;padding:6px 10px;border:1px solid #ddd;border-radius:6px;"
    f"background:#f8f8f8;display:inline-block;'>{month_name}</div>",
    unsafe_allow_html=True,
)
year = int(st.session_state.sel_year)
month = int(st.session_state.sel_month)

dates = compute_dates(year, month)

# ──────────────────────────── Ensure Cycle Exists ────────────────────────────
with engine.begin() as conn:
    cycle = conn.execute(
        text("""SELECT id, downtime_hours, status, completed_bool, completed_at
                FROM release_cycle WHERE year=:y AND month=:m"""),
        {"y": year, "m": month}
    ).mappings().first()
    if not cycle:
        conn.execute(text("""
            INSERT INTO release_cycle
            (year, month, kickoff_dt, ingest_start_dt, ingest_end_dt, validation_start_dt, validation_end_dt, release_dt)
            VALUES (:y,:m,:k,:is,:ie,:vs,:ve,:r)
        """), {
            "y": year, "m": month,
            "k": dates["kickoff"].isoformat(),
            "is": dates["ingest_start"].isoformat(),
            "ie": dates["ingest_end"].isoformat(),
            "vs": dates["validation_start"].isoformat(),
            "ve": dates["validation_end"].isoformat(),
            "r": dates["release_dt"].isoformat()
        })
        cycle = conn.execute(
            text("""SELECT id, downtime_hours, status, completed_bool, completed_at
                    FROM release_cycle WHERE year=:y AND month=:m"""),
            {"y": year, "m": month}
        ).mappings().first()

cycle_id = cycle["id"]

# ──────────────────────────── Completion Logic ────────────────────────────
def recompute_cycle_completion(cycle_id: int):
    """A cycle is complete when every section has >=1 item checked. Stamp completed_at once."""
    with engine.begin() as conn:
        stats = conn.execute(text("""
            SELECT
              COUNT(DISTINCT section) AS total_sections,
              COUNT(DISTINCT CASE WHEN done_bool=1 THEN section END) AS sections_with_any_done
            FROM checklist_item
            WHERE cycle_id = :cid
        """), {"cid": cycle_id}).mappings().first()

        total_sections = int(stats["total_sections"] or 0)
        sections_with_any_done = int(stats["sections_with_any_done"] or 0)
        all_sections_covered = (total_sections > 0 and sections_with_any_done == total_sections)

        prior = conn.execute(text("""
            SELECT completed_bool FROM release_cycle WHERE id=:cid
        """), {"cid": cycle_id}).mappings().first()
        was_complete = bool(prior["completed_bool"]) if prior else False

        if all_sections_covered and not was_complete:
            # Transition to complete — set timestamp once
            conn.execute(text("""
                UPDATE release_cycle
                SET completed_bool = 1,
                    status = 'complete',
                    completed_at = UTC_TIMESTAMP()
                WHERE id = :cid
            """), {"cid": cycle_id})
        elif all_sections_covered and was_complete:
            # Already complete — keep timestamp stable
            conn.execute(text("""
                UPDATE release_cycle
                SET status = 'complete'
                WHERE id = :cid
            """), {"cid": cycle_id})
        else:
            # Not complete — allow status to reflect planning/in_progress; clear timestamp
            conn.execute(text("""
                UPDATE release_cycle
                SET completed_bool = 0,
                    status = CASE
                        WHEN :sections_with_any_done > 0 THEN 'in_progress'
                        ELSE 'planning'
                    END,
                    completed_at = NULL
                WHERE id = :cid
            """), {"cid": cycle_id, "sections_with_any_done": sections_with_any_done})

# Keep status fresh on load
recompute_cycle_completion(cycle_id)

with engine.begin() as conn:
    cyc_meta = conn.execute(text("""
        SELECT downtime_hours, status, completed_bool, completed_at
        FROM release_cycle WHERE id=:id
    """), {"id": cycle_id}).mappings().first()

completed_bool = bool(cyc_meta["completed_bool"])
completed_at = cyc_meta["completed_at"]
status_label = "Completed" if completed_bool else ("In Progress" if cyc_meta["status"] == "in_progress" else "Planning")

# ──────────────────────────── Header ────────────────────────────
hdr_l, hdr_r = st.columns([8, 2])
with hdr_l:
    st.subheader(f"Cycle {date(year, month, 1):%b %Y}")
with hdr_r:
    st.markdown(f"<h3 style='margin:0;'>Status: <span style='color:#d6262f;'>{status_label}</span></h3>", unsafe_allow_html=True)

m1, m2, m3, m4, m5 = st.columns([1, 1, 1, 1, 1.5])
m1.metric("Kickoff", fmt_d(dates["kickoff"]))
m2.metric("Ingestion", f"{fmt_d(dates['ingest_start'])}-{fmt_d(dates['ingest_end'])}")
m3.metric("Validation", f"{fmt_d(dates['validation_start'])}-{fmt_d(dates['validation_end'])}")
m4.metric("Release", fmt_d(dates["release_dt"]))
if completed_bool and completed_at:
    m5.metric("Completed At", str(completed_at))
else:
    with m5:
        st.empty()

# Downtime editor
dcol_num, dcol_status = st.columns([0.4, 0.6])
with engine.begin() as conn:
    current_downtime = conn.execute(
        text("SELECT downtime_hours FROM release_cycle WHERE id=:id"), {"id": cycle_id}
    ).scalar_one()

with dcol_num:
    st.markdown("**Downtime (hrs)**")
    new_downtime = st.number_input(
        "Downtime (hrs)",
        value=int(current_downtime or 2),
        min_value=0,
        max_value=6,
        key=f"downtime_{cycle_id}",
        label_visibility="collapsed",
        step=1,
    )

if int(new_downtime) != int(current_downtime or 0):
    with engine.begin() as conn:
        conn.execute(text("UPDATE release_cycle SET downtime_hours=:d WHERE id=:id"),
                     {"d": int(new_downtime), "id": cycle_id})
    with dcol_status:
        st.success("Downtime saved.", icon="💾")
else:
    with dcol_status:
        st.write("")

st.markdown("---")

# ──────────────────────────── Sections ────────────────────────────
SECTION_DEFS = [
    ("Step 1 (15th): Information Gathering",
     f"Step 1 ({fmt_d(dates['kickoff'])}): Information Gathering"),
    ("Step 2 (16th-17th): Data Update",
     f"Step 2 (ends {fmt_d(dates['ingest_end'])}): Data Update"),
    ("Step 3 (18th-23rd): Data Validation & App Testing",
     f"Step 3 (ends {fmt_d(dates['validation_end'])}): Data Validation & App Testing"),
    ("Step 4 (24th) (next biz day): Release",
     f"Step 4 ({fmt_d(dates['release_dt'])}): Release"),
    ("Step 5: Post-Release",
     f"Step 5 ({fmt_d(next_biz_day(dates['release_dt'] + timedelta(days=1)))}): Post-Release"),
]

SECTIONS_ITEMS = {
    "Step 1 (15th): Information Gathering": [
        "Freeze app updates (merge complete; staging stable)",
        "Collect data changes (if any)",
        "New retailers list ingested in ChainXY portal and SIP (if any)"
    ],
    "Step 2 (16th-17th): Data Update": [
        "ETL ingestion completed & data loaded to staging; logs reviewed",
    ],
    "Step 3 (18th-23rd): Data Validation & App Testing": [
        "Data quality validation",
        "Prepare unvalidated data spreadsheet for upload",
        "App updates testing",
        "Draft release notes (data + app)",
        "Client pre-notice (~20th) sent"
    ],
    "Step 4 (24th) (next biz day): Release": [
        "Downtime notice ≥24h in advance",
        "Promote data from staging to production",
        "Uploaded Unvalidated data",
        "Deploy app updates to production",
        "Internal + external release comms",
    ],
    "Step 5: Post-Release": [
        "Monitor metrics & tickets",
        "Hotfix critical issues if any",
        "Wrap-up comms & retrospective"
    ]
}

# Seed items & contacts
with engine.begin() as conn:
    for section_key, _title in SECTION_DEFS:
        step_dt = section_single_date(section_key, dates)
        for item_title in SECTIONS_ITEMS[section_key]:
            exists = conn.execute(text("""
                SELECT id FROM checklist_item WHERE cycle_id=:cid AND section=:s AND title=:t
            """), {"cid": cycle_id, "s": section_key, "t": item_title}).first()
            if not exists:
                conn.execute(text("""
                    INSERT INTO checklist_item (cycle_id, section, title, owner, due_dt)
                    VALUES (:cid, :s, :t, :owner, :due)
                """), {
                    "cid": cycle_id, "s": section_key, "t": item_title,
                    "owner": user_email,
                    "due": step_dt.isoformat()
                })
        sc = conn.execute(text("""
            SELECT contact_email FROM section_contact WHERE cycle_id=:cid AND section=:s
        """), {"cid": cycle_id, "s": section_key}).scalar()
        if not sc:
            conn.execute(text("""
                INSERT INTO section_contact(cycle_id, section, contact_email, updated_at)
                VALUES (:cid, :s, :e, :ts)
            """), {"cid": cycle_id, "s": section_key, "e": user_email,
                   "ts": datetime.utcnow().isoformat(sep=" ", timespec="seconds")})

# Render sections
for section_key, display_title in SECTION_DEFS:
    st.markdown(f"### {display_title}")
    step_date = section_single_date(section_key, dates)

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, title, done_bool
            FROM checklist_item
            WHERE cycle_id=:cid AND section=:s ORDER BY id
        """), {"cid": cycle_id, "s": section_key}).mappings().all()

        combined_notes = conn.execute(text("""
            SELECT combined_notes FROM release_notes WHERE cycle_id=:cid AND section=:s
        """), {"cid": cycle_id, "s": section_key}).scalar() or ""

        contact_email = conn.execute(text("""
            SELECT contact_email FROM section_contact WHERE cycle_id=:cid AND section=:s
        """), {"cid": cycle_id, "s": section_key}).scalar() or user_email

    section_locked = any(bool(r["done_bool"]) for r in rows) if rows else False

    with st.form(f"form_{cycle_id}_{section_key}", clear_on_submit=False):
        st.text_input("Step Date", value=fmt_d(step_date),
                      key=f"date_{cycle_id}_{section_key}", disabled=True)
        st.text_input(f"Section Completion User Email: {display_title}", value=contact_email,
                      key=f"contact_{cycle_id}_{section_key}", disabled=True)

        new_values = []
        for r in rows:
            cid = r["id"]
            cols = st.columns([1.0, 0.1])
            done_new = cols[0].checkbox(
                r["title"], value=bool(r["done_bool"]),
                key=f"done_{cycle_id}_{cid}", disabled=section_locked
            )
            cols[1].markdown("&nbsp;")
            new_values.append((cid, done_new))

        st.caption("Combined Notes (required). Copied into each item’s notes with a [Section: ...] prefix.")
        combined_notes_input = st.text_area(
            f"Notes: {display_title}",
            value=combined_notes,
            key=f"combined_{cycle_id}_{section_key}",
            height=160,
            placeholder="- What changed: ...\n- Risks / dependencies: ...\n- QA/Validation summary: ...\n- Comms prep: ...",
            disabled=section_locked
        )

        submitted = st.form_submit_button(f"Save {display_title}", disabled=section_locked)

    if submitted and not section_locked:
        any_checked_now = any(v for _, v in new_values)
        if not any_checked_now:
            st.error("Select at least one checkbox before saving this section.")
        elif not combined_notes_input.strip():
            st.error("Combined Notes are required for this section.")
        else:
            ts = datetime.utcnow().isoformat(sep=" ", timespec="seconds")
            checked_now_titles = [title for (_cid, val), title in zip(new_values, [r["title"] for r in rows]) if val]

            with engine.begin() as conn:
                for cid, done_new in new_values:
                    conn.execute(text("""
                        UPDATE checklist_item
                        SET done_bool=:done,
                            done_at=CASE WHEN :done=1 THEN :ts ELSE done_at END,
                            notes=:notes
                        WHERE id=:id
                    """), {
                        "done": 1 if done_new else 0,
                        "ts": ts,
                        "notes": f"[Section: {section_key}] {combined_notes_input}",
                        "id": cid
                    })
                conn.execute(text("""
                    INSERT INTO release_notes(cycle_id, section, combined_notes, updated_at)
                    VALUES (:cid, :s, :n, :ts)
                    ON DUPLICATE KEY UPDATE combined_notes=VALUES(combined_notes), updated_at=VALUES(updated_at)
                """), {"cid": cycle_id, "s": section_key, "n": combined_notes_input, "ts": ts})

            try:
                send_step_email(year, month, display_title, contact_email, user_email, checked_now_titles, combined_notes_input)
            except Exception as e:
                st.warning(f"Saved, but email failed to send: {e}")

            recompute_cycle_completion(cycle_id)
            st.session_state["__saved_section__"] = display_title
            st.rerun()

    if section_locked:
        st.info("This section is completed and is read only.")

st.markdown("---")

# ──────────────────────────── Release History ────────────────────────────
st.markdown("## Release History")
with st.expander("Show all previous release cycles and notes", expanded=False):
    with engine.begin() as conn:
        cycles = conn.execute(text("""
            SELECT id, year, month, kickoff_dt, ingest_start_dt, ingest_end_dt,
                   validation_start_dt, validation_end_dt, release_dt,
                   downtime_hours, status, completed_bool, completed_at
            FROM release_cycle
            WHERE NOT (year=:y AND month=:m)
            ORDER BY year DESC, month DESC
        """), {"y": year, "m": month}).mappings().all()

    if not cycles:
        st.info("No previous cycles found.")
    else:
        for c in cycles:
            cid = c["id"]
            with engine.begin() as conn:
                stats = conn.execute(text("""
                    SELECT
                      COUNT(DISTINCT section) AS total_sections,
                      COUNT(DISTINCT CASE WHEN done_bool=1 THEN section END) AS sections_with_any_done
                    FROM checklist_item
                    WHERE cycle_id = :cid
                """), {"cid": cid}).mappings().first()

                notes = conn.execute(text("""
                    SELECT section, combined_notes, updated_at
                    FROM release_notes WHERE cycle_id=:cid ORDER BY section
                """), {"cid": cid}).mappings().all()

                contacts = conn.execute(text("""
                    SELECT section, contact_email, updated_at
                    FROM section_contact WHERE cycle_id=:cid ORDER BY section
                """), {"cid": cid}).mappings().all()

                items = conn.execute(text("""
                    SELECT section, title, done_bool, done_at
                    FROM checklist_item WHERE cycle_id=:cid ORDER BY section, id
                """), {"cid": cid}).mappings().all()

            total_sections = int(stats["total_sections"] or 0)
            sections_with_any_done = int(stats["sections_with_any_done"] or 0)
            all_sections_covered = (total_sections > 0 and sections_with_any_done == total_sections)

            if all_sections_covered:
                hist_status = "Completed"
            elif sections_with_any_done > 0:
                hist_status = "In Progress"
            else:
                hist_status = "Planning"

            comp_when = f" | Completed At: {c['completed_at']}" if all_sections_covered and c["completed_at"] else ""
            coverage = f" (Sections covered: {sections_with_any_done}/{total_sections})" if total_sections else ""

            st.markdown(
                f"### {c['year']}-{str(c['month']).zfill(2)} — "
                f"Status: **{hist_status}** — Downtime (hrs): **{c['downtime_hours']}**{comp_when}{coverage}  \n"
                f"Kickoff: {c['kickoff_dt']} | "
                f"Ingest (start–end): {c['ingest_start_dt']}–{c['ingest_end_dt']} | "
                f"Validation (start–end): {c['validation_start_dt']}–{c['validation_end_dt']} | "
                f"Release: {c['release_dt']}"
            )

            contact_map = {x["section"]: x["contact_email"] for x in contacts} if contacts else {}

            if notes:
                st.markdown("**Section Notes & Contacts:**")
                for n in notes:
                    contact = contact_map.get(n["section"], "—")
                    st.markdown(f"**{n['section']}**  \n_Contact: {contact} — Last updated: {n['updated_at']}_")
                    st.code(n["combined_notes"] or "", language="markdown")

            if items:
                st.markdown("**Checklist Summary:**")
                current_section = None
                overall_completed = (hist_status == "Completed")
                for it in items:
                    if it["section"] != current_section:
                        current_section = it["section"]
                        st.markdown(f"- **{current_section}**")
                    status_str = (
                        "✅ Done"
                        if it["done_bool"]
                        else ("❌ Not selected" if overall_completed else "⏳ Pending")
                    )
                    st.markdown(f"  - {status_str} — {it['title']} (Done at: {it['done_at'] or '-'})")
