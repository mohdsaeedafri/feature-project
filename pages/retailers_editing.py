# pages/edit_parent_chain_names_data.py
import os, json
from datetime import datetime, date
import numpy as np
import pandas as pd
import streamlit as st
from html_utils import include_html
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ✉️ email imports
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication  # <-- for attachments

try:
    from streamlit_cookies_controller import CookieController
except Exception:
    CookieController = None

# ──────────────────────────── Boot ────────────────────────────
load_dotenv()
st.set_page_config(
    page_title="Retailers Table",
    page_icon="https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
    layout="wide",
)

IS_STAGING = os.getenv("ENVIRONMENT", "").lower() == "staging"
STAGING_BASE = "https://sip-portal-stg.coresight.com"
PROD_BASE = "https://sip-portal.coresight.com"
CURRENT_PATH = "/retailers_editing"
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

# ──────────────────────────── Auth (employee-only) ────────────────────────────
try:
    from auth_utils import logout as _shared_logout
except Exception:
    _shared_logout = None

LOGIN_URL = "/login"
EMPLOYEE_ID = os.getenv("EMPLOYEE_ID", "101318").strip()

def _redirect_immediate(url: str):
    st.markdown(f"""<script>window.location.replace("{url}");</script>""", unsafe_allow_html=True)
    st.stop()

def logout():
    if _shared_logout:
        _shared_logout()
    else:
        try:
            if CookieController:
                CookieController(key="auth_cookies").remove("auth_data")
        except Exception:
            pass
        for k in list(st.session_state.keys()):
            del st.session_state[k]

def _read_auth_cookie() -> dict:
    try:
        if not CookieController:
            return {}
        cc = CookieController(key="auth_cookies")
        raw = cc.get("auth_data")
        if not raw:
            return {}
        if isinstance(raw, str):  return json.loads(raw)
        if isinstance(raw, dict): return raw
        return {}
    except Exception:
        return {}

def ensure_auth_in_session():
    if st.session_state.get("user_email"):
        return
    ck = _read_auth_cookie() or {}
    if not ck:
        return
    st.session_state["session_id"] = ck.get("session_id")
    st.session_state["user_email"] = ck.get("user_email")
    st.session_state["token"] = ck.get("token")
    st.session_state["membership"] = ck.get("membership") or {}
    st.session_state["membership_id"] = ck.get("membership_id")
    st.session_state["membership_type"] = ck.get("membership_type")

def _login_link_url():
    is_staging = os.getenv("ENVIRONMENT", "").lower() == "staging"
    return "https://sip-portal-stg.coresight.com/" if is_staging else "https://sip-portal.coresight.com/"

ensure_auth_in_session()
user_email = (st.session_state.get("user_email") or "").strip()
membership_id = str(st.session_state.get("membership_id") or "").strip()

if not user_email:
    st.warning("You must be logged in to access this page.")
    st.link_button("Login", _login_link_url())
    st.markdown(
        f"""
        <script>
          (function(){{
            const next = encodeURIComponent(window.location.pathname + window.location.search);
            window.location.replace("{LOGIN_URL}?next=" + next);
          }})();
        </script>
        """,
        unsafe_allow_html=True,
    )
    st.stop()

if membership_id != EMPLOYEE_ID:
    st.info("Access restricted to employees. Redirecting you…")
    st.switch_page("pages/opening.py")

st.markdown(
    """
    <style>
    .stApp, .main, .css-18e3th9, .css-1y0tads, main { padding: 0 !important; margin: 0 !important; }
    .css-18e3th9 { padding-top: 85px !important; margin-top: 0 !important; }
    header[data-testid="stHeader"], footer { display: none !important; }
    #MainMenu { visibility: hidden !important; }
    body, .stApp { background: #fff !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

include_html("header.html")

# ──────────────────────────── DB ────────────────────────────
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
SSL_CA = os.getenv("SSL_CA")

# ✉️ email settings
EMAIL_RECIPIENTS = [
    "ShashankGupta@coresight.com",
    'johnmercer@coresight.com',
    'MohdSaeedAfri@coresight.com',
    'vaishnavinayakk@coresight.com', 
    'dataautomation@coresight.com'
]
FROM_EMAIL = os.getenv("FROM_EMAIL", "dataautomation@coresight.com")
FROM_EMAIL_PASSWORD = os.getenv("FROM_EMAIL_PASSWORD") or "D)636612595014om"  # prefer env-only
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

def make_engine(host, db, user, pwd, ssl_ca=None):
    return create_engine(
        f"mysql+mysqlconnector://{user}:{pwd}@{host}/{db}",
        connect_args={"ssl_ca": ssl_ca} if ssl_ca else {},
        pool_pre_ping=True,
    )

engine = make_engine(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, SSL_CA)
TABLE = "parent_chain_names_data"

# 🔎 NEW: transactional audit table (one row per changed parent row)
AUDIT_TABLE = "retailers_edit_audit_log"

try:
    cookie_controller = CookieController(key="auth_cookies")
except Exception:
    cookie_controller = {}

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

def save_auth_cookie(data: dict):
    try:
        cc = CookieController(key="auth_cookies")
        cc.set("auth_data", json.dumps(data), max_age_days=30, same_site="Lax")
    except Exception:
        pass

auth_cookie = load_auth_cookie() or {}
user_filters = (auth_cookie.get("filters") or {})

# ──────────────────────────── Schema-aware typing ────────────────────────────
BIGINT_COLS = ("Chain_ID", "ChainID_ChainXY")  # parent-side numeric-ish cols
DATETIME_COLS = ("LastRunDate_ChainXY", "FirstAppearedDate_ChainXY")  # DATETIME
DATE_AS_TEXT_COLS = ("FirstAppearedDate_Coresight",)  # TEXT date
BOOL_BINARY_COLS = ()
  # BINARY(1) (if present)
TEXT_LIKE_COLS = (
    "ChainName_Coresight",
    "ParentName_Coresight",
    "ChainName_ChainXY",
    "Sector_Coresight",
    "UpdateCycle_ChainXY",
    "Average_Square_Footage",
)

# UI helpers
BOOLEAN_COLUMNS = ("is_active",)
ID_LIKE_COLUMNS = ("Chain_ID",)  # canonical PK in the parent

# Columns in the parent that must be view-only (no edits) and never cascade to children
READONLY_PARENT_COLS = (
    "Chain_ID",   # ← example; add more below as needed
    "LastRunDate_ChainXY",
    "FirstAppearedDate_ChainXY",
    "FirstAppearedDate_Coresight"
)

# ──────────────────────────── Child tables config ────────────────────────────
DEPENDENCY_TABLES = [
    "all_opened_py",
    "all_opened_cy",
    "all_opened_log",
    "all_closed_py",
    "all_closed_cy",
    "all_closed_log",
    "data_change_log",
    "all_active_py",
    "all_active_cy",
]
# parent PK and child FK column names
PARENT_PK_COL = "Chain_ID"
CHILD_ID_COL  = "chainid_coresight"  # <-- kept for backwards-compat in some places (we won't delete it)

# ──────────────────────────── Column name mapping (parent → child synonyms) ────────────────────────────
# Keys are parent column names; values are lists of acceptable child column names (first match wins).
COLUMN_SYNONYMS: dict[str, list[str]] = {
    "UpdateCycle_ChainXY": ["updatecycle", "UpdateCycle"],
    "ChainName_ChainXY": ["ChainName"],
    "ChainID_ChainXY": ["ChainId"],
    
    # add more as needed:
    # "Average_Square_Footage": ["avg_sqft", "AverageSqFt"],
    # "Sector_Coresight": ["sector", "Sector"],
    # "ChainName_Coresight": ["chainname_coresight", "ChainName"],
}

# ──────────────────────────── Type coercion helpers ────────────────────────────
def _parse_to_timestamp_or_na(v):
    """
    Smart parser:
    - returns NaT on nulls
    - detects UNIX epoch in ms (12–15 digits), s (10 digits), and ns (>=18 digits)
    - otherwise falls back to pandas' parser
    """
    # null-ish
    if v is None or v is pd.NA or v is pd.NaT:
        return pd.NaT
    if isinstance(v, float) and pd.isna(v):
        return pd.NaT

    # numeric-like handling
    try:
        if isinstance(v, (int, np.integer, float, np.floating)) and not pd.isna(v):
            n = int(v)
            d = len(str(abs(n)))
            if 12 <= d <= 15:
                # milliseconds
                return pd.to_datetime(n, unit="ms")
            if d == 10:
                # seconds
                return pd.to_datetime(n, unit="s")
            if d >= 18:
                # nanoseconds
                return pd.to_datetime(n, unit="ns")
    except Exception:
        pass

    try:
        return pd.to_datetime(v)
    except Exception:
        return pd.NaT

def _to_sql_scalar_generic(v):
    if v is None or v is pd.NA or v is pd.NaT:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (bytes, bytearray)):
        return v
    return v

def _to_sql_datetime(v):
    if v is None or v is pd.NaT or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.datetime64):
        return pd.Timestamp(v).to_pydatetime()
    if isinstance(v, datetime):
        return v
    try:
        return pd.to_datetime(v).to_pydatetime()
    except Exception:
        return None

def _to_sql_date_text(v):
    if v is None or v is pd.NaT or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.date().isoformat()
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    try:
        return pd.to_datetime(v).date().isoformat()
    except Exception:
        return str(v)

def _to_sql_binary_bool(v):
    if v is None or v is pd.NA:
        return None
    if isinstance(v, (bytes, bytearray)):
        if v in (b"\x00", b"\x01"):
            return bytes(v)
        try:
            s = v.decode("utf-8", "ignore").strip()
            return b"\x01" if s in ("1", "true", "t", "yes", "y") else b"\x00"
        except Exception:
            return b"\x00"
    if isinstance(v, (bool, np.bool_)):
        return b"\x01" if bool(v) else b"\x00"
    try:
        return b"\x01" if int(v) == 1 else b"\x00"
    except Exception:
        return None

def coerce_row_for_db(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        if k in BIGINT_COLS:
            vv = _to_sql_scalar_generic(v)
            out[k] = int(vv) if vv is not None else None
        elif k in DATETIME_COLS:
            out[k] = _to_sql_datetime(v)
        elif k in DATE_AS_TEXT_COLS:
            out[k] = _to_sql_date_text(v)
        elif k in BOOL_BINARY_COLS:
            out[k] = _to_sql_binary_bool(v)
        else:
            vv = _to_sql_scalar_generic(v)
            if k in TEXT_LIKE_COLS:
                out[k] = None if vv is None else str(vv)
            else:
                out[k] = vv
    return out

def _binary_to_int01_ui(v):
    if v is None or v is pd.NA or v is pd.NaT:
        return pd.NA
    if isinstance(v, (bytes, bytearray)):
        if v in (b"\x01", b"1"): return 1
        if v in (b"\x00", b"0"): return 0
        try:
            s = v.decode("utf-8", "ignore").strip()
            if s == "1": return 1
            if s == "0": return 0
        except Exception:
            return pd.NA
        return pd.NA
    if isinstance(v, (bool, np.bool_)):
        return 1 if bool(v) else 0
    try:
        n = int(v)
        if n in (0, 1): return n
    except Exception:
        pass
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "t", "yes", "y"): return 1
        if s in ("0", "false", "f", "no", "n"): return 0
    return pd.NA

def _normalize_for_compare(v):
    if v is None or v is pd.NA or v is pd.NaT:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, (bool, np.bool_)):
        return 1 if bool(v) else 0
    if isinstance(v, (bytes, bytearray)):
        if v in (b"\x00", b"0"): return 0
        if v in (b"\x01", b"1"): return 1
        try:
            s = v.decode("utf-8", "ignore").strip().lower()
            if s in ("0", "false", "f", "no", "n"): return 0
            if s in ("1", "true", "t", "yes", "y"): return 1
        except Exception:
            pass
        return None
    if isinstance(v, pd.Timestamp):
        return None if v is pd.NaT else v.normalize().date().isoformat()
    if isinstance(v, np.datetime64):
        return pd.Timestamp(v).normalize().date().isoformat()
    if isinstance(v, (datetime, date)):
        return (v.date() if isinstance(v, datetime) else v).isoformat()
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return None if pd.isna(v) else float(v)
    return v

def _format_for_email(v):
    if v is None:
        return "—"
    if isinstance(v, (bool, np.bool_)):
        return "1" if v else "0"
    if isinstance(v, (bytes, bytearray)):
        return "1" if v in (b"\x01", b"1") else "0"
    if isinstance(v, pd.Timestamp):
        return "—" if v is pd.NaT else v.date().isoformat()
    if isinstance(v, (datetime, date)):
        return v.date().isoformat() if isinstance(v, datetime) else v.isoformat()
    return str(v)

# ──────────────────────────── Transactional audit helpers ────────────────────────────
def _ensure_audit_table(conn):
    # Transactional, per-row audit with JSON before/after
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS `{AUDIT_TABLE}` (
            `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
            `batch_id` VARCHAR(64) NOT NULL,
            `table_name` VARCHAR(255) NOT NULL,
            `user_email` VARCHAR(255),
            `action_type` ENUM('INSERT','UPDATE','DELETE') NOT NULL,
            `pk_name` VARCHAR(128) NOT NULL,
            `pk_value` VARCHAR(128) NOT NULL,
            `changed_columns` TEXT,
            `before_change` JSON NULL,
            `after_change` JSON NULL,
            `data_insert_timestamp` DATETIME NOT NULL,
            INDEX (`batch_id`),
            INDEX (`table_name`),
            INDEX (`action_type`),
            INDEX (`pk_name`, `pk_value`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """))

def _json_serialize_value(v):
    # Make JSON-safe
    if v is None or v is pd.NA:
        return None
    if isinstance(v, (pd.Timestamp, datetime)):
        if pd.isna(v) if isinstance(v, pd.Timestamp) else False:
            return None
        # naive isoformat (no tz)
        return (v.to_pydatetime() if isinstance(v, pd.Timestamp) else v).isoformat(sep=' ')
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return None if pd.isna(v) else float(v)
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8", "ignore")
        except Exception:
            return str(v)
    return v

def _row_to_jsonable_dict(row_like: dict, only_cols: list[str] | None = None) -> dict:
    out = {}
    cols = (only_cols or list(row_like.keys()))
    for c in cols:
        out[c] = _json_serialize_value(row_like.get(c, None))
    return out

def _insert_audit_rows(conn, batch_id: str, action_type: str, pk_name: str, pk_value, *,
                       before_dict: dict | None, after_dict: dict | None,
                       changed_cols: list[str] | None, when_utc: datetime):
    _ensure_audit_table(conn)
    conn.execute(
        text(f"""
            INSERT INTO `{AUDIT_TABLE}`
            (batch_id, table_name, user_email, action_type, pk_name, pk_value,
             changed_columns, before_change, after_change, data_insert_timestamp)
            VALUES
            (:batch_id, :table_name, :user_email, :action_type, :pk_name, :pk_value,
             :changed_columns, CAST(:before_change AS JSON), CAST(:after_change AS JSON), :ts)
        """),
        {
            "batch_id": batch_id,
            "table_name": TABLE,
            "user_email": user_email or "",
            "action_type": action_type,
            "pk_name": pk_name,
            "pk_value": str(pk_value),
            "changed_columns": ",".join(changed_cols or []),
            "before_change": json.dumps(before_dict or {}, ensure_ascii=False),
            "after_change": json.dumps(after_dict or {}, ensure_ascii=False),
            "ts": when_utc,
        },
    )

# ──────────────────────────── Load ────────────────────────────
@st.cache_data(show_spinner=False, ttl=60)
def load_table() -> pd.DataFrame:
    df = pd.read_sql(text(f"SELECT * FROM {TABLE}"), engine)

    # bytes -> string for object cols
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].map(lambda x: x.decode("utf-8", "ignore") if isinstance(x, (bytes, bytearray)) else x)

    # BIGINT -> Int64
    for c in BIGINT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # DATETIME -> pandas datetime (smart: handles ms/s/ns epochs)
    for c in DATETIME_COLS:
        if c in df.columns:
            df[c] = df[c].map(_parse_to_timestamp_or_na)

    # DATE_AS_TEXT -> parse to Timestamp for UI
    for c in DATE_AS_TEXT_COLS:
        if c in df.columns:
            df[c] = df[c].map(_parse_to_timestamp_or_na)

    # Additional safeguard: convert any numeric columns that look like UNIX ms to datetime
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c].dtype):
            s = df[c].dropna().astype(str)
            if len(s) > 0 and s.str.match(r"^\d{12,15}$").all():
                df[c] = pd.to_datetime(df[c], unit="ms", errors="coerce")

    # BINARY(1) -> Int only 0/1 in UI
    for c in BOOL_BINARY_COLS:
        if c in df.columns:
            df[c] = df[c].map(_binary_to_int01_ui).fillna(0).astype(int)

    return df

with st.spinner("Loading table…"):
    df_from_db = load_table()

if "edit_df" not in st.session_state:
    st.session_state["edit_df"] = df_from_db.copy()

# ──────────────────────────── UI Header ────────────────────────────
hh1, hh2, hh3, hh4, hh5 = st.columns([6.4, 1.5, 1.6, 1, 1], vertical_alignment="bottom")
with hh1:
    st.subheader("Retailers Table (Staging)")
with hh2:
    if st.button("App Release Portal", type="tertiary", icon=":material/release_alert:"):
        user_filters["returnPage"] = "retailers_editing"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        st.switch_page("pages/app_release_portal.py")
with hh3:
    if st.button("Missing Logs Upload", key="go_to_missing_logs", type="tertiary", icon=":material/cloud_upload:"):
        user_filters["returnPage"] = "retailers_editing"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        st.switch_page("pages/missing_logs_upload.py")
with hh4:
    if st.button("Sync Data", key="go_to_promote_stg_to_prod",
                 type="tertiary", icon=":material/sync:"):
        # remember return page in the cookie
        user_filters["returnPage"] = "retailers_editing"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        st.switch_page("pages/sync_data.py")
with hh5:
    if st.button("Log Out", type="tertiary", icon=":material/logout:"):
        logout()
        _redirect_immediate(LOGIN_URL)

# ──────────────────────────── Editor (stateful, in-place apply) ────────────────────────────
def _apply_editor_changes():
    changes = st.session_state.get("pcnd_editor", {})
    df = st.session_state["edit_df"]

    # Cell edits
    for r_idx, edits in (changes.get("edited_rows") or {}).items():
        for col, newval in edits.items():
            # 🚫 Guard: ignore modifications to read-only parent columns
            if col in READONLY_PARENT_COLS:
                continue
            row_i = int(r_idx)
            if col in BOOLEAN_COLUMNS:
                newval = 1 if str(newval) == "1" else 0
            df.at[row_i, col] = newval

    # Adds
    for newrow in (changes.get("added_rows") or []):
        row = {c: newrow.get(c, None) for c in df.columns}
        for bcol in BOOLEAN_COLUMNS:
            if row.get(bcol) is None or pd.isna(row.get(bcol)):
                row[bcol] = 0
        df.loc[len(df)] = row

    # Deletes
    to_drop = changes.get("deleted_rows") or []
    if to_drop:
        df.drop(index=to_drop, inplace=True)
        df.reset_index(drop=True, inplace=True)

# Build a VIEW DF for the editor ONLY:
# For read-only date/datetime columns, show formatted strings so Streamlit doesn't coerce them to integers.
df_for_editor = st.session_state["edit_df"].copy()
_readonly_date_cols = [c for c in READONLY_PARENT_COLS if c in df_for_editor.columns and (c in DATETIME_COLS or c in DATE_AS_TEXT_COLS or str(c).lower().endswith(("date", "timestamp")))]
for c in _readonly_date_cols:
    # try to format as datetime first; if parsing fails, fall back to original string
    try:
        ser = pd.to_datetime(df_for_editor[c], errors="coerce")
        # choose a readable format: time if in DATETIME_COLS, else just date
        fmt = "%Y-%m-%d %H:%M:%S" if c in DATETIME_COLS else "%Y-%m-%d"
        df_for_editor[c] = np.where(ser.notna(), ser.dt.strftime(fmt), df_for_editor[c].astype("string").fillna(""))
    except Exception:
        df_for_editor[c] = df_for_editor[c].astype("string").fillna("")

# Build column config once
cfg = {}
for c in st.session_state["edit_df"].columns:
    if c in ID_LIKE_COLUMNS:
        cfg[c] = st.column_config.NumberColumn(c, disabled=True)
    elif c in BOOLEAN_COLUMNS:
        cfg[c] = st.column_config.SelectboxColumn(c, options=[0, 1], default=0, required=True)
    elif c in READONLY_PARENT_COLS:
        # Read-only in UI; shown as text (already formatted in df_for_editor if date-like)
        cfg[c] = st.column_config.TextColumn(c, disabled=True)
    elif c in DATETIME_COLS:
        cfg[c] = st.column_config.DatetimeColumn(c, format="YYYY-MM-DD HH:mm:ss", step="second")
    elif c in DATE_AS_TEXT_COLS:
        cfg[c] = st.column_config.DatetimeColumn(c, format="YYYY-MM-DD", step="day")

st.data_editor(
    df_for_editor,  # <— use the formatted view, NOT the underlying state df
    key="pcnd_editor",
    column_config=cfg or None,
    num_rows="dynamic",
    use_container_width=True,
    on_change=_apply_editor_changes,
)

# ──────────────────────────── Diff & Save ────────────────────────────
def diff_with_pk(before: pd.DataFrame, after: pd.DataFrame, pk: str = None):
    pk_candidates = [c for c in ID_LIKE_COLUMNS if c in before.columns and c in after.columns]
    pk = pk or (pk_candidates[0] if pk_candidates else None)
    if not pk:
        return None, None, None, None

    b = before.copy(); a = after.copy()
    b[pk] = pd.to_numeric(b[pk], errors="coerce").astype("Int64")
    a[pk] = pd.to_numeric(a[pk], errors="coerce").astype("Int64")

    b_ids = set(b[pk].dropna().astype(int).tolist())
    a_ids = set(a[pk].dropna().astype(int).tolist())
    deleted = b_ids - a_ids
    shared  = b_ids & a_ids
    inserts = a[a[pk].isna()].copy()

    # Exclude read-only parent cols from triggering updates
    compare_cols = [c for c in a.columns if c != pk and c not in READONLY_PARENT_COLS]
    upd = []
    a_idx = a.set_index(pk, drop=False)
    b_idx = b.set_index(pk, drop=False)

    def _norm(x): return _normalize_for_compare(x)

    for i in sorted(shared):
        try:
            arow = a_idx.loc[i, compare_cols]
            brow = b_idx.loc[i, compare_cols]
        except KeyError:
            continue

        changed = {}
        for c in compare_cols:
            if _norm(arow[c]) != _norm(brow[c]):
                changed[c] = arow[c]

        if changed:
            changed[pk] = int(i)
            upd.append(changed)

    return inserts, upd, deleted, pk

# ──────────────────────────── Child cascade helpers (multi-FK aware) ────────────────────────────
_TABLE_COLUMNS_CACHE: dict[str, set[str]] = {}

def _get_child_columns(conn, table_name: str) -> set:
    if table_name in _TABLE_COLUMNS_CACHE:
        return _TABLE_COLUMNS_CACHE[table_name]
    cols = set()
    try:
        res = conn.execute(text(f"SHOW COLUMNS FROM `{table_name}`"))
        for row in res.fetchall():
            cols.add(row[0])
    except Exception:
        pass
    _TABLE_COLUMNS_CACHE[table_name] = cols
    return cols

def _lookup_before_row(before_df: pd.DataFrame, pk: str, rid: int) -> dict:
    try:
        row = before_df.set_index(pk, drop=False).loc[rid]
        if isinstance(row, pd.Series):
            return row.to_dict()
        return dict(row)
    except Exception:
        return {}

# NEW: list *all* FK columns we support for direct ID matching
def _list_child_fk_cols(child_cols: set) -> list[str]:
    """Return all FK columns present in the child in a stable order."""
    candidates = ["chainid_coresight", "ChainID_Coresight"]
    return [c for c in candidates if c in child_cols]

def _preupdate_child_ids_if_parent_id_changed(
    updates: list[dict], before_df: pd.DataFrame, conn, pk: str = PARENT_PK_COL
) -> int:
    """
    If Chain_ID changed in parent, retarget ALL FK columns that exist in each child
    (chainid_coresight, ChainID_Coresight).
    Returns number of child rows affected.
    """
    if not updates:
        return 0
    affected = 0
    for newrow in updates:
        if pk not in newrow:
            continue
        new_id = newrow.get(pk)
        if new_id is None or (isinstance(new_id, float) and pd.isna(new_id)):
            continue

        try:
            rid = int(newrow[pk])
        except Exception:
            rid = newrow[pk]

        before_row = _lookup_before_row(before_df, pk, rid)
        old_id = before_row.get(pk, None)
        id_changed = (_normalize_for_compare(old_id) != _normalize_for_compare(new_id))
        if not id_changed or old_id is None or new_id is None:
            continue

        try: old_id = int(old_id)
        except Exception: pass
        try: new_id = int(new_id)
        except Exception: pass

        for child_tbl in DEPENDENCY_TABLES:
            child_cols = _get_child_columns(conn, child_tbl)
            fk_cols = _list_child_fk_cols(child_cols)
            if not fk_cols:
                st.info(f"[skip id-retarget] {child_tbl}: no FK among chainid_coresight/ChainID_Coresight")
                continue

            for fk_col in fk_cols:
                result = conn.execute(
                    text(f"UPDATE `{child_tbl}` SET `{fk_col}`=:new_id WHERE `{fk_col}`=:old_id"),
                    {"old_id": old_id, "new_id": new_id},
                )
                st.info(f"[id-retarget] {child_tbl}.{fk_col}: {result.rowcount or 0} rows")
                affected += result.rowcount or 0
    return affected

def _resolve_child_set_map(
    parent_changed_cols: list[str],
    child_cols: set[str],
    fk_cols: list[str],
) -> dict[str, str]:
    """
    Return a mapping {parent_col -> child_col} for columns that should be SET in the child.
    - Prefers exact name match
    - Otherwise tries COLUMN_SYNONYMS[parent_col] in order
    - Excludes any target that is an FK column
    """
    set_map: dict[str, str] = {}
    for pcol in parent_changed_cols:
        # 1) exact same name present in child?
        if pcol in child_cols and pcol not in fk_cols:
            set_map[pcol] = pcol
            continue

        # 2) try synonyms
        for alias in COLUMN_SYNONYMS.get(pcol, []):
            if alias in child_cols and alias not in fk_cols:
                set_map[pcol] = alias
                break
    return set_map

def _propagate_updates_to_children(
    updates: list[dict], before_df: pd.DataFrame, conn, pk: str = PARENT_PK_COL
) -> int:
    """
    Push changed/denormalized columns to child tables using:
      - FK match on chainid_coresight/ChainID_Coresight = parent Chain_ID
      - OR (child.ChainName_Coresight IS NULL AND child.chain_id/Chain_ID = parent ChainID_ChainXY)
    Never overwrite FK columns themselves.
    """
    if not updates:
        return 0

    total_child_updates = 0
    for newrow in updates:
        if pk not in newrow or newrow[pk] is None or (isinstance(newrow[pk], float) and pd.isna(newrow[pk])):
            continue

        try:
            rid = int(newrow[pk])  # parent Chain_ID (coresight id)
        except Exception:
            rid = newrow[pk]

        # we need parent ChainID_ChainXY too (may be unchanged, so fetch from before if not in newrow)
        before_row = _lookup_before_row(before_df, pk, rid)
        chainxy = newrow.get("ChainID_ChainXY", before_row.get("ChainID_ChainXY") if before_row else None)

        # Exclude read-only parent columns from propagation
        changed_cols_parent = [c for c in newrow.keys() if c != pk and c not in READONLY_PARENT_COLS]

        for child_tbl in DEPENDENCY_TABLES:
            child_cols = _get_child_columns(conn, child_tbl)
            if not child_cols:
                st.info(f"[skip cascade] {child_tbl}: could not read columns")
                continue

            fk_cols = _list_child_fk_cols(child_cols)

            # Update mapping from parent->child columns (supports synonyms; excludes FKs)
            set_map = _resolve_child_set_map(changed_cols_parent, child_cols, fk_cols)

            if not set_map:
                st.info(f"[skip cascade] {child_tbl}: no matching changed columns {changed_cols_parent}")
                continue

            # Build WHERE:
            # 1) FK match: (chainid_coresight=:rid OR ChainID_Coresight=:rid)
            where_parts = []
            params = {}

            if fk_cols:
                fk_or = " OR ".join(f"`{fk}`=:rid" for fk in fk_cols)
                where_parts.append(f"({fk_or})")
                params["rid"] = rid

            # 2) Fallback: if child has ChainName_Coresight AND it's NULL, match child chain_id/Chain_ID to parent ChainID_ChainXY
            chainname_col_present = "ChainName_Coresight" in child_cols
            chain_id_variants = [c for c in ("chain_id", "Chain_ID") if c in child_cols]
            if chainname_col_present and chain_id_variants and chainxy is not None and not (isinstance(chainxy, float) and pd.isna(chainxy)):
                id_or = " OR ".join(f"`{c}`=:px_chainxy" for c in chain_id_variants)
                where_parts.append(f"(`ChainName_Coresight` IS NULL AND ({id_or}))")
                try:
                    params["px_chainxy"] = int(chainxy)
                except Exception:
                    params["px_chainxy"] = chainxy

            if not where_parts:
                st.info(f"[skip cascade] {child_tbl}: no usable WHERE (no FK, no fallback columns)")
                continue

            where_clause = " OR ".join(where_parts)

            # SET `<child_col>` = :<parent_col>  (we keep parent col names as bind keys)
            set_clause = ", ".join(f"`{child_col}`=:{parent_col}" for parent_col, child_col in set_map.items())
            sql = f"UPDATE `{child_tbl}` SET {set_clause} WHERE {where_clause}"

            payload_raw = {pcol: newrow.get(pcol, None) for pcol in set_map.keys()}
            payload = coerce_row_for_db(payload_raw)
            payload.update(params)

            result = conn.execute(text(sql), payload)
            st.info(f"[cascade] {child_tbl}: {result.rowcount or 0} rows; SET {list(set_map.items())}; WHERE {where_clause}")
            total_child_updates += result.rowcount or 0

    return total_child_updates

# ──────────────────────────── Save flow ────────────────────────────
btn_col1, btn_col2 = st.columns([1.2, 1])
save_clicked = btn_col1.button("Save Changes", type="primary")
st.warning("Note: Saving any changes will send an email alert to the SIP team.")
if btn_col2.button("Reload"):
    st.cache_data.clear()
    st.session_state["edit_df"] = load_table()
    st.rerun()

def _render_html_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df is None or df.empty:
        return "<em>None</em>"
    if len(df) > max_rows:
        df = df.head(max_rows).copy()
    return df.to_html(index=False, border=0).replace('class="dataframe"', 'style="font-size:12px"')

def _send_email(subject: str, html_body: str, attachments: list | None = None):
    try:
        msg = MIMEMultipart("mixed")
        msg["From"] = FROM_EMAIL
        msg["To"] = ", ".join(EMAIL_RECIPIENTS)
        msg["Subject"] = subject

        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(html_body, "html"))
        msg.attach(alt)

        for att in (attachments or []):
            content = att.get("content", b"")
            filename = att.get("filename", "attachment.bin")
            mime = att.get("mime", "application/octet-stream")
            part = MIMEApplication(content, _subtype=mime.split("/")[-1])
            part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
            part.add_header("Content-Type", mime)
            msg.attach(part)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.ehlo(); server.starttls(); server.ehlo()
            server.login(FROM_EMAIL, FROM_EMAIL_PASSWORD)
            server.sendmail(FROM_EMAIL, EMAIL_RECIPIENTS, msg.as_string())
    except Exception as e:
        st.warning(f"Email failed: {e}")

if save_clicked:
    try:
        inserts, updates, deleted, pk = diff_with_pk(df_from_db, st.session_state["edit_df"], pk=PARENT_PK_COL)
        ins_n = upd_n = del_n = 0
        child_id_retarget_n = 0
        child_value_cascade_n = 0

        # transactional audit context
        batch_id = os.getenv("HOSTNAME", "local") + "-" + datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        now_utc = datetime.utcnow()

        with engine.begin() as conn:
            # Deletes (no child cascades)
            if deleted:
                # Log deletes with BEFORE snapshot only (after_change={})
                before_idx = df_from_db.set_index(pk, drop=False)
                for did in sorted(deleted):
                    try:
                        brow = before_idx.loc[int(did)]
                        before_dict = _row_to_jsonable_dict(dict(brow))
                    except Exception:
                        before_dict = {"_missing": True}
                    _insert_audit_rows(
                        conn, batch_id, "DELETE", pk, did,
                        before_dict=before_dict,
                        after_dict={},
                        changed_cols=list(before_dict.keys()),
                        when_utc=now_utc,
                    )
                stmt = text(f"DELETE FROM `{TABLE}` WHERE `{pk}`=:id")
                for did in sorted(deleted):
                    conn.execute(stmt, {"id": int(did)})
                del_n = len(deleted)

            # Updates
            if updates:
                # 0) Pre-step: retarget child IDs if parent ID changed
                child_id_retarget_n = _preupdate_child_ids_if_parent_id_changed(updates, df_from_db, conn, pk=pk)

                # 1) Update parent rows
                upd_n = 0
                before_idx = df_from_db.set_index(pk, drop=False)
                for row in updates:
                    # Exclude read-only cols from parent updates
                    changed_cols = [c for c in row.keys() if c != pk and c not in READONLY_PARENT_COLS]
                    if not changed_cols:
                        continue

                    rid = row[pk]
                    try:
                        oldrow = before_idx.loc[int(rid)]
                        before_subset = {c: oldrow.get(c, None) for c in changed_cols}
                    except Exception:
                        before_subset = {}

                    after_subset = {c: row[c] for c in changed_cols}

                    # audit first
                    _insert_audit_rows(
                        conn, batch_id, "UPDATE", pk, rid,
                        before_dict=_row_to_jsonable_dict(before_subset),
                        after_dict=_row_to_jsonable_dict(after_subset),
                        changed_cols=changed_cols,
                        when_utc=now_utc,
                    )

                    # then SQL
                    set_clause = ", ".join(f"`{c}`=:{c}" for c in changed_cols)
                    stmt = text(f"UPDATE `{TABLE}` SET {set_clause} WHERE `{pk}`=:{pk}")
                    payload_raw = {pk: row[pk], **{c: row[c] for c in changed_cols}}
                    payload = coerce_row_for_db(payload_raw)
                    conn.execute(stmt, payload)
                    upd_n += 1

                # 2) Cascade changed values to children (FK OR fallback match)
                child_value_cascade_n = _propagate_updates_to_children(updates, df_from_db, conn, pk=pk)

            # Inserts (no child cascades)
            if inserts is not None and len(inserts) > 0:
                cols = [c for c in st.session_state["edit_df"].columns if c != pk]
                stmt = text(
                    f"INSERT INTO `{TABLE}` ({', '.join('`'+c+'`' for c in cols)}) "
                    f"VALUES ({', '.join(':'+c for c in cols)})"
                )
                for _, r in inserts.iterrows():
                    rid = None  # pk may be auto; we’ll store whatever is present in the row
                    try:
                        rid = int(r.get(pk)) if pd.notna(r.get(pk)) else None
                    except Exception:
                        rid = r.get(pk)

                    # Wipe read-only columns on insert as well (set None)
                    payload_src = {c: (None if c in READONLY_PARENT_COLS else r.get(c, None)) for c in cols}
                    payload = coerce_row_for_db(payload_src)

                    # audit (after snapshot = inserted fields)
                    _insert_audit_rows(
                        conn, batch_id, "INSERT", pk, rid if rid is not None else "(auto)",
                        before_dict={},
                        after_dict=_row_to_jsonable_dict(payload_src),
                        changed_cols=[c for c in payload_src.keys() if payload_src[c] is not None],
                        when_utc=now_utc,
                    )

                    conn.execute(stmt, payload)
                ins_n = len(inserts)

        st.success(
            f"Saved — Inserted: {ins_n}, Updated: {upd_n}, Deleted: {del_n}. "
            f"Child ID retargets: {child_id_retarget_n}, Child value cascades: {child_value_cascade_n}."
        )

        # ✉️ email after save (left intact) — audit is now transactional and independent of email
        try:
            ins_sample = inserts.drop(columns=[pk], errors="ignore").head(10) if inserts is not None else None
            # Summaries for email
            def _summarize_updates_for_email(updates: list[dict], before_df: pd.DataFrame, pk: str):
                if not updates:
                    empty_summary = pd.DataFrame(columns=[pk, "Changed_Columns"])
                    empty_detailed = pd.DataFrame(columns=[pk, "Column", "Before", "After"])
                    return empty_summary, empty_detailed
                before_idx = before_df.set_index(pk, drop=False)
                summary_rows, detailed_rows = [], []
                for newrow in updates:
                    rid = newrow.get(pk)
                    if rid is None or (isinstance(rid, float) and pd.isna(rid)):
                        continue
                    try:
                        rid_int = int(rid)
                    except Exception:
                        rid_int = rid
                    try:
                        oldrow = before_idx.loc[rid_int]
                    except Exception:
                        oldrow = None
                    changed_cols = []
                    for c, newv in newrow.items():
                        if c == pk or c in READONLY_PARENT_COLS:
                            continue
                        oldv = None if oldrow is None else oldrow.get(c, None)
                        n_old = _normalize_for_compare(oldv)
                        n_new = _normalize_for_compare(newv)
                        if n_old == n_new:
                            continue
                        changed_cols.append(c)
                        detailed_rows.append({
                            pk: rid_int,
                            "Column": c,
                            "Before": _format_for_email(oldv),
                            "After": _format_for_email(newv),
                        })
                    summary_rows.append({pk: rid_int, "Changed_Columns": ", ".join(changed_cols) if changed_cols else "(unknown)"})
                summary_df = pd.DataFrame(summary_rows)
                detailed_df = pd.DataFrame(detailed_rows).sort_values([pk, "Column"], ignore_index=True)
                return summary_df, detailed_df

            upd_summary, upd_detailed = _summarize_updates_for_email(updates or [], df_from_db, pk) if updates else (None, None)
            del_df = pd.DataFrame({pk: sorted(list(deleted))}) if deleted else pd.DataFrame(columns=[pk])

            html = f"""
            <html><body>
              <p>Saved by: <b>{user_email}</b></p>
              <p>Table: <b>{TABLE}</b></p>
              <p>Result — Inserted: <b>{ins_n}</b>, Updated rows: <b>{upd_n}</b>, Deleted rows: <b>{del_n}</b></p>
              <p>Child ID retargets: <b>{child_id_retarget_n}</b>, Child value cascades: <b>{child_value_cascade_n}</b></p>

              <h4>Inserted (sample)</h4>
              {_render_html_table(ins_sample)}

              <h4>Updated — Changed Columns per Row</h4>
              {_render_html_table(upd_summary)}

              <h4>Updated — Detailed Changes (Before → After)</h4>
              {_render_html_table(upd_detailed, max_rows=200)}

              <h4>Deleted (IDs)</h4>
              {_render_html_table(del_df)}
            </body></html>
            """
            subject = f"[Retailer Edit] {TABLE}: ins={ins_n}, upd={upd_n}, del={del_n}, child_id={child_id_retarget_n}, child_vals={child_value_cascade_n}"

            _send_email(subject=subject, html_body=html)

        except Exception as mail_err:
            st.warning(f"Changes saved, but email/audit messaging failed: {mail_err}")

        # Refresh editor data from DB snapshot again
        st.cache_data.clear()
        st.session_state["edit_df"] = load_table()
        st.rerun()
    except Exception as e:
        st.error(f"Save failed: {e}")

# ──────────────────────────── Bulk Upload (optional; unchanged) ────────────────────────────
st.markdown("---")
st.subheader("Bulk Upload (CSV/Excel)")
st.warning("Note: Adding any data will send an email alert to the SIP team.")

uploaded = st.file_uploader("Upload file to merge into editor", type=["csv", "xlsx", "xls"])

def _read_any(f) -> pd.DataFrame:
    if f.name.lower().endswith(".csv"):
        df = pd.read_csv(f)
    else:
        df = pd.read_excel(f)
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].map(lambda x: x.decode("utf-8", "ignore") if isinstance(x, (bytes, bytearray)) else x)
    for c in BIGINT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in DATETIME_COLS:
        if c in df.columns:
            df[c] = df[c].map(_parse_to_timestamp_or_na)
    for c in DATE_AS_TEXT_COLS:
        if c in df.columns:
            df[c] = df[c].map(_parse_to_timestamp_or_na)
    for c in BOOL_BINARY_COLS:
        if c in df.columns:
            df[c] = df[c].map(_binary_to_int01_ui).fillna(0).astype(int)
    return df

def _align_columns(df_ref: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    keep = list(df_ref.columns)
    aligned = pd.DataFrame(columns=keep)
    for c in keep:
        aligned[c] = df_new[c] if c in df_new.columns else None
    return aligned

if uploaded is not None:
    try:
        df_raw = _read_any(uploaded)
        st.write(f"Loaded **{uploaded.name}** — {len(df_raw):,} rows")
        st.dataframe(df_raw.head())

        df_aligned = _align_columns(st.session_state["edit_df"], df_raw)

        # 🛡️ Bulk-append guard: wipe read-only parent columns so they are not inserted/overwritten
        for c in READONLY_PARENT_COLS:
            if c in df_aligned.columns:
                df_aligned[c] = None

        c1, c2 = st.columns(2)
        if c1.button("Load into Editor (Append)"):
            st.session_state["edit_df"] = pd.concat([st.session_state["edit_df"], df_aligned], ignore_index=True)
            st.success(f"Appended {len(df_aligned):,} rows to editor (not saved yet).")
            st.rerun()

        if c2.button("Write to DB Now (Append)"):
            df_db = df_aligned.copy()
            for c in df_db.columns:
                if c in DATETIME_COLS:
                    df_db[c] = df_db[c].map(_to_sql_datetime)
                elif c in DATE_AS_TEXT_COLS:
                    df_db[c] = df_db[c].map(_to_sql_date_text)
                elif c in BOOL_BINARY_COLS:
                    df_db[c] = df_db[c].map(_to_sql_binary_bool)
                elif c in BIGINT_COLS:
                    df_db[c] = pd.to_numeric(df_db[c], errors="coerce").astype("Int64").map(lambda x: int(x) if pd.notna(x) else None)
                elif c in TEXT_LIKE_COLS:
                    df_db[c] = df_db[c].map(lambda x: None if x is None or (isinstance(x, float) and pd.isna(x)) else str(x))
                else:
                    df_db[c] = df_db[c].map(_to_sql_scalar_generic)

            with engine.begin() as conn:
                df_db.to_sql(TABLE, con=conn.connection, if_exists="append", index=False)

            st.cache_data.clear()
            st.session_state["edit_df"] = load_table()
            st.success(f"Appended {len(df_aligned):,} rows to DB.")

            # ✉️ email after bulk append WITH ATTACHMENT (left intact)
            try:
                html = f"""
                <html><body>
                  <p>Uploaded by: <b>{user_email}</b></p>
                  <p>Table: <b>{TABLE}</b></p>
                  <p>Bulk append completed.</p>
                  <ul>
                    <li><b>File:</b> {uploaded.name}</li>
                    <li><b>Rows appended:</b> {len(df_aligned):,}</li>
                  </ul>
                  <h4>Sample</h4>
                  {_render_html_table(df_aligned.head(15))}
                </body></html>
                """
                fname = uploaded.name
                fbytes = uploaded.getvalue()
                if fname.lower().endswith(".csv"):
                    mime = "text/csv"
                elif fname.lower().endswith(".xlsx"):
                    mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                elif fname.lower().endswith(".xls"):
                    mime = "application/vnd.ms-excel"
                else:
                    mime = "application/octet-stream"
                subject = f"[Retailer Bulk Upload] {TABLE}: +{len(df_aligned):,} rows"
                _send_email(
                    subject=subject,
                    html_body=html,
                    attachments=[{"filename": fname, "content": fbytes, "mime": mime}],
                )
            except Exception as mail_err:
                st.warning(f"Append succeeded, but email/audit logging failed: {mail_err}")

            st.rerun()
    except Exception as e:
        st.error(f"Upload failed: {e}")
