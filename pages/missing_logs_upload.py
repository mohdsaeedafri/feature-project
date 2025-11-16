import streamlit as st
import pandas as pd
import os, json, smtplib
from html_utils import include_html
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from streamlit_cookies_controller import CookieController
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
try:
    from streamlit_cookies_controller import CookieController
except Exception:
    CookieController = None

# ──────────────────────────── Boot ────────────────────────────
load_dotenv()
st.set_page_config(
    page_title="Missing Data Release Notes Uploader",
    layout="wide",
    page_icon="https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
    initial_sidebar_state="collapsed",
)

# ──────────────────────────── Staging-only access gate ────────────────────────────
# If ENVIRONMENT != 'staging', redirect to the production URL for this page.
IS_STAGING = os.getenv("ENVIRONMENT", "").lower() == "staging"
STAGING_BASE = "https://sip-portal-stg.coresight.com"
PROD_BASE    = "https://sip-portal.coresight.com"
CURRENT_PATH = "/missing_logs_upload"  # this file's route

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


# ──────────────────────────── Auth helpers (employee-only) ────────────────────────────
try:
    from auth_utils import logout as _shared_logout  # optional shared helper
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
            cc = CookieController(key="auth_cookies")
            cc.remove("auth_data")
        except Exception:
            pass
        for k in list(st.session_state.keys()):
            del st.session_state[k]

def _read_auth_cookie() -> dict:
    try:
        cc = CookieController(key="auth_cookies")
        raw = cc.get("auth_data")
        if not raw: return {}
        if isinstance(raw, str):  return json.loads(raw)
        if isinstance(raw, dict): return raw
        return {}
    except Exception:
        return {}

def ensure_auth_in_session():
    if st.session_state.get("user_email"): return
    ck = _read_auth_cookie() or {}
    if not ck: return
    st.session_state["session_id"] = ck.get("session_id")
    st.session_state["user_email"] = ck.get("user_email")
    st.session_state["token"] = ck.get("token")
    st.session_state["membership"] = ck.get("membership") or {}
    st.session_state["membership_id"] = ck.get("membership_id")
    st.session_state["membership_type"] = ck.get("membership_type")

def _login_link_url():
    is_staging = os.getenv("ENVIRONMENT", "").lower() == "staging"
    return "https://sip-portal-stg.coresight.com/" if is_staging else "https://sip-portal.coresight.com/"

# NEW: save back to cookie (you used this name elsewhere)
def save_auth_cookie(data: dict):
    try:
        cc = CookieController(key="auth_cookies")
        cc.set("auth_data", json.dumps(data), max_age_days=30, same_site="Lax")
    except Exception:
        pass

# Enforce employee-only
ensure_auth_in_session()
user_email = (st.session_state.get("user_email") or "").strip()
membership_id = str(st.session_state.get("membership_id") or "").strip()

if not user_email:
    st.warning("You must be logged in to access this page.")
    st.link_button("Login", _login_link_url())
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

if membership_id != EMPLOYEE_ID:
    st.info("Access restricted to employees. Redirecting you…")
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

include_html("header.html")

# ──────────────────────────── DB bootstrap ────────────────────────────
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
SSL_CA = os.getenv("SSL_CA")
data_change_log_stg = 'data_change_log'
# data_change_log_prod = 'data_change_log'

# DB_HOST_PROD = os.getenv("DB_HOST_PROD")
# DB_NAME_PROD = os.getenv("DB_NAME_PROD")
# DB_USER_PROD = os.getenv("DB_USER_PROD")
# DB_PASSWORD_PROD = os.getenv("DB_PASSWORD_PROD")

engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}",
    connect_args={"ssl_ca": SSL_CA} if SSL_CA else {}
)
# engine_prod = create_engine(
#     f"mysql+mysqlconnector://{DB_USER_PROD}:{DB_PASSWORD_PROD}@{DB_HOST_PROD}/{DB_NAME_PROD}",
#     connect_args={"ssl_ca": SSL_CA} if SSL_CA else {}  # optional, but safer if prod requires SSL
# )

# ──────────────────────────── Cookie access (safe) ────────────────────────────
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

# ──────────────────────────── UI header ────────────────────────────
col1, col3, col4, col5, col2 = st.columns([6.5, 1.5, 1.2, 1, 1], vertical_alignment="bottom")
with col1:
    st.subheader("Missing Data Release Notes Uploader")
    st.html("<style>[data-testid='stHeaderActionElements'] {display: none;}</style>")
with col3:
    # put the portal hop behind a button (prevents auto-redirect loops)
    if st.button("App Release Portal", type="tertiary", icon=":material/release_alert:"):
        user_filters["returnPage"] = "missing_logs_upload"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        # same-tab navigation
        st.switch_page("pages/app_release_portal.py")

with col4:
    if st.button("Edit Retailers", key="go_to_retailers_editing",
                 type="tertiary", icon=":material/edit:"):
        # remember return page in the cookie
        user_filters["returnPage"] = "missing_logs_upload"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        st.switch_page("pages/retailers_editing.py")

with col5:
    if st.button("Sync Data", key="go_to_promote_stg_to_prod",
                 type="tertiary", icon=":material/sync:"):
        # remember return page in the cookie
        user_filters["returnPage"] = "missing_logs_upload"
        auth_cookie["filters"] = user_filters
        save_auth_cookie(auth_cookie)
        st.switch_page("pages/sync_data.py")


with col2:
    if st.button("Log Out", key="Logout", type="tertiary", icon=":material/logout:"):
        logout()
        _redirect_immediate(LOGIN_URL)


# ──────────────────────────── Email settings ────────────────────────────
EMAIL_RECIPIENTS = [
    'ShashankGupta@coresight.com',
    'johnmercer@coresight.com',
    'MohdSaeedAfri@coresight.com',
    'vaishnavinayakk@coresight.com', 
    'dataautomation@coresight.com'
]
FROM_EMAIL = "dataautomation@coresight.com"
FROM_EMAIL_PASSWORD = os.getenv("FROM_EMAIL_PASSWORD") or "D)636612595014om"
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

def send_email(subject: str, html_body: str, recipients: list[str]):
    msg = MIMEMultipart("alternative")
    msg["From"] = FROM_EMAIL
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
        server.ehlo(); server.starttls(); server.ehlo()
        server.login(FROM_EMAIL, FROM_EMAIL_PASSWORD)
        server.sendmail(FROM_EMAIL, recipients, msg.as_string())

# ──────────────────────────── Upload logic ────────────────────────────
uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx", "xls"])
st.warning("Note: Adding any data will send an email alert to the SIP team.")

if uploaded_file:
    try:
        df = pd.read_excel(uploaded_file)

        df.to_sql(name=data_change_log_stg, con=engine, if_exists="append", index=False)
        st.success(f"Data appended to staging table `{data_change_log_stg}`!")

        # df.to_sql(name=data_change_log_prod, con=engine_prod, if_exists="append", index=False)
        # st.success(f"Data appended to production table `{data_change_log_prod}`!")

        preview = pd.read_sql(
            f"SELECT * FROM `{data_change_log_stg}` ORDER BY 1 DESC LIMIT 10",
            con=engine
        )
        st.subheader(f"Last 10 rows in `{data_change_log_stg}`")
        st.dataframe(preview)

        filename = uploaded_file.name
        row_count = len(df)
        sample_html = df.head(10).to_html(index=False, border=0)
        html = f"""
        <html><body>
          <p>Hi team,</p>
          <p><b>Missing Data Release Notes</b> file was uploaded successfully.</p>
          <ul>
            <p>Uploaded by: <b>{user_email}</b></p>
            <li><b>File:</b> {filename}</li>
            <li><b>Rows appended:</b> {row_count:,}</li>
            <li><b>Staging table:</b> {data_change_log_stg}</li>
          </ul>
          <p><b>Sample of uploaded rows (first 10):</b></p>
          {sample_html}
          <p style="color:#666;margin-top:16px;">— Automated notice from Streamlit uploader</p>
        </body></html>
        """
        try:
            send_email(
                subject=f"[Release Notes] Upload OK: {filename} ({row_count:,} rows)",
                html_body=html,
                recipients=EMAIL_RECIPIENTS,
            )
            st.success(f"Email notification sent to {len(EMAIL_RECIPIENTS)} recipients.")
        except Exception as mail_err:
            st.warning(f"Upload succeeded, but email failed: {mail_err}")

    except Exception as e:
        st.error(f"Upload or database operation failed:\n{e}")
