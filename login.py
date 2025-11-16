#!/usr/bin/env python3
# login.py — v3.4 (JSON primary + fallbacks incl. backslash-escape mode)
# Attempts:
#   A) JSON wire (replace \" -> \u0022, ' -> \u0027)
#   B) JSON raw  (requests json=payload)
#   C) FORM      (urlencoded)
#   D) Backslash-escape quotes in value (" -> \", ' -> \') then: JSON wire → JSON raw → FORM
# Rationale: some PHP/WP stacks unslash incoming values before auth. Sending backslash-escaped quotes
# lets PHP un-slash to the exact original password.

import os
import json
import logging
import requests
import traceback
from time import sleep, perf_counter
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Optional, Union, Tuple

import streamlit as st
from dotenv import load_dotenv
from streamlit_cookies_controller import CookieController

from auth_utils import  login_user, create_db_connection, _write_auth_cookie, _read_auth_cookie, _clear_all_auth_cookies
from html_utils import include_html

# ===========================
# Bootstrap / Config
# ===========================
load_dotenv()

st.set_page_config(
    page_title="SIP Login",
    page_icon="https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
    initial_sidebar_state="collapsed",
    layout="centered",
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s",
)
log = logging.getLogger("sip-login")

AUTH_URL = os.getenv("AUTH_URL", "").strip()
AUTH_URL_PAID = os.getenv("AUTH_URL_PAID", "").strip()
MEMBERSHIP_ABSOLUTE_URL = os.getenv("MEMBERSHIP_ABSOLUTE_URL", "").strip()
REQUIRE_MEMBERSHIP = os.getenv("REQUIRE_MEMBERSHIP", "1").strip() == "1"

FORCE_URLENCODE_ONLY = 0

EMPLOYEE_ID = os.getenv("EMPLOYEE_ID", "101318").strip()
PAID_MEMBER_ID = os.getenv("PAID_MEMBER_ID", "100939").strip()
FREE_TRIAL_MEMBER_ID = os.getenv("FREE_TRIAL_MEMBER_ID", "164720").strip()
SIP_MEMBER_FREE_TRIAL_MEMBER_ID = os.getenv("FREE_TRIAL_MEMBER_ID", "325429").strip()
MEMBERSHIP_IDS_CSV = os.getenv(
    "MEMBERSHIP_IDS_CSV",
    f"{EMPLOYEE_ID},{PAID_MEMBER_ID},{FREE_TRIAL_MEMBER_ID}"
).strip()

MEMBERSHIP_IDS_CSV = f"{EMPLOYEE_ID},{PAID_MEMBER_ID},{FREE_TRIAL_MEMBER_ID},{SIP_MEMBER_FREE_TRIAL_MEMBER_ID}"

log.info("=== SIP Login starting (v3.4 JSON primary + backslash-escape fallback) ===")
log.info(f"LOG_LEVEL={LOG_LEVEL}")
log.info(f"REQUIRE_MEMBERSHIP={REQUIRE_MEMBERSHIP}")
log.info(f"AUTH_URL={'<unset>' if not AUTH_URL else AUTH_URL}")
log.info(f"AUTH_URL_PAID={'<unset>' if not AUTH_URL_PAID else AUTH_URL_PAID}")
log.info(f"MEMBERSHIP_ABSOLUTE_URL={'<unset>' if not MEMBERSHIP_ABSOLUTE_URL else MEMBERSHIP_ABSOLUTE_URL}")
log.info(f"FORCE_URLENCODE_ONLY={FORCE_URLENCODE_ONLY}")
log.info(f"EMPLOYEE_ID={EMPLOYEE_ID} | PAID_MEMBER_ID={PAID_MEMBER_ID} | FREE_TRIAL_MEMBER_ID={FREE_TRIAL_MEMBER_ID}| SIP_MEMBER_FREE_TRIAL_MEMBER_ID={SIP_MEMBER_FREE_TRIAL_MEMBER_ID}")
log.info(f"MEMBERSHIP_IDS_CSV={MEMBERSHIP_IDS_CSV}")


# ===========================
# Small utils
# ===========================
def _exc() -> str:
    return "".join(traceback.format_exc(limit=2))

def _mask(s: str, keep=4) -> str:
    if not s:
        return ""
    if len(s) <= keep:
        return "*" * len(s)
    return s[:keep] + "..." + "*" * (max(0, len(s) - keep - 3))

def _mask_bearer(hdrs: dict) -> dict:
    if not isinstance(hdrs, dict):
        return {}
    out = dict(hdrs)
    val = out.get("Authorization")
    if isinstance(val, str) and val.lower().startswith("bearer "):
        out["Authorization"] = "Bearer " + _mask(val.split(" ", 1)[1], keep=6)
    return out

def _as_str_or_empty(x) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    return str(x)

def sanitize_username(u) -> str:
    if u is None:
        return ""
    return u.strip() if isinstance(u, str) else str(u)

def sanitize_password(p: Union[str, bytes, None]) -> str:
    # Do not mutate for logging/preview; transport handling happens later.
    if p is None:
        return ""
    if isinstance(p, bytes):
        return p.decode("utf-8", errors="replace")
    return str(p)

def _has_quote(s: str) -> bool:
    return isinstance(s, str) and ("'" in s or '"' in s)

# ===========================
# Cookie Controller
# ===========================
 
def clear_auth_cookie():
    try:
        controller.remove("auth_data")
        log.info("Auth cookie cleared.")
    except Exception as e:
        log.warning(f"Cookie delete failed: {e} | { _exc() }")

def get_current_domain(default: Optional[str] = None) -> str:
    """
    Best-effort current host → cookie domain.
    - Returns '.coresight.com' on any coresight subdomain (cross-subdomain cookie).
    - Else returns the exact host (e.g., 'localhost').
    - Falls back to default or '' on failure.
    """
    try:
        import streamlit as st

        host = ""
        # Streamlit ≥ 1.29 sometimes exposes request headers here
        ctx = getattr(st, "context", None)
        if ctx is not None and getattr(ctx, "headers", None):
            headers = ctx.headers or {}
            host = headers.get("host") or headers.get("Host") or ""

        if not host:
            # Fallback for most versions
            try:
                from streamlit.web.server.websocket_headers import _get_websocket_headers
                ws = _get_websocket_headers() or {}
                host = ws.get("host") or ws.get("Host") or ""
            except Exception:
                pass

        domain = host.split(":", 1)[0].strip()
        if domain.endswith(".coresight.com"):
            return ".coresight.com"
        return domain or (default or "")
    except Exception:
        return default or ""

# ===========================
# URL Helpers
# ===========================
def _strip_trailing(s: str, suffix: str) -> str:
    return s[:-len(suffix)] if s and s.endswith(suffix) else s

def _no_trailing_slash(url: str) -> str:
    if not url:
        return url
    return url[:-1] if url.endswith("/") else url

def _wpjson_base(api_url: str):
    if not api_url:
        log.debug("_wpjson_base: empty api_url -> None")
        return None
    url = _no_trailing_slash(api_url)
    lower = url.lower()

    token_suffix = "/jwt-auth/v1/token"
    if lower.endswith(token_suffix):
        base = _strip_trailing(url, token_suffix)
        out = _no_trailing_slash(base + "/wp-json") if "/wp-json" not in base.lower() else _no_trailing_slash(base)
        log.debug(f"_wpjson_base from token URL: {api_url} -> {out}")
        return out

    if "/wp-json" in lower:
        parts = url.split("/wp-json")
        out = _no_trailing_slash(parts[0] + "/wp-json")
        log.debug(f"_wpjson_base trimming after /wp-json: {api_url} -> {out}")
        return out

    out = _no_trailing_slash(url + "/wp-json")
    log.debug(f"_wpjson_base appending /wp-json: {api_url} -> {out}")
    return out

def _auth_endpoint():
    base_paid = _wpjson_base(AUTH_URL_PAID) if AUTH_URL_PAID else None
    if base_paid:
        url = f"{base_paid}/jwt-auth/v1/token"
        log.info(f"JWT endpoint (from AUTH_URL_PAID): {url}")
        return url

    base_auth = _wpjson_base(AUTH_URL) if AUTH_URL else None
    if base_auth:
        url = f"{base_auth}/jwt-auth/v1/token"
        log.info(f"JWT endpoint (from AUTH_URL): {url}")
        return url

    log.error("JWT endpoint could not be derived. Set AUTH_URL_PAID or AUTH_URL in .env.")
    return None

def _membership_endpoint(ids_csv: str):
    if MEMBERSHIP_ABSOLUTE_URL:
        url = MEMBERSHIP_ABSOLUTE_URL.format(ids=ids_csv)
        log.info(f"Membership endpoint (absolute override): {url}")
        return url

    base_paid = _wpjson_base(AUTH_URL_PAID) if AUTH_URL_PAID else None
    if base_paid:
        url = f"{base_paid}/custom-api/v1/has-access?ids={ids_csv}"
        log.info(f"Membership endpoint (from AUTH_URL_PAID): {url}")
        return url

    log.error("Membership endpoint not derivable (no AUTH_URL_PAID and no MEMBERSHIP_ABSOLUTE_URL).")
    return None

# ===========================
# HTTP helpers (retry/lockout)
# ===========================
def _retry_after_minutes(resp: requests.Response) -> Optional[int]:
    if not resp:
        return None
    ra = resp.headers.get("Retry-After")
    if not ra:
        return None
    ra = ra.strip()
    if ra.isdigit():
        secs = int(ra)
        return max(1, (secs + 59) // 60)
    try:
        dt = parsedate_to_datetime(ra)
        if dt:
            delta = (dt - datetime.utcnow()).total_seconds()
            return max(1, int((delta + 59) // 60))
    except Exception:
        pass
    return None

def _is_lockout_response(resp: requests.Response) -> bool:
    if resp is None:
        return False
    if resp.status_code in (429, 503, 403):
        txt = (resp.text or "").lower()
        markers = (
            "you are temporarily locked out",
            "too many login attempts",
            "limit login attempts",
            "blocked by wordfence",
            "blocked by ithemes",
            "access denied by security policy",
            "exceeded the rate limit",
            "temporarily unavailable",
        )
        return any(m in txt for m in markers)
    return False

# ===========================
# Membership helpers
# ===========================
def _normalize_has_access_response(resp_json):
    has_access = False
    matches = []
    if isinstance(resp_json, dict):
        access_dict = resp_json.get("access", {})
        if isinstance(access_dict, dict):
            matches = [str(k) for k, v in access_dict.items() if v is True]
            has_access = len(matches) > 0
        if "has_access" in resp_json and isinstance(resp_json["has_access"], bool):
            has_access = resp_json["has_access"]
        if "matches" in resp_json and isinstance(resp_json["matches"], (list, tuple)):
            matches = [str(x) for x in resp_json["matches"]]
        if "allowed" in resp_json and isinstance(resp_json["allowed"], bool):
            has_access = resp_json["allowed"]
    log.info(f"[MEMBERSHIP] Normalized -> has_access={has_access}, matches={matches}")
    return {"has_access": has_access, "matches": matches, "raw": resp_json or {}}

def _has_premium_access(membership_id: str, membership_result: dict) -> bool:
    if membership_id:
        log.info(f"[GATE] Premium via membership_id={membership_id}")
        return True
    if membership_result and isinstance(membership_result, dict):
        if membership_result.get("has_access") and membership_result.get("matches"):
            log.info("[GATE] Premium via membership_result flags.")
            return True
    log.info("[GATE] Premium not granted.")
    return False

# ===========================
# JSON & FORM body builders
# ===========================
def _build_wire_json(payload: dict) -> Tuple[str, str]:
    """
    Returns (raw_json, wire_json).
    raw_json: json.dumps(..., ensure_ascii=False, compact)
    wire_json: with \" -> \u0022 and ' -> \u0027
    """
    raw_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    wire_json = raw_json.replace('\\"', '\\u0022').replace("'", "\\u0027")
    return raw_json, wire_json

def _escape_for_unslash(value: str) -> str:
    """
    Backslash-escape quotes so that PHP stripslashes/wp_unslash will restore the original.
    We also double any existing backslashes to avoid accidental loss.
    Example:  He said "Hi" -> send He said \"Hi\"
              It's me      -> send It\'s me
    """
    if value is None:
        return ""
    # First escape backslashes themselves
    v = value.replace("\\", "\\\\")
    # Then escape quotes
    v = v.replace('"', '\\"').replace("'", "\\'")
    return v

# ===========================
# AUTH senders
# ===========================
def _send_json_wire(token_url: str, payload: dict, timeout: int = 12):
    headers = {
        "Accept": "application/json, */*;q=0.5",
        "User-Agent": "SIPLogin/3.4 (+https://coresight.com)",
        "Content-Type": "application/json; charset=utf-8",
    }
    raw_json, wire_json = _build_wire_json(payload)
    st.session_state["__auth_json_preview_raw__"] = raw_json
    st.session_state["__auth_json_preview_wire__"] = wire_json
    # log.warning(f"[DEBUG][AUTH][JSON][PREVIEW_RAW]  {raw_json}")
    # st.warning(f"[DEBUG][AUTH][JSON][PREVIEW_RAW]  {raw_json}")
    # log.warning(f"[DEBUG][AUTH][JSON][PREVIEW_WIRE] {wire_json}")
    # st.warning(f"[DEBUG][AUTH][JSON][PREVIEW_WIRE] {wire_json}")

    prep = requests.Request("POST", token_url, data=wire_json, headers=headers)
    prep = requests.Session().prepare_request(prep)
    body_preview = prep.body if isinstance(prep.body, str) else (prep.body or b"")
    # log.warning(f"[DEBUG][AUTH][JSON][PREPARED_BODY] {body_preview if isinstance(body_preview, str) else body_preview.decode('utf-8', 'replace')}")
    # st.warning(f"[DEBUG][AUTH][JSON][PREPARED_BODY] {body_preview if isinstance(body_preview, str) else body_preview.decode('utf-8', 'replace')}")

    resp = requests.post(token_url, data=wire_json, headers=headers, timeout=timeout, allow_redirects=False)
    log.info(f"[AUTH][JSON-WIRE] status={resp.status_code} ct={resp.headers.get('Content-Type')}")
    # st.info(f"[AUTH][JSON-WIRE] status={resp.status_code} ct={resp.headers.get('Content-Type')}")
    return resp, "json-wire"

def _send_json_raw(token_url: str, payload: dict, timeout: int = 12):
    headers = {
        "Accept": "application/json, */*;q=0.5",
        "User-Agent": "SIPLogin/3.4 (+https://coresight.com)",
    }
    preview = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    # log.warning(f"[DEBUG][AUTH][JSON-RAW][PREVIEW] {preview}")
    # st.warning(f"[DEBUG][AUTH][JSON-RAW][PREVIEW] {preview}")

    prep = requests.Request("POST", token_url, json=payload, headers=headers)
    prep = requests.Session().prepare_request(prep)
    body_preview = prep.body if isinstance(prep.body, str) else (prep.body or b"")
    # log.warning(f"[DEBUG][AUTH][JSON-RAW][PREPARED_BODY] {body_preview if isinstance(body_preview, str) else body_preview.decode('utf-8', 'replace')}")
    # st.warning(f"[DEBUG][AUTH][JSON-RAW][PREPARED_BODY] {body_preview if isinstance(body_preview, str) else body_preview.decode('utf-8', 'replace')}")

    resp = requests.post(token_url, json=payload, headers=headers, timeout=timeout, allow_redirects=False)
    # log.info(f"[AUTH][JSON-RAW] status={resp.status_code} ct={resp.headers.get('Content-Type')}")
    # st.info(f"[AUTH][JSON-RAW] status={resp.status_code} ct={resp.headers.get('Content-Type')}")
    return resp, "json-raw"

def _send_form(token_url: str, payload: dict, timeout: int = 12):
    headers = {
        "Accept": "application/json, */*;q=0.5",
        "User-Agent": "SIPLogin/3.4 (+https://coresight.com)",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    prep = requests.Request("POST", token_url, data=payload, headers=headers)
    prep = requests.Session().prepare_request(prep)
    body_preview = prep.body if isinstance(prep.body, str) else (prep.body or b"")
    # log.warning(f"[DEBUG][AUTH][FORM][PREPARED_BODY] {body_preview if isinstance(body_preview, str) else body_preview.decode('utf-8', 'replace')}")
    # st.warning(f"[DEBUG][AUTH][FORM][PREPARED_BODY] {body_preview if isinstance(body_preview, str) else body_preview.decode('utf-8', 'replace')}")

    resp = requests.post(token_url, data=payload, headers=headers, timeout=timeout, allow_redirects=False)
    # log.info(f"[AUTH][FORM] status={resp.status_code} ct={resp.headers.get('Content-Type')}")
    # st.info(f"[AUTH][FORM] status={resp.status_code} ct={resp.headers.get('Content-Type')}")
    return resp, "form"

def _parse_error(resp: requests.Response):
    code = msg = None
    try:
        j = resp.json()
        code = (j.get("code") or "").strip()
        msg = j.get("message")
    except Exception:
        pass
    return code, msg

# ===========================
# Authenticate (A→B→C, then D with backslash-escape)
# ===========================
def authenticate_user(username, password):
    token_url = _auth_endpoint()
    if not token_url:
        # st.error("Cannot derive JWT token endpoint. Check AUTH_URL/AUTH_URL_PAID.")
        return None

    username = sanitize_username(_as_str_or_empty(username))
    password = sanitize_password(_as_str_or_empty(password))
    if not username or not password:
        st.error("Please enter both email and password.")
        return None

    payload = {"username": username, "password": password}

    log.warning(f"[DEBUG][AUTH] POST {token_url} | username={username} | pw_len={len(password)} has_quote={_has_quote(password)}")
    # st.warning(f"[DEBUG][AUTH] POST {token_url} | username={username} | pw_len={len(password)} has_quote={_has_quote(password)})

    t0 = perf_counter()
    try:
        # A) JSON wire
        resp, mode = _send_json_wire(token_url, payload, timeout=12)
        if resp is not None and resp.status_code == 200:
            return _handle_success(resp, mode)
        _log_err(resp, mode)

        # B) JSON raw
        respB, modeB = _send_json_raw(token_url, payload, timeout=12)
        if respB is not None and respB.status_code == 200:
            return _handle_success(respB, modeB)
        _log_err(respB, modeB)

        # C) FORM
        respC, modeC = _send_form(token_url, payload, timeout=12)
        if respC is not None and respC.status_code == 200:
            return _handle_success(respC, modeC)
        _log_err(respC, modeC)

        # D) Backslash-escape password value, then retry A→B→C
        if _has_quote(password):
            escaped_pw = _escape_for_unslash(password)
            log.warning("[DEBUG][AUTH] Retrying with backslash-escaped password value (hidden)")
            # st.warning(f"[DEBUG][AUTH] Retrying with backslash-escaped password value: {escaped_pw}")
            payload2 = {"username": username, "password": escaped_pw}

            respD1, modeD1 = _send_json_wire(token_url, payload2, timeout=12)
            if respD1 is not None and respD1.status_code == 200:
                return _handle_success(respD1, modeD1)
            _log_err(respD1, modeD1)

            respD2, modeD2 = _send_json_raw(token_url, payload2, timeout=12)
            if respD2 is not None and respD2.status_code == 200:
                return _handle_success(respD2, modeD2)
            _log_err(respD2, modeD2)

            respD3, modeD3 = _send_form(token_url, payload2, timeout=12)
            if respD3 is not None and respD3.status_code == 200:
                return _handle_success(respD3, modeD3)
            _log_err(respD3, modeD3)

        # Final user-facing guidance
        # st.error(
        #     "Login failed. We attempted JSON (escaped), JSON (raw), FORM, and a backslash-escape variant "
        #     "to work around server-side unslashing. The endpoint still returned `incorrect_password`."
        # )
        # if _has_quote(password):
            # st.info(
            #     "The account password contains quotes. Some server stacks strip or alter quotes before auth. "
            #     "As a last resort, change your password to one without quotes and try again."
            # )
            # st.markdown("Reset it here: [Password Reset](https://coresight.com/csradmin?action=lostpassword)")
        # return None

    except requests.RequestException as e:
        log.error(f"[AUTH] Request error: {e} | { _exc() }")
        # st.error(f"[AUTH] Request error: {e} | { _exc() }")
        return None
    except Exception as e:
        log.error(f"[AUTH] Unexpected error: {e} | { _exc() }")
        # st.error(f"[AUTH] Unexpected error: {e} | { _exc() }")
        return None

def _handle_success(resp: requests.Response, mode: str):
    try:
        data = resp.json()
    except Exception:
        log.error(f"[AUTH] 200 but non-JSON payload ({mode}).")
        # st.error(f"[AUTH] 200 but non-JSON payload ({mode}).")
        return None
    tok = data.get("token")
    if tok:
        log.info(f"[AUTH] token={_mask(tok, keep=6)} user_email={data.get('user_email')}")
        # st.info(f"[AUTH] token={_mask(tok, keep=6)} user_email={data.get('user_email')}")
    else:
        log.warning(f"[AUTH] 200 but missing token key ({mode}).")
        # st.warning(f"[AUTH] 200 but missing token key ({mode}).")
    return data

def _log_err(resp: requests.Response, mode: str):
    if resp is None:
        log.error(f"[AUTH] No response from token endpoint ({mode}).")
        # st.error(f"[AUTH] No response from token endpoint ({mode}).")
        return
    ms = "n/a"
    try:
        code, msg = _parse_error(resp)
        if code or msg:
            log.warning(f"[AUTH] JWT error ({mode}) {resp.status_code}: code={code} msg={msg}")
            # st.warning(f"[AUTH] JWT error ({mode}) {resp.status_code}: code={code} msg={msg}")
        else:
            snippet = (resp.text or "")[:300].replace("\n", " ")
            log.warning(f"[AUTH] Failure ({mode}) {resp.status_code}: {snippet}")
            # st.warning(f"[AUTH] Failure ({mode}) {resp.status_code}: {snippet}")
    except Exception:
        snippet = (getattr(resp, "text", "") or "")[:300].replace("\n", " ")
        log.warning(f"[AUTH] Failure ({mode}) {getattr(resp, 'status_code', 'n/a')}: {snippet}")
        # st.warning(f"[AUTH] Failure ({mode}) {getattr(resp, 'status_code', 'n/a')}: {snippet}")

# ===========================
# Membership (unchanged)
# ===========================
def check_membership(token: str, ids_csv: str = MEMBERSHIP_IDS_CSV, timeout=10):
    if not token:
        log.error("[MEMBERSHIP] Missing token.")
        return None
    has_access_url = _membership_endpoint(ids_csv)
    if not has_access_url:
        log.error("[MEMBERSHIP] Endpoint not configured (AUTH_URL_PAID or MEMBERSHIP_ABSOLUTE_URL required).")
        return None
    headers = {"Authorization": f"Bearer {token}"}
    masked = _mask_bearer(headers)
    log.info(f"[MEMBERSHIP] GET {has_access_url} | headers={masked}")
    try:
        for attempt in range(2):
            t0 = perf_counter()
            resp = requests.get(has_access_url, headers=headers, timeout=timeout)
            ms = int((perf_counter() - t0) * 1000)
            log.info(f"[MEMBERSHIP] Response (try {attempt+1}/2): status={resp.status_code} in {ms}ms")
            if resp.status_code == 200:
                try:
                    j = resp.json()
                    log.debug(f"[MEMBERSHIP] Raw JSON (truncated): {str(j)[:500]}")
                    return _normalize_has_access_response(j)
                except Exception as e:
                    log.error(f"[MEMBERSHIP] JSON parse error: {e}; text={resp.text[:300]}")
                    return None
            else:
                snippet = (resp.text or "")[:300].replace("\n", " ")
                log.warning(f"[MEMBERSHIP] HTTP {resp.status_code}: {snippet}")
                if resp.status_code == 404:
                    log.warning("[MEMBERSHIP] 404 suggests wrong path/host (check AUTH_URL_PAID / override).")
                    break
        return None
    except requests.RequestException as e:
        log.error(f"[MEMBERSHIP] Request error: {e} | { _exc() }")
        return None
    except Exception as e:
        log.error(f"[MEMBERSHIP] Unexpected error: {e} | { _exc() }")
        return None

# ===========================
# UI + Styles
# ===========================
st.markdown("""
    <style>
    .stApp, .main, .block-container, .css-18e3th9, .css-1y0tads, main { padding: 0 !important; margin: 0 !important; }
    .block-container, .css-18e3th9 { padding-top: 85px !important; margin-top: 0 !important; }
    header[data-testid="stHeader"], footer { display: none !important; }
    #MainMenu { visibility: hidden !important; }
    body, .stApp { background: #fff !important; }
    </style>
""", unsafe_allow_html=True)

include_html("header_logout.html")

st.markdown(
    "<h4 style='margin-bottom:-30px;'>Welcome to the Coresight Research</h4>"
    "<h1 style='font-size:40px;'>Store Intelligence Platform</h1>",
    unsafe_allow_html=True
)
st.markdown(
    """
**Coresight Research Premium members** may use their existing login credentials 
to access a complimentary trial of the **Store Intelligence Platform**.

<a href="https://coresight.com/store-intelligence-platform-overview/" 
   target="_blank" 
   style="text-decoration: none; color: #d6262f; font-weight: 600;">
   Learn more: Store Intelligence Platform Overview >
</a>
""",
    unsafe_allow_html=True
)


# ===========================
# Auto-restore
# ===========================
existing = _read_auth_cookie()
try:
    user_email = existing.get("user_email")
    token = existing.get("token")
    session_id = existing.get("session_id")
    membership_id = existing.get("membership_id")
    membership_result = existing.get("membership", {}) if isinstance(existing.get("membership"), dict) else {}
    membership_type = existing.get("membership_type")

    if user_email and token and session_id:
        key_prefix = f"{user_email}_{session_id}"
        st.session_state.update(
            {
                f"{key_prefix}_session_id": session_id,
                f"{key_prefix}_user_id": user_email,
                f"{key_prefix}_token": token,
                f"{key_prefix}_membership": membership_result,
                f"{key_prefix}_membership_id": membership_id,
                f"{key_prefix}_membership_type": membership_type,
            }
        )
        if _has_premium_access(membership_id, membership_result) or not REQUIRE_MEMBERSHIP:
            log.info("[RESTORE] Access confirmed → redirect pages/opening.py")
            st.switch_page("pages/opening.py")
        else:
            log.info("[RESTORE] Access not confirmed → clearing cookie.")
            _clear_all_auth_cookies()
except Exception as e:
    log.error(f"[RESTORE] Error restoring session from cookie: {e} | { _exc() }")
    _clear_all_auth_cookies()

# ===========================
# Login Form
# ===========================
st.markdown(
    """
<style>
div.stButton > button:first-child {
    background-color: #d32f2f; color: white; width: 100%; height: 3em;
    font-size: 18px; border-radius: 4px; border: none;
}
div.stButton > button:first-child:hover { background-color: #b71c1c; color: white; }
</style>
""",
    unsafe_allow_html=True,
)

placeholder = st.empty()
with placeholder.container():
    username = st.text_input("Email or Username")
    password = st.text_input("Password", type="password")
    login_button = st.button("Login")

# Dev previews
# if "__auth_json_preview_raw__" in st.session_state:
#     st.caption("JSON preview (what json.dumps would produce):")
#     st.code(st.session_state["__auth_json_preview_raw__"], language="json")
# if "__auth_json_preview_wire__" in st.session_state:
#     st.caption("Exact JSON body sent over the wire (quotes unicode-escaped):")
#     st.code(st.session_state["__auth_json_preview_wire__"], language="json")

# ===========================
# Login Flow
# ===========================
if login_button:
    log.info(f"[LOGIN] Clicked | username={username!r} | pw_len={len(password)}")
    # st.info(f"[LOGIN] Clicked | username={username!r} | password={password!r}")
    token_data = authenticate_user(username, password)
    # st.info(token_data)
    if token_data:
        user_email = token_data.get("user_email")
        token = token_data.get("token")
        log.info(f"[LOGIN] JWT success user_email={user_email}")
        # st.info(f"[LOGIN] JWT success user_email={user_email} token={token}")

        if not (user_email and token):
            log.error(f"[LOGIN] Unexpected token payload keys={list(token_data.keys())}")
            # st.error(f"[LOGIN] Unexpected token payload keys={list(token_data.keys())}")
            # st.error("Login response was invalid. Please try again.")
            st.stop()

        membership_result = None
        membership_id = None
        membership_type = None

        if REQUIRE_MEMBERSHIP:
            log.info("[LOGIN] REQUIRE_MEMBERSHIP=1 → membership check.")
            membership_result = check_membership(token)
            if membership_result is None:
                log.error("[LOGIN] Membership check returned None → block.")
                st.error(
                    "Unable to verify Premium access at the moment. "
                    "If this persists, please contact us."
                )
                st.stop()

            if membership_result.get("matches"):
                membership_id = str(membership_result["matches"][0]).strip()
                log.info(f"[LOGIN] membership_id={membership_id}")

            if membership_id:
                conn = create_db_connection()
                if conn:
                    try:
                        with conn.cursor() as cur:
                            cur.execute("SELECT label FROM membership_types WHERE id = %s", (membership_id,))
                            row = cur.fetchone()
                            if row:
                                membership_type = row[0]
                                log.info(f"[LOGIN] membership_type={membership_type}")
                    except Exception as e:
                        log.error(f"[LOGIN] Error fetching membership_type label: {e} | { _exc() }")
                    finally:
                        conn.close()

            if not _has_premium_access(membership_id, membership_result):
                log.info("[LOGIN] Gate denied (not premium).")
                st.session_state["np_flash_msg_shown"] = True
                st.success("Logged in successfully.")  # UX confirmation of JWT
                st.error(
                    "For SIP Members Only.  Please contact us for a free 1-week trial."
                )
                st.stop()
        else:
            log.info("[LOGIN] REQUIRE_MEMBERSHIP=0 → best-effort membership (no block).")
            tmp_result = check_membership(token)
            if isinstance(tmp_result, dict):
                membership_result = tmp_result
                if membership_result.get("matches"):
                    membership_id = str(membership_result["matches"][0]).strip()
                    log.info(f"[LOGIN] (best-effort) membership_id={membership_id}")
                if membership_id:
                    conn = create_db_connection()
                    if conn:
                        try:
                            with conn.cursor() as cur:
                                cur.execute("SELECT label FROM membership_types WHERE id = %s", (membership_id,))
                                row = cur.fetchone()
                                if row:
                                    membership_type = row[0]
                                    log.info(f"[LOGIN] (best-effort) membership_type={membership_type}")
                        except Exception as e:
                            log.error(f"[LOGIN] (best-effort) membership_type fetch error: {e} | { _exc() }")
                        finally:
                            conn.close()

        session_id = login_user(
            user_email,
            token_data.get("user_nicename"),
            token_data.get("user_display_name"),
            token,
            membership_id=membership_id,
            membership_type=membership_type
        )
        log.info(f"[LOGIN] App session created session_id={session_id}")

        key_prefix = f"{user_email}_{session_id}"
        st.session_state.update(
            {
                f"{key_prefix}_session_id": session_id,
                f"{key_prefix}_user_id": user_email,
                f"{key_prefix}_token": token,
                f"{key_prefix}_membership": membership_result or {},
                f"{key_prefix}_membership_id": membership_id,
                f"{key_prefix}_membership_type": membership_type,
            }
        )
        log.debug(f"[LOGIN] Session state set with prefix={key_prefix}")

        auth_data = {
            "user_email": user_email,
            "token": token,
            "session_id": session_id,
            "filters": {},
            "membership": membership_result or {},
            "membership_id": membership_id,
            "membership_type": membership_type,
        }
        _write_auth_cookie(auth_data)

        st.success("Logged in successfully!")
        (0.5)
        log.info("[LOGIN] Redirect → pages/opening.py")
        st.switch_page("pages/opening.py")
    else:
        log.warning("[LOGIN] Authentication failed (JWT error or incorrect credentials).")
        st.error("Incorrect username or password.")

st.markdown(
    """
<div style='margin-top:20px;margin-bottom:40px'>Not a Premium member? Please   <a href="https://coresight.com/contact/" target="_blank" style="text-decoration:none; color:#d6262f;">Contact Us</a> to request trial access.</div>
""",
    unsafe_allow_html=True
)

# Footer
include_html("footer.html")