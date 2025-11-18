# Version 2.2.1
import streamlit as st
import pandas as pd
import os
import logging
from dotenv import load_dotenv
import calendar
from st_aggrid import AgGrid, GridOptionsBuilder
import mysql.connector
from streamlit_cookies_controller import CookieController
from html_utils import include_html
import io
import base64, pathlib
from pathlib import Path
import streamlit.components.v1 as components
from auth_utils import require_auth
# removed duplicate: from st_aggrid.grid_options_builder import GridOptionsBuilder
from typing import Optional
from time import sleep
import json

require_auth()
load_dotenv()

st.set_page_config(
    page_title="Data Change Logs",
    layout="wide",
    page_icon = "https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
    initial_sidebar_state="collapsed"
)

STRICT_AUTH = os.getenv("AUTH_STRICT", "1") == "1"

st.set_option("client.showErrorDetails", False)


@st.dialog(" ")
def _trial_modal_opened():

    # 1) Encode your lock icon so we can place it inside the HTML card
    ICON = pathlib.Path(__file__).resolve().parent.parent / "assets" / "icons" / "lock.png"
    lock_b64 = base64.b64encode(ICON.read_bytes()).decode("utf-8")

    # 2) One HTML block: icon + text + CTA all INSIDE the bordered box
    st.markdown(f"""
    <div style="
        border:3px solid #d62e2f;
        border-radius:14px;
        padding:26px 24px;
        text-align:center;
        max-width:720px;
        margin:28px auto 0 auto;
    ">
      <img src="data:image/png;base64,{lock_b64}"
           alt="lock"
           style="width:54px;height:auto;display:block;margin:0 auto 14px auto;" />
      <h3 style="margin:0 0 10px 0;">For SIP Members Only</h3>
      <p style="margin:8px 0 18px 0;">
        Data downloads are exclusively accessible to SIP members and are excluded from trial access.
      </p>
      <a href="https://coresight.com/contact/" target="_blank" rel="noopener noreferrer"
         style="display:inline-block;background:#d62e2f;color:#fff;
                padding:10px 18px;border-radius:10px;text-decoration:none;font-weight:600;">
        Contact Us
      </a>
    </div>
    """, unsafe_allow_html=True)

def _has_premium(auth_cookie: Optional[dict]):
    if not isinstance(auth_cookie, dict):
        return None
    membership_id = auth_cookie.get("membership_id")
    if membership_id:
        return True
    m = auth_cookie.get("membership") or {}
    if m.get("has_access") and m.get("matches"):
        return True
    if m.get("has_access") is False or m.get("matches") == []:
        return False
    return None

try:
    cookie_controller = CookieController(key="auth_cookies") or {}
    auth_cookie = cookie_controller.get("auth_data")
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
                return json.loads(raw_cookie)          # ✅ requires import json
            if isinstance(raw_cookie, dict):
                return raw_cookie
        except Exception:
            return {}
        return {}

auth_cookie = load_auth_cookie()
# status = _has_premium(auth_cookie)

FREE_TRIAL_ID = {"100939", "325429"}
mid = str((auth_cookie or {}).get("membership_id") or "").strip()
is_free_trial = mid in FREE_TRIAL_ID

# 🔑 Single source of truth for table gating
# A user is premium only if they have premium access AND are not the free-trial id
# is_premium_user = (status is True) and (not is_free_trial)

# If you truly want to block non-premium completely, keep this.
# (Trial users won't hit this because `_has_premium` returns True when membership_id exists.)
# if status is False:
#     st.error(
#         "Sorry, Store Intelligence Platform is available for premium subscribers only. "
#         "Please contact us to know more details."
#     )
#     st.stop()

# Set up logging
LOG_LEVEL_STR = os.getenv("LOG_LEVEL") or 'INFO'
log_level = getattr(logging, LOG_LEVEL_STR, logging.INFO)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
)

@st.cache_data(show_spinner=False)
def fetch_data_change_log():
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    SSL_CA = os.getenv("SSL_CA")  # full path to ca.pem or None

    connection_params = {
        'user': DB_USER,
        'password': DB_PASSWORD,
        'host': DB_HOST,
        'database': DB_NAME
    }
    if os.getenv('ENABLE_SSL', 'false').lower() == 'true' and SSL_CA:
        connection_params['ssl_ca'] = SSL_CA
        logging.info("SSL connection enabled")
    else:
        logging.info("SSL connection not enabled")

    conn = None
    try:
        conn = mysql.connector.connect(**connection_params)

        query = """
            SELECT
                l.Release_Notes_Number,
                l.Year_Data_Change,
                l.Month_Data_Change,
                l.Day_Data_Change,
                l.Data_Release_Type,
                l.ParentName_Coresight,
                l.ChainName_Coresight,
                l.Sector_Coresight,
                l.Descriptive_Summary,
                l.Data_Update_Period,
                l.Previous_Count,
                l.Rectified_Count,
                l.Category_of_Release_Notes,
                l.Run_Identifier,
                l.Data_Inserted_At
            FROM data_change_log l
            left join parent_chain_names_data p
            on l.chainname_coresight = p.chainname_coresight
            where p.is_active = 1"""

        df = pd.read_sql(query, con=conn)

        # Month text to number for filtering
        month_map = {m: i for i, m in enumerate(calendar.month_name) if m}
        month_map.update({m: i for i, m in enumerate(calendar.month_abbr) if m})
        df['Month_Numeric'] = df['Month_Data_Change'].map(month_map)
        df['Year_Numeric'] = pd.to_numeric(df['Year_Data_Change'], errors='coerce')
        return df

    except Exception as e:
        logging.error(f"Error fetching data_change_log: {e}")
        raise
    finally:
        if conn:
            conn.close()

st.markdown("""
    <style>
    .block-container {
        max-width: 1300px !important;
        margin-left: auto !important;
        margin-right: auto !important;
        padding-left: 0 !important;
        padding-right: 0 !important;
    }
    .main {
        padding-left: 0 !important;
        padding-right: 0 !important;
    }
    </style>
""", unsafe_allow_html=True)

hide_streamlit_style = """
<style>
#MainMenu {visibility: hidden;}
header {padding-top: 0rem;} /* Adjusts padding for the header */
.css-1y0tads, .block-container {
    padding-top: 2.1rem !important;
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

st.markdown('<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">', unsafe_allow_html=True)

st.markdown("""
    <style>
    /* Remove ALL margin/padding from Streamlit app wrappers/content */
    .stApp, .main, .css-18e3th9, .css-1y0tads, main {
        padding: 0 !important;
        margin: 0 !important;
    }
    /* Remove top gutter above content for custom header */
    .css-18e3th9 {
        padding-top: 85px !important; /* match header height exactly */
        margin-top: 0 !important;
    }

    /* Remove Streamlit main bar/header/footer */
    header[data-testid="stHeader"],
    footer { display: none !important; }
    #MainMenu { visibility: hidden !important; }

    body, .stApp { background: #fff !important; }
    </style>
    """, unsafe_allow_html=True)

include_html("header.html")

st.markdown(
    """
    <style>
    span[data-baseweb="tag"] {
        background-color: #d62e2f !important;
        color: white !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

col1, col4  = st.columns([9,0.6]) 
with col1:
    st.title("Data Release Notes")
with col4:
    if st.button("Back", key="back", type="tertiary", icon=":material/arrow_back:"):
        from time import sleep
        sleep(1)

        try:
            # Parse the cookie into a dict (handles str/dict/None safely)
            auth_cookie = load_auth_cookie()
        except Exception:
            auth_cookie = {}

        return_page = ((auth_cookie or {}).get("filters") or {}).get("returnPage")
        if return_page:
            st.switch_page(f"pages/{return_page}.py")
        else:
            st.switch_page("pages/opening.py")


with st.spinner('Loading data, please wait...'):
    change_log_data = fetch_data_change_log()

# --- SESSION STATE INITIALIZATION ---
for key, default in [
    ("expand_filters", True),
    ("selected_chain_name", ["All"]),
    ("parent_chain_name", ["All"]),
    ("selected_sector_name", "All"),
    ("selected_data_release_type", "All"),
]:
    if key not in st.session_state:
        st.session_state[key] = default

top_col0,top_col1, top_col2 = st.columns([6,1,1])
with top_col1:
    if st.button("Reset Filters", key="reset_filters_btn", use_container_width=True, type="secondary"):
        # Reset all filter session states to default values
        st.session_state["selected_data_release_type"] = ["All"]
        
        # For date filters, we need to determine the latest year/month from the data
        year_options = (
            change_log_data['Year_Numeric']
            .dropna().astype(int).unique().tolist()
        )
        year_options.sort(reverse=True)
        latest_year = year_options[0] if year_options else None
        
        st.session_state["selected_year"] = latest_year
        st.session_state["selected_month"] = "All"  # Default to "All" for month
        
        st.session_state["selected_sector_name"] = ["All"]
        st.session_state["parent_chain_name"] = ["All"]
        st.session_state["selected_chain_name"] = ["All"]
        st.session_state["selected_release_note_category"] = "All"
        st.rerun()
with top_col2:
    all_expanded = st.session_state.get("expand_filters", True)
    if st.button("Close All Filters" if all_expanded else "Expand All Filters", key="all_filters_btn", use_container_width=True):
        st.session_state["expand_filters"] = not all_expanded
        st.rerun()

col1, col2, col3, col4, col5 = st.columns([1.2, 1.2, 1.2, 1.3, 1.7])

# ---- RELEASE TYPE FILTER (was CATEGORY OF UPDATE) ----
with col1:
    with st.expander("Release Type", expanded=st.session_state["expand_filters"]):
        data_release_types = change_log_data["Data_Release_Type"].dropna().unique().tolist()
        data_release_types.sort()
        data_release_types = ["All"] + data_release_types if data_release_types else ["All"]

        # Initialization
        current_release_types = st.session_state.get("selected_data_release_type", ["All"])
        if "All" in current_release_types:
            current_release_types = ["All"]
        else:
            valid_selected = [x for x in current_release_types if x in data_release_types]
            current_release_types = valid_selected if valid_selected else ["All"]

        selected_data_release_type = st.multiselect(
            "Select Release Type",
            data_release_types,
            default=current_release_types,
            max_selections=len(data_release_types),
            help="Tip: Please remove All if selecting other options",
            key="data_release_type_multiselect"
        )

        # 'All' logic
        if len(selected_data_release_type) > 1 and "All" in selected_data_release_type:
            if selected_data_release_type[-1] == "All":
                selected_data_release_type = ["All"]
            else:
                selected_data_release_type = [x for x in selected_data_release_type if x != "All"]
        elif not selected_data_release_type:
            selected_data_release_type = ["All"]

        if selected_data_release_type != current_release_types:
            st.session_state["selected_data_release_type"] = selected_data_release_type
            st.rerun()

# --- APPLY RELEASE TYPE FILTER ---
if "All" in st.session_state["selected_data_release_type"]:
    type_filtered_data = change_log_data
else:
    type_filtered_data = change_log_data[
        change_log_data['Data_Release_Type'].isin(st.session_state["selected_data_release_type"])
    ]

# ---- DATE FILTER (SECOND) ----
with col2:
    with st.expander("Select Date of Release Notes", expanded=st.session_state["expand_filters"]):
        # Year options
        year_options = (
            type_filtered_data['Year_Numeric']
            .dropna().astype(int).unique().tolist()
        )
        year_options.sort(reverse=True)
        
        # Always set default to latest year available
        latest_year = year_options[0] if year_options else None
        if "selected_year" not in st.session_state or st.session_state["selected_year"] not in year_options:
            st.session_state["selected_year"] = latest_year
        
        selected_year = st.selectbox(
            "Select Year",
            options=year_options,
            index=year_options.index(st.session_state["selected_year"]) if st.session_state.get("selected_year") in year_options else 0,
            key="year_selectbox"
        )
        st.session_state["selected_year"] = selected_year
        
        # Month options for selected year + All
        month_options = (
            type_filtered_data[type_filtered_data['Year_Numeric']==selected_year]['Month_Data_Change']
            .dropna().unique().tolist()
        )
        # Sort months in calendar order, latest first
        month_options = sorted(
            month_options,
            key=lambda m: list(calendar.month_name).index(m) if m in calendar.month_name else 99,
            reverse=True
        )
        # Add "All" at the top
        month_options = ["All"] + month_options

        # Set default selection to latest month (if exists), or "All"
        latest_month = month_options[1] if len(month_options) > 1 else "All"  # 0 is "All"
        if "selected_month" not in st.session_state or st.session_state["selected_month"] not in month_options:
            st.session_state["selected_month"] = latest_month
            st.rerun()

        selected_month = st.selectbox(
            "Select Month",
            options=month_options,
            index=month_options.index(st.session_state["selected_month"]),
            key="month_selectbox"
        )
        st.session_state["selected_month"] = selected_month

# --- DATA FILTERED BY DATE ---
if st.session_state.get("selected_month") == "All":
    date_filtered_data = type_filtered_data[
        type_filtered_data['Year_Numeric'] == st.session_state.get("selected_year")
    ]
else:
    date_filtered_data = type_filtered_data[
        (type_filtered_data['Year_Numeric'] == st.session_state.get("selected_year")) & 
        (type_filtered_data['Month_Data_Change'] == st.session_state.get("selected_month"))
    ]

# ---- SECTOR FILTER (make multi-select) ----
with col3:
    logging.info("--- SECTOR SECTION START ---")
    with st.expander("Sector", expanded=st.session_state["expand_filters"]):
        # Get sector names from date_filtered_data
        sector_names = date_filtered_data["Sector_Coresight"].dropna().unique().tolist()

        # Separate "Other"/"Others" at end
        others_items = [s for s in sector_names if s and s.lower() in ["other", "others"]]
        regular_items = [s for s in sector_names if not (s and s.lower() in ["other", "others"])]
        regular_items = sorted(regular_items, key=lambda x: x.lower())
        sector_names = ["All"] + regular_items + others_items

        current_sectors = st.session_state.get("selected_sector_name", ["All"])
        if "All" in current_sectors:
            current_sectors = ["All"]
        else:
            valid_selected = [s for s in current_sectors if s in sector_names]
            current_sectors = valid_selected if valid_selected else ["All"]

        selected_sector_name = st.multiselect(
            "Select Sector",
            sector_names,
            default=current_sectors,
            max_selections=len(sector_names),
            help="Tip: Please remove All if selecting other options",
            key="sector_multiselect"
        )
        # 'All' logic
        if len(selected_sector_name) > 1 and "All" in selected_sector_name:
            if selected_sector_name[-1] == "All":
                selected_sector_name = ["All"]
            else:
                selected_sector_name = [s for s in selected_sector_name if s != "All"]
        elif not selected_sector_name:
            selected_sector_name = ["All"]

        if selected_sector_name != current_sectors:
            st.session_state["selected_sector_name"] = selected_sector_name
            st.session_state["parent_chain_name"] = ["All"]
            st.session_state["selected_chain_name"] = ["All"]
            st.rerun()
    logging.info("--- SECTOR SECTION END ---")

if "All" in st.session_state["selected_sector_name"]:
    sector_filtered_data = date_filtered_data
else:
    sector_filtered_data = date_filtered_data[
        date_filtered_data['Sector_Coresight'].isin(st.session_state["selected_sector_name"])
    ]

# ---- RETAILER FILTER (FOURTH) ----
with col4:
    logging.info("--- RETAILERS SECTION START ---")
    # Use sector_filtered_data
    sector_filtered_data['ParentName_Coresight'] = sector_filtered_data['ParentName_Coresight'].where(
        pd.notna(sector_filtered_data['ParentName_Coresight']),
        "No Parent Retailer"
    )
    parent_names = sector_filtered_data['ParentName_Coresight'].unique().tolist()
    parent_names.sort()
    if "No Parent Retailer" in parent_names:
        parent_names.remove("No Parent Retailer")
        parent_names.append("No Parent Retailer")
    parent_names = ["All"] + parent_names

    current_parent = st.session_state.get("parent_chain_name", ["All"])
    if "All" in current_parent:
        current_parent = ["All"]
    else:
        valid_selected = [p for p in current_parent if p in parent_names]
        current_parent = valid_selected if valid_selected else ["All"]

    with st.expander("Retailers", expanded=st.session_state.get("expand_filters", True)):
        parent_chain_name = st.multiselect(
            "Select Company Name", 
            parent_names, 
            default=current_parent,
            max_selections=10, 
            help="Tip: Please remove All if selecting other options", 
            key="multiselect_parent"
        )

        # "All" handling
        if len(parent_chain_name) > 1 and "All" in parent_chain_name:
            if parent_chain_name[-1] == "All":
                parent_chain_name = ["All"]
            else:
                parent_chain_name = [p for p in parent_chain_name if p != "All"]
        elif not parent_chain_name:
            parent_chain_name = ["All"]

        # Sync session_state and reset dependent filters
        if parent_chain_name != current_parent:
            st.session_state["parent_chain_name"] = parent_chain_name
            st.session_state["selected_chain_name"] = ["All"]
            st.rerun()

        st.markdown(
            """
            <div style='text-align: center; font-size: 0.9rem; font-weight: 500; margin-top: -0.4rem; margin-bottom: -0.2rem; color: #444;'>or</div>
            """,
            unsafe_allow_html=True
        )

        # Apply filter: if "All", don't filter by parent, else filter
        if "All" in parent_chain_name or not parent_chain_name:
            filtered_for_chains = sector_filtered_data
        else:
            filtered_for_chains = sector_filtered_data[sector_filtered_data["ParentName_Coresight"].isin(parent_chain_name)]

        # ------------ Banner/Brand (Chain) filter ------------
        chain_names = sorted(
            [x for x in filtered_for_chains['ChainName_Coresight'].dropna().unique().tolist() if x],
            key=lambda x: str(x)
        )
        chain_names = ["All"] + chain_names

        current_chain = st.session_state.get("selected_chain_name", ["All"])
        if "All" in current_chain:
            current_chain = ["All"]
        else:
            valid_chains = [c for c in current_chain if c in chain_names]
            current_chain = valid_chains if valid_chains else ["All"]

        selected_chain_name = st.multiselect(
            "Select Banner/Brand", 
            chain_names, 
            default=current_chain,
            max_selections=10, 
            help="Tip: Please remove All if selecting other options",
            key="multiselect_chain"
        )
        if len(selected_chain_name) > 1 and "All" in selected_chain_name:
            if selected_chain_name[-1] == "All":
                selected_chain_name = ["All"]
            else:
                selected_chain_name = [c for c in selected_chain_name if c != "All"]
        elif not selected_chain_name:
            selected_chain_name = ["All"]

        if selected_chain_name != current_chain:
            st.session_state["selected_chain_name"] = selected_chain_name
            st.rerun()

# ---- APPLY RETAILER FILTERS ----
retailer_filtered_data = sector_filtered_data.copy()
if "All" not in st.session_state["parent_chain_name"] and st.session_state["parent_chain_name"]:
    retailer_filtered_data = retailer_filtered_data[retailer_filtered_data['ParentName_Coresight'].isin(st.session_state["parent_chain_name"])]
if "All" not in st.session_state["selected_chain_name"] and st.session_state["selected_chain_name"]:
    retailer_filtered_data = retailer_filtered_data[retailer_filtered_data['ChainName_Coresight'].isin(st.session_state["selected_chain_name"])]

# ---- RELEASE NOTES CATEGORY FILTER (FIFTH) ----
with col5:
    with st.expander("Release Notes Category", expanded=st.session_state["expand_filters"]):
        release_note_categories = retailer_filtered_data["Category_of_Release_Notes"].dropna().unique().tolist()
        release_note_categories.sort()
        
        # EXCLUDE the specific unwanted category
        release_note_categories = [cat for cat in release_note_categories]
        
        release_note_categories = ["All"] + release_note_categories if release_note_categories else ["All"]

        if "selected_release_note_category" not in st.session_state or st.session_state["selected_release_note_category"] not in release_note_categories:
            st.session_state["selected_release_note_category"] = "All"

        selected_release_note_category = st.selectbox(
            "Select Release Notes Category",
            release_note_categories,
            index=release_note_categories.index(st.session_state["selected_release_note_category"]),
            key="release_note_category_selectbox"
        )
        if selected_release_note_category != st.session_state["selected_release_note_category"]:
            st.session_state["selected_release_note_category"] = selected_release_note_category
            st.rerun()

# --- APPLY RELEASE NOTES CATEGORY FILTER (Final filtered data) ---
if st.session_state["selected_release_note_category"] != "All":
    filtered_data = retailer_filtered_data[
        retailer_filtered_data['Category_of_Release_Notes'] == st.session_state["selected_release_note_category"]
    ]
else:
    filtered_data = retailer_filtered_data

# Create the merged "Period" column
filtered_data["Period"] = filtered_data["Month_Data_Change"].astype(str) + " " + filtered_data["Year_Data_Change"].astype(str)

# Columns to keep and rename mapping
columns_to_display = [
    "Period",
    "Data_Release_Type",
    "ParentName_Coresight",
    "ChainName_Coresight",
    "Data_Update_Period",
    "Previous_Count",
    "Rectified_Count",
    "Category_of_Release_Notes",
    "Descriptive_Summary",
]

columns_rename = {
    "Period": "Release Month",
    "Data_Release_Type": "Release Type",
    "ParentName_Coresight": "Company Name",
    "ChainName_Coresight": "Banner/Brand Name",
    "Data_Update_Period": "Data Update Period",
    "Previous_Count": "Previous Count",
    "Rectified_Count": "Rectified Count",
    "Category_of_Release_Notes": "Release Notes Category",
    "Descriptive_Summary": "Summary",
}

display_df = filtered_data[columns_to_display].rename(columns=columns_rename)

st.markdown("<hr>", unsafe_allow_html=True)

col1, col4  = st.columns([7,1.05]) 
with col1:
    st.subheader('Data Release Notes Table')
with col4:
    excel_buffer = io.BytesIO()
    if not display_df.empty:
        # ✅ ALLOW download for NON–free-trial users; block free trials (same pattern as openings.py)
        if not is_free_trial:
            display_df.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)

            st.download_button(
                label="Download Data",
                data=excel_buffer,
                file_name=f"data_change_logs_{st.session_state.get('selected_year')}_{st.session_state.get('selected_month')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_changelog_xlsx",
                icon=":material/download:"
            )
        else:
            if st.button("Download Data", key="download_opened_blocked", use_container_width=True):
                _trial_modal_opened()

if display_df.empty:
    st.info("No data matches your filters.")
else:
    dup = display_df['Data Update Period'].astype(str).str.strip()
    s = pd.to_datetime(dup, format='%B %Y', errors='coerce') \
            .fillna(pd.to_datetime(dup, format='%b %Y', errors='coerce'))

    display_df['Data Update Period Sort'] = s

    # Filter out 2018; keep NaT (unparsed) rows
    mask = display_df['Data Update Period Sort'].dt.year.ne(2018) | display_df['Data Update Period Sort'].isna()
    display_df = display_df[mask].copy()

    # Create helper sort columns
    display_df['Release Month Sort'] = pd.to_datetime(display_df['Release Month'],format='%B %Y', errors='coerce')
    display_df['Data Update Period Sort'] = pd.to_datetime(display_df['Data Update Period'],format='%B %Y', errors='coerce')

    display_df = display_df.sort_values(
        by=['Release Type','Release Month Sort', 'Data Update Period Sort','Banner/Brand Name'],
        ascending=[True, False, False, True],
        na_position='last'
    ).drop(columns=['Release Month Sort', 'Data Update Period Sort'])

    display_df['Release Month'] = display_df['Release Month'].astype(str)
    display_df['Data Update Period'] = display_df['Data Update Period'].astype(str)

    # ======== TABLE RENDERING (mutually exclusive) ========
    if not is_free_trial:
        # PREMIUM GRID (non–free-trial users)
        gb = GridOptionsBuilder.from_dataframe(display_df)
        gb.configure_default_column(resizable=True, filterable=True, sortable=True)
        gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=25)
        gb.configure_column("Release Month", width=50)
        gb.configure_column("Release Type", width=50)
        gb.configure_column("Company Name", width=50)
        gb.configure_column("Banner/Brand Name", width=50)
        gb.configure_column("Data Update Period", width=50)
        gb.configure_column("Previous Count", width=50)
        gb.configure_column("Rectified Count", width=50)
        gb.configure_column("Release Notes Category", width=50)
        gb.configure_column("Summary", width=50, wrapText=False, autoHeight=True)

        grid_options = gb.build()

        AgGrid(
            display_df.reset_index(drop=True),
            gridOptions=grid_options,
            enable_enterprise_modules=False,
            theme="alpine",
            fit_columns_on_grid_load=False,
            reload_data=True,
            update_mode="NO_UPDATE"
        )
        st.caption(f"Showing {len(display_df)} records for {st.session_state.get('selected_month', 'month')} {st.session_state.get('selected_year', 'year')}")
    else:
        # TRIAL GRID + OVERLAY (free trials see overlay + blocked download)
        GRID_H = 600  # must match your AgGrid height
        df_for_grid = display_df

        ICON = pathlib.Path(__file__).resolve().parent.parent / "assets" / "icons" / "lock.png"
        lock_b64 = base64.b64encode(ICON.read_bytes()).decode("utf-8")

        gb = GridOptionsBuilder.from_dataframe(df_for_grid)
        grid_options = gb.build()

        AgGrid(
            df_for_grid,
            gridOptions=grid_options,
            height=GRID_H,
            key="opened_grid",
        )

        st.markdown(f"""
        <style>
          .grid-overlay-flag[data-target="opened"] {{
            position: relative;
            height: {GRID_H}px;
            margin-top: -{GRID_H}px;
            display: flex;
            align-items: center;
            justify-content: center;
            flex-direction: column;
            gap: 20px;
            background: rgba(255,255,255,0.55);
            backdrop-filter: blur(4px);
            -webkit-backdrop-filter: blur(4px);
            border-radius: 6px;
            z-index: 1000;
            pointer-events: auto;
            text-align: center;
            font-family: inherit;
          }}
          .grid-overlay-flag .lock {{
            width: 48px; height: 48px; object-fit: contain; margin: 0 auto;
          }}
          .grid-overlay-flag .title {{
            font-size: 40px; font-weight: 900; color: #111;
          }}
          .grid-overlay-flag .cta {{
            display: inline-block;
            padding: 14px 28px;
            border: none; border-radius: 10px;
            background: #E53935; color: #fff !important;
            font-weight: 800; font-size: 18px; text-decoration: none;
            cursor: pointer;
            box-shadow: 0 2px 8px rgba(0,0,0,0.15);
          }}
          .grid-overlay-flag .cta:hover {{ background:#c62828; }}
        </style>
        <div class="grid-overlay-flag" data-target="opened">
          <img src="data:image/png;base64,{lock_b64}" class="lock" alt="Locked"/>
          <div class="title">For SIP Members Only</div>
          <p style="margin:8px 0 18px 0;">
          Individual Store data is exclusively accessible to SIP members and is not available for trial users.
          </p>
          <a class="cta" href="https://coresight.com/contact/" target="_blank">Contact Us</a>
        </div>
        """, unsafe_allow_html=True)

    

# Footer
st.markdown("---")
# Determine the environment and set the appropriate URL
try:
    current_url = st.context.headers.get("host", "")
    is_staging = "stage" in current_url.lower()
except:
    is_staging = False

if is_staging:
    overview_url = "https://stage3.coresight.com/store-intelligence-platform-overview/"
else:
    overview_url = "https://www.coresight.com/store-intelligence-platform-overview/"

st.markdown(
    f"""
    <p style='color: gray; font-size: small;'>
    Disclaimer: Certain data are derived from calculations that use data licensed from third parties, including ChainXY. 
    Coresight Research has made substantial efforts to clean the data and identify potential issues. However, changes to retailers' store locators 
    may impact database-sourced data. See our 
    <a href="{overview_url}" target="_blank">Overview</a> 
    document and <a href="/changelogs" target="_self">Data Release Notes</a>
    for more details.
    </p>
    """,
    unsafe_allow_html=True,
)

include_html("footer.html")
