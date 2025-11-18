# Version 2.2.2
try:
    import streamlit as st
    import pandas as pd
    import mysql.connector
    import plotly.express as px
    import warnings
    import math
    import matplotlib.pyplot as plt
    import plotly.graph_objects as go
    import os
    from datetime import timedelta
    from mysql.connector import pooling
    from concurrent.futures import ThreadPoolExecutor
    import logging
    from time import sleep
    from datetime import datetime, date
    import logging
    from sqlalchemy import create_engine
    import calendar
    import time
    from dotenv import load_dotenv
    import datetime as dt
    import requests
    import io
    from PIL import Image
    import streamlit as st
    import plotly.express as px
    import streamlit as st
    import pandas as pd
    from datetime import date
    import calendar
    import plotly.express as px
    import plotly.graph_objects as go
    import colorsys
    from html_utils import include_html
    from streamlit.components.v1 import html
    from sqlalchemy import create_engine
    from datetime import datetime
    import math
    from st_aggrid import AgGrid
    from streamlit_cookies_controller import CookieController
    from streamlit_extras.switch_page_button import switch_page
    import base64, pathlib
    from pathlib import Path
    import streamlit.components.v1 as components
    from st_aggrid.grid_options_builder import GridOptionsBuilder
    from auth_utils import logout,require_auth,make_json_safe,get_current_domain
    from typing import Optional
    import json
    import time
    require_auth()
    load_dotenv()

    st.set_page_config(
        page_title="Active Stores",  # Title of the app
        layout="wide",                 # Use the 'wide' layout
        page_icon = "https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
        initial_sidebar_state="collapsed"  # Sidebar state (optional)
    )

    # Add cache control headers to prevent browser caching
    st.markdown("""
        <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
        <meta http-equiv="Pragma" content="no-cache">
        <meta http-equiv="Expires" content="0">
    """, unsafe_allow_html=True)
    st.markdown("""
                <style>
                .st-emotion-cache-vlxhtx .e1lln2w83 {
                        width: 100%;
                        max-width: 100%;
                        position: relative;
                        display: flex;
                        flex: 1 1 0%;
                    flex-direction: column;
                    gap: 0.2rem;
                    }
            </style>
    """, unsafe_allow_html=True)
    STRICT_AUTH = os.getenv("AUTH_STRICT", "1") == "1"


    cookie_controller = CookieController(key="auth_cookies")

    raw = cookie_controller.get("auth_data")
    try:
        auth_cookie = json.loads(raw) if isinstance(raw, str) else (raw if isinstance(raw, dict) else {})
    except Exception:
        auth_cookie = {}

    # Always ensure we have a dict for storing user-level filters
    user_filters = auth_cookie.setdefault("filters", {})
    domain = get_current_domain()
    def save_auth_cookie():
        try:
            cookie_controller.set(
                "auth_data",
                json.dumps(auth_cookie),                    # must be a string
                expires=datetime.utcnow() + timedelta(days=30),
                path="/",
                domain=domain,
            )
        except TypeError:
            # Fallback for even older signatures that don't take expires/path
            try:
                cookie_controller.set(
                    "auth_data",
                    json.dumps(auth_cookie),                    # must be a string
                    expires=datetime.utcnow() + timedelta(days=30),
                    path="/",
                    domain=domain,
                )
            except Exception as e:
                logging.warning(f"cookie write failed (fallback): {e}")
        except Exception as e:
            logging.warning(f"cookie write failed: {e}")    

    st.set_option("client.showErrorDetails", False)


    @st.dialog(" ")
    def _trial_modal_active():

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



    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    LOG_LEVEL_STR = os.getenv("LOG_LEVEL") or 'INFO'
    SSL_CA = os.getenv("SSL_CA")  # Full path to your ca.pem file

    log_level = getattr(logging, LOG_LEVEL_STR, logging.INFO) # default to INFO if invalid level

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    def get_synchronized_date_range(data, default_months_back=12, label=""):
            data['Period'] = pd.to_datetime(data['Period'])
            min_date = data['Period'].min().date()
            max_date = data['Period'].max().date()
            
            # Fix for exact 12 months calculation
            default_start = (max_date.replace(day=1) - pd.DateOffset(months=default_months_back-1))
            default_start = default_start.date()
            default_start = max(min_date, default_start)  # Ensure it's not before min_date

            qparams = st.query_params
            start_str = qparams.get("start_date", [default_start.isoformat()])
            end_str = qparams.get("end_date", [max_date.isoformat()])

            try:
                start_date = pd.to_datetime(start_str).date()
            except Exception as e:
                start_date = default_start

            try:
                end_date = pd.to_datetime(end_str).date()
            except Exception as e:
                end_date = max_date

            # Clamp to bounds
            start_date = max(start_date, min_date)
            end_date = min(end_date, max_date)

            return (start_date, end_date), min_date, max_date

    # Automatically expand filters on first load or when navigating
    if "expand_filters" not in st.session_state:
        st.session_state["expand_filters"] = True

    # try:
    #     cookie_controller = CookieController(key="auth_cookies")
    # except Exception:
    #     cookie_controller = {}

    # try:
    #     auth_cookie = cookie_controller.get("auth_data")
    # except Exception:
    #     auth_cookie = {}

    # try:
    #     cookie_controller = CookieController(key="auth_cookies")
    # except Exception:
    #     cookie_controller = {}

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

    auth_cookie = load_auth_cookie()


    FREE_TRIAL_ID = {"100939", "325429"}
    mid = str((auth_cookie or {}).get("membership_id") or "").strip()
    is_free_trial = mid in FREE_TRIAL_ID

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

    # -- Layout for buttons --
    col_space, col4, col1, col2, col3  = st.columns([56, 14, 14, 16, 8]) 
    user_filters = auth_cookie.get("filters", {}) if isinstance(auth_cookie, dict) else {}
    with col4:
        if auth_cookie.get("membership_type") == "Employee":
            is_staging = os.getenv("ENVIRONMENT", "").lower() == "staging"
        
            if is_staging:
                overview_url = "https://sip-portal-stg.coresight.com/app_release_portal"
            else:
                overview_url = "https://sip-portal.coresight.com/app_release_portal"
            st.link_button("App Release Portal",
                overview_url,
                type="tertiary", 
                icon=":material/release_alert:"
            )
    with col1:
        if st.button("Data Release Notes", key="go_to_changelog", type="tertiary", icon=":material/history_2:"):
            user_filters["returnPage"] = "active"
            auth_cookie["filters"] = user_filters
            save_auth_cookie()
            st.switch_page("pages/changelogs.py")  # use the script name without the .py

    with col2:
        # Determine URL based on environment
        is_staging = os.getenv("ENVIRONMENT", "").lower() == "staging"
        
        if is_staging:
            overview_url = "https://stage3.coresight.com/store-intelligence-platform-overview/"
        else:
            overview_url = "https://www.coresight.com/store-intelligence-platform-overview/"
        
        # Use st.link_button to open in new tab
        st.link_button(
            "Overview/Retailer List",
            overview_url,
            type="tertiary",
            icon=":material/help:"
        )

    with col3:
        if st.button(
            "Log Out",
            key="Logout",
            type="tertiary",
            icon=":material/logout:"
        ):
            logout()

    warnings.filterwarnings("ignore")

    today = dt.date.today()

    # Check if we've cleared cache today
    if "last_clear_date" not in st.session_state:
        st.session_state.last_clear_date = today

    # If it's a new day, clear cache
    if st.session_state.last_clear_date != today:
        st.cache_data.clear()
        st.session_state.last_clear_date = today
        st.toast("Cache cleared at midnight")

    # st.cache_data.clear()
    @st.cache_data(show_spinner=False)
    def fetch_data():
        start_total = time.time()
        
        connection_params = {
            # 'user': os.getenv('DB_USER'),
            # 'password': os.getenv('DB_PASSWORD'),
            # 'host': os.getenv('DB_HOST'),
            # 'database': os.getenv('DB_NAME')

            'user': DB_USER,
            'password': DB_PASSWORD,
            'host': DB_HOST,
            'database': DB_NAME
        }

        if os.getenv('ENABLE_SSL', 'false').lower() == 'true' and os.getenv('SSL_CA'):
            connection_params['ssl_ca'] = os.getenv('SSL_CA')
            logging.info("SSL connection enabled")
        else:
            logging.info("SSL connection not enabled")

        conn = mysql.connector.connect(**connection_params)
        cursor = conn.cursor()

        cursor.execute("SELECT MAX(Period) FROM all_active_cy")
        max_cy = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(Period) FROM all_active_py")
        max_py = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        max_period = max(pd.to_datetime(max_cy), pd.to_datetime(max_py)).date()
        twelve_months_back = (max_period - pd.DateOffset(months=11)).replace(day=1).date()
        
        logging.info(f"[fetch_data] Database date range: {twelve_months_back} to {max_period}")

        # Step 2: Check query parameters
        qparams = st.query_params
        start_date_param = qparams.get("start_date", [None])
        end_date_param = qparams.get("end_date", [None])
        
        # Initialize with database-derived dates (last 12 months)
        query_start_date = twelve_months_back
        query_end_date = max_period
        
        # Initialize display dates (may be different from query dates)
        display_start_date = twelve_months_back
        display_end_date = max_period

        if start_date_param and end_date_param:
            try:
                requested_start = pd.to_datetime(start_date_param).date()
                requested_end = pd.to_datetime(end_date_param).date()
                
                # Adjust query dates based on requested range
                if requested_start < twelve_months_back:
                    # If requested start is before our 12-month window, expand query
                    query_start_date = requested_start
                    logging.info(f"[fetch_data] Expanding query start to {query_start_date} (before 12-month window)")
                
                if requested_end > max_period:
                    # If requested end is after max period, cap at max period
                    query_end_date = max_period
                    logging.warning(f"[fetch_data] Requested end {requested_end} is after max period, capping at {max_period}")
                else:
                    query_end_date = requested_end
                    
                # Set display dates to exactly what was requested
                display_start_date = requested_start
                display_end_date = requested_end
                
                logging.info(f"[fetch_data] Using query params - Display range: {display_start_date} to {display_end_date}")
                logging.info(f"[fetch_data] Actual query range: {query_start_date} to {query_end_date}")
                
            except Exception as e:
                logging.warning(f"[fetch_data] Invalid query params. Using default dates. Error: {e}")
        
        # Step 3: Define queries to fetch all needed data
        base_columns = """
            c.ChainName_Coresight, Label, c.ParentName_Coresight, Address, Address2, City, MsaName,
            PostalCode, State, Country, c.Sector_Coresight, Period, Population, UpdateCycle
        """

        query_cy = f"""
            SELECT {base_columns}
            FROM all_active_cy c
            LEFT JOIN parent_chain_names_data p 
            ON c.chainname_coresight = p.chainname_coresight
            WHERE
            (
              (c.ChainName_Coresight IS NULL OR c.ChainName_Coresight NOT IN ('Hoka', 'Sephora', 'Lululemon Athletica','Finish Line','Sunglass Hut',"Carter's",'Marc Jacobs','Bulgari', "Dick's Sporting Goods", 'CVS Pharmacy', 'Circle K', 'Aerie', 'American Eagle Outfitters','99 Cents Only Stores',
              '7-Eleven','Anthropologie','Babies"R"Us','Balenciaga','Bottega Veneta','Burberry','Cartier','Century 21','Chanel','Christian Dior',"Claire’s",'Coach',
              "Conn's",'Converse','Deciem','Dolce & Gabbana','Fendi','Giant Eagle','Givenchy','Gucci','Hallmark','Hy-Vee','James Avery Artisan Jewelry',
              "L'Occitane",'LensCrafters',"Levi's",'Lord & Taylor','Louis Vuitton','LUSH','Marc Jacobs','Massimo Dutti','Moncler','Pandora','Prada','Ralph Lauren',
              'Ted Baker','The North Face','Under Armour','Williams Sonoma','Yves Saint laurent','Free People'))
              OR (c.ChainName_Coresight = 'Hoka' AND LOWER(c.storename) LIKE '%hoka%')
              OR (c.ChainName_Coresight = 'Sephora' AND LOWER(c.storename) NOT LIKE '%penney%' AND LOWER(c.storename) NOT LIKE '%kohl%')
              OR (c.ChainName_Coresight = 'Lululemon Athletica' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Finish Line' AND LOWER(c.storename) NOT LIKE '%macy%' AND LOWER(c.storename) NOT LIKE '%jd sports%')
              AND (c.ChainName_Coresight = 'Finish Line' AND COALESCE(c.storetype, '') NOT IN ('JD Sports'))
              OR (c.ChainName_Coresight = 'Sunglass Hut' AND LOWER(c.storename) NOT LIKE '%macy%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = "Carter's" AND COALESCE(c.storetype, '') <> 'Oshkosh')
              OR (c.ChainName_Coresight = 'Marc Jacobs' AND LOWER(c.storename) NOT LIKE '%macy%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%'
                AND LOWER(storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE 'bookmarc%')
              OR (c.ChainName_Coresight = "Bulgari" AND COALESCE(c.storetype, '') NOT IN ('Official Retailers','Dept. Store (Ds)'))
              OR (c.ChainName_Coresight = "Dick's Sporting Goods" AND LOWER(c.storename) NOT LIKE '%warehouse%' AND LOWER(c.storename) NOT LIKE '%temporary%' AND LOWER(storename) NOT LIKE '%going%')
              OR (c.ChainName_Coresight = "CVS Pharmacy" AND LOWER(c.storename) NOT LIKE '%target%' AND LOWER(c.storename) NOT LIKE '%schnucks%')
              OR (c.ChainName_Coresight = "Circle K" AND LOWER(c.storename) NOT LIKE '%car wash%' AND LOWER(c.storename) NOT LIKE '%holiday station%' AND LOWER(c.storename) NOT LIKE '%gas station%' AND LOWER(c.storename) NOT LIKE '%on the run%' AND LOWER(c.storetype) NOT LIKE '%holiday station%')
              OR (c.ChainName_Coresight = 'Aerie' AND LOWER(c.storename) NOT IN ('american eagle', 'american eagle store', 'offline', 'american eagle , offline store', 'offline store', 'american eagle & offline', 'american eagle outlet', 'american eagle , offline outlet', 'american eagle clearance store', 'american eagle , offline', 'american eagle denim deli'))
              OR (c.ChainName_Coresight = 'American Eagle Outfitters' AND LOWER(c.storename) NOT IN ('aerie - closed boulevard mall', 'offline', 'offline store', 'offline store - closed', 'aerie & offline', 'aerie store', 'aerie clearance store', 'aerie - santa rosa plaza', 'aerie outlet', 'aerie outlet - closed', 'aerie , offline store', 'aerie store - closed', 'unsubscribed', 'offline clearance store', 'aerie , offline', 'aerie streets at southpoint', 'aerie bangor mall', 'aerie crystal mall', 'aerie spring street', 'aerie lakeline mall', 'aerie south side works', 'aerie - closed spring street', 'aerie northlake mall', 'aerie los cerritos center', 'aerie annapolis mall', 'aerie green acres mall', 'aerie exton square mall', 'aerie anchorage fifth avenue mall', 'aerie fox river mall', 'aerie staten island mall', 'aerie charleston town center', 'aerie san francisco center', 'aerie the mall @ johnson city', 'aerie - west town mall', 'aerie the oaks', 'aerie - closed crystal mall', 'aerie park plaza mall', 'offline - mall of georgia', 'offline - natick mall'))
              OR (c.ChainName_Coresight = '7-Eleven' AND COALESCE(c.storetype, '') NOT IN ('Stripes','Speedway'))
              OR (c.ChainName_Coresight = 'Anthropologie' AND COALESCE(c.storetype, '') NOT IN ('Temp'))
              OR (c.ChainName_Coresight = 'Babies"R"Us' AND LOWER(c.storename) NOT LIKE '%kohl%')
              OR (c.ChainName_Coresight = 'Balenciaga' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Bottega Veneta' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Burberry' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Cartier' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Century 21' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Chanel' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Christian Dior' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = "Claire’s" AND LOWER(c.storename) NOT LIKE '%walmart%')
              OR (c.ChainName_Coresight = 'Coach' AND COALESCE(c.storetype, '') NOT IN ('Coffee Shop'))
              OR (c.ChainName_Coresight = "Conn's" AND LOWER(c.storename) NOT LIKE '%belk%')
              OR (c.ChainName_Coresight = 'Converse' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Deciem' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Dolce & Gabbana' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Fendi' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'Finish Line' AND COALESCE(c.storetype, '') NOT IN ('JD Sports'))
              OR (c.ChainName_Coresight = 'Giant Eagle' AND COALESCE(c.storetype, '') NOT IN ('WetGo'))
              OR (c.ChainName_Coresight = 'Givenchy' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'Gucci' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'Hallmark' AND LOWER(c.storename) NOT LIKE '%ace%' AND LOWER(c.storename) NOT LIKE '%pharmacy%' AND LOWER(c.storename) NOT LIKE '%health%')
              OR (c.ChainName_Coresight = 'Hy-Vee' AND COALESCE(c.storetype, '') NOT IN ('Pharmacy'))
              OR (c.ChainName_Coresight = 'James Avery Artisan Jewelry' AND LOWER(c.storename) NOT LIKE '%dillard%')
              OR (c.ChainName_Coresight = "L'Occitane" AND COALESCE(c.storetype, '') NOT IN ('NON_OWNED'))
              OR (c.ChainName_Coresight = 'LensCrafters' AND LOWER(c.storename) NOT LIKE '%macy%')
              OR (c.ChainName_Coresight = "Levi's" AND COALESCE(c.storetype, '') NOT IN ('Retail Partner'))
              OR (c.ChainName_Coresight = 'Lord & Taylor' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Louis Vuitton' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%' AND LOWER(c.storename) NOT LIKE '%temporary%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'LUSH' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = "Marc Jacobs" AND COALESCE(c.storetype, '') NOT IN ('Authorized retailer'))
              OR (c.ChainName_Coresight = 'Massimo Dutti' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Moncler' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%' AND LOWER(c.storename) NOT LIKE '%neiman%')
              OR (c.ChainName_Coresight = 'Pandora' AND LOWER(c.storename) NOT LIKE '%macy%')
              OR (c.ChainName_Coresight = 'Prada' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Ralph Lauren' AND LOWER(c.storename) NOT LIKE '%Ralph''s Coffee%')
              OR (c.ChainName_Coresight = 'Stop & Shop' AND COALESCE(c.storetype, '') NOT IN ('Pharmacy'))
              OR (c.ChainName_Coresight = 'Ted Baker' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'The North Face' AND LOWER(c.storename) NOT LIKE '%dick%' AND LOWER(c.storename) NOT LIKE '%oshkosh%')
              OR (c.ChainName_Coresight = 'Under Armour' AND COALESCE(c.storetype, '') NOT IN ('Brand House'))
              OR (c.ChainName_Coresight = 'Williams Sonoma' AND COALESCE(c.storetype, '') NOT IN ('STORE_IN_STORE'))
              OR (c.ChainName_Coresight = 'Yves Saint laurent' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.Chainname_Coresight = 'Free People' AND COALESCE(c.storetype, '') NOT IN ('FP Movement'))
              AND (c.ChainName_Coresight = 'Free People' AND LOWER(c.storename) NOT LIKE '%movement%')
            ) 
            AND p.is_active = 1
            AND Period BETWEEN '{query_start_date}' AND '{query_end_date}'
        """

        query_py = f"""
            SELECT {base_columns}
            FROM all_active_py c
            LEFT JOIN parent_chain_names_data p 
            ON c.chainname_coresight = p.chainname_coresight
            WHERE
            (
              (c.ChainName_Coresight IS NULL OR c.ChainName_Coresight NOT IN ('Hoka', 'Sephora', 'Lululemon Athletica','Finish Line','Sunglass Hut',"Carter's",'Marc Jacobs','Bulgari', "Dick's Sporting Goods", 'CVS Pharmacy', 'Circle K', 'Aerie', 'American Eagle Outfitters','99 Cents Only Stores',
              '7-Eleven','Anthropologie','Babies"R"Us','Balenciaga','Bottega Veneta','Burberry','Cartier','Century 21','Chanel','Christian Dior',"Claire’s",'Coach',
              "Conn's",'Converse','Deciem','Dolce & Gabbana','Fendi','Giant Eagle','Givenchy','Gucci','Hallmark','Hy-Vee','James Avery Artisan Jewelry',
              "L'Occitane",'LensCrafters',"Levi's",'Lord & Taylor','Louis Vuitton','LUSH','Marc Jacobs','Massimo Dutti','Moncler','Pandora','Prada','Ralph Lauren',
              'Ted Baker','The North Face','Under Armour','Williams Sonoma','Yves Saint laurent','Free People'))
              OR (c.ChainName_Coresight = 'Hoka' AND LOWER(c.storename) LIKE '%hoka%')
              OR (c.ChainName_Coresight = 'Sephora' AND LOWER(c.storename) NOT LIKE '%penney%' AND LOWER(c.storename) NOT LIKE '%kohl%')
              OR (c.ChainName_Coresight = 'Lululemon Athletica' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Finish Line' AND LOWER(c.storename) NOT LIKE '%macy%' AND LOWER(c.storename) NOT LIKE '%jd sports%')
              AND (c.ChainName_Coresight = 'Finish Line' AND COALESCE(c.storetype, '') NOT IN ('JD Sports'))
              OR (c.ChainName_Coresight = 'Sunglass Hut' AND LOWER(c.storename) NOT LIKE '%macy%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = "Carter's" AND COALESCE(c.storetype, '') <> 'Oshkosh')
              OR (c.ChainName_Coresight = 'Marc Jacobs' AND LOWER(c.storename) NOT LIKE '%macy%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%'
                AND LOWER(storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE 'bookmarc%')
              OR (c.ChainName_Coresight = "Bulgari" AND COALESCE(c.storetype, '') NOT IN ('Official Retailers','Dept. Store (Ds)'))
              OR (c.ChainName_Coresight = "Dick's Sporting Goods" AND LOWER(c.storename) NOT LIKE '%warehouse%' AND LOWER(c.storename) NOT LIKE '%temporary%' AND LOWER(storename) NOT LIKE '%going%')
              OR (c.ChainName_Coresight = "CVS Pharmacy" AND LOWER(c.storename) NOT LIKE '%target%' AND LOWER(c.storename) NOT LIKE '%schnucks%')
              OR (c.ChainName_Coresight = "Circle K" AND LOWER(c.storename) NOT LIKE '%car wash%' AND LOWER(c.storename) NOT LIKE '%holiday station%' AND LOWER(c.storename) NOT LIKE '%gas station%' AND LOWER(c.storename) NOT LIKE '%on the run%' AND LOWER(c.storetype) NOT LIKE '%holiday station%')
              OR (c.ChainName_Coresight = 'Aerie' AND LOWER(c.storename) NOT IN ('american eagle', 'american eagle store', 'offline', 'american eagle , offline store', 'offline store', 'american eagle & offline', 'american eagle outlet', 'american eagle , offline outlet', 'american eagle clearance store', 'american eagle , offline', 'american eagle denim deli'))
              OR (c.ChainName_Coresight = 'American Eagle Outfitters' AND LOWER(c.storename) NOT IN ('aerie - closed boulevard mall', 'offline', 'offline store', 'offline store - closed', 'aerie & offline', 'aerie store', 'aerie clearance store', 'aerie - santa rosa plaza', 'aerie outlet', 'aerie outlet - closed', 'aerie , offline store', 'aerie store - closed', 'unsubscribed', 'offline clearance store', 'aerie , offline', 'aerie streets at southpoint', 'aerie bangor mall', 'aerie crystal mall', 'aerie spring street', 'aerie lakeline mall', 'aerie south side works', 'aerie - closed spring street', 'aerie northlake mall', 'aerie los cerritos center', 'aerie annapolis mall', 'aerie green acres mall', 'aerie exton square mall', 'aerie anchorage fifth avenue mall', 'aerie fox river mall', 'aerie staten island mall', 'aerie charleston town center', 'aerie san francisco center', 'aerie the mall @ johnson city', 'aerie - west town mall', 'aerie the oaks', 'aerie - closed crystal mall', 'aerie park plaza mall', 'offline - mall of georgia', 'offline - natick mall'))
              OR (c.ChainName_Coresight = '99 Cents Only Stores' and c.period >= '2025-05-01')
              OR (c.ChainName_Coresight = '7-Eleven' AND COALESCE(c.storetype, '') NOT IN ('Stripes','Speedway'))
              OR (c.ChainName_Coresight = 'Anthropologie' AND COALESCE(c.storetype, '') NOT IN ('Temp'))
              OR (c.ChainName_Coresight = 'Babies"R"Us' AND LOWER(c.storename) NOT LIKE '%kohl%')
              OR (c.ChainName_Coresight = 'Balenciaga' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Bottega Veneta' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Burberry' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Cartier' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Century 21' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Chanel' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Christian Dior' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = "Claire’s" AND LOWER(c.storename) NOT LIKE '%walmart%')
              OR (c.ChainName_Coresight = 'Coach' AND COALESCE(c.storetype, '') NOT IN ('Coffee Shop'))
              OR (c.ChainName_Coresight = "Conn's" AND LOWER(c.storename) NOT LIKE '%belk%')
              OR (c.ChainName_Coresight = 'Converse' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Deciem' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Dolce & Gabbana' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Fendi' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'Finish Line' AND COALESCE(c.storetype, '') NOT IN ('JD Sports'))
              OR (c.ChainName_Coresight = 'Giant Eagle' AND COALESCE(c.storetype, '') NOT IN ('WetGo'))
              OR (c.ChainName_Coresight = 'Givenchy' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'Gucci' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'Hallmark' AND LOWER(c.storename) NOT LIKE '%ace%' AND LOWER(c.storename) NOT LIKE '%pharmacy%' AND LOWER(c.storename) NOT LIKE '%health%')
              OR (c.ChainName_Coresight = 'Hy-Vee' AND COALESCE(c.storetype, '') NOT IN ('Pharmacy'))
              OR (c.ChainName_Coresight = 'James Avery Artisan Jewelry' AND LOWER(c.storename) NOT LIKE '%dillard%')
              OR (c.ChainName_Coresight = "L'Occitane" AND COALESCE(c.storetype, '') NOT IN ('NON_OWNED'))
              OR (c.ChainName_Coresight = 'LensCrafters' AND LOWER(c.storename) NOT LIKE '%macy%')
              OR (c.ChainName_Coresight = "Levi's" AND COALESCE(c.storetype, '') NOT IN ('Retail Partner'))
              OR (c.ChainName_Coresight = 'Lord & Taylor' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Louis Vuitton' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%' AND LOWER(c.storename) NOT LIKE '%temporary%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'LUSH' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = "Marc Jacobs" AND COALESCE(c.storetype, '') NOT IN ('Authorized retailer'))
              OR (c.ChainName_Coresight = 'Massimo Dutti' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Moncler' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%' AND LOWER(c.storename) NOT LIKE '%neiman%')
              OR (c.ChainName_Coresight = 'Pandora' AND LOWER(c.storename) NOT LIKE '%macy%')
              OR (c.ChainName_Coresight = 'Prada' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Ralph Lauren' AND LOWER(c.storename) NOT LIKE '%Ralph''s Coffee%')
              OR (c.ChainName_Coresight = 'Stop & Shop' AND COALESCE(c.storetype, '') NOT IN ('Pharmacy'))
              OR (c.ChainName_Coresight = 'Ted Baker' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'The North Face' AND LOWER(c.storename) NOT LIKE '%dick%' AND LOWER(c.storename) NOT LIKE '%oshkosh%')
              OR (c.ChainName_Coresight = 'Under Armour' AND COALESCE(c.storetype, '') NOT IN ('Brand House'))
              OR (c.ChainName_Coresight = 'Williams Sonoma' AND COALESCE(c.storetype, '') NOT IN ('STORE_IN_STORE'))
              OR (c.ChainName_Coresight = 'Yves Saint laurent' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.Chainname_Coresight = 'Free People' AND COALESCE(c.storetype, '') NOT IN ('FP Movement'))
              AND (c.ChainName_Coresight = 'Free People' AND LOWER(c.storename) NOT LIKE '%movement%')
            )
            AND p.is_active = 1
            AND Period BETWEEN '{query_start_date}' AND '{query_end_date}'
        """

        query_acq = f"""
            SELECT {base_columns}
            FROM all_active_acquisition c
            LEFT JOIN parent_chain_names_data p 
            ON c.chainname_coresight = p.chainname_coresight
            WHERE 
            (
              (c.ChainName_Coresight IS NULL OR c.ChainName_Coresight NOT IN ('Hoka', 'Sephora', 'Lululemon Athletica','Finish Line','Sunglass Hut',"Carter's",'Marc Jacobs','Bulgari', "Dick's Sporting Goods", 'CVS Pharmacy', 'Circle K', 'Aerie', 'American Eagle Outfitters',
              '7-Eleven','Anthropologie','Babies"R"Us','Balenciaga','Bottega Veneta','Burberry','Cartier','Century 21','Chanel','Christian Dior',"Claire’s",'Coach',
              "Conn's",'Converse','Deciem','Dolce & Gabbana','Fendi','Giant Eagle','Givenchy','Gucci','Hallmark','Hy-Vee','James Avery Artisan Jewelry',
              "L'Occitane",'LensCrafters',"Levi's",'Lord & Taylor','Louis Vuitton','LUSH','Marc Jacobs','Massimo Dutti','Moncler','Pandora','Prada','Ralph Lauren',
              'Ted Baker','The North Face','Under Armour','Williams Sonoma','Yves Saint laurent','Free People'))
              OR (c.ChainName_Coresight = 'Hoka' AND LOWER(c.storename) LIKE '%hoka%')
              OR (c.ChainName_Coresight = 'Sephora' AND LOWER(c.storename) NOT LIKE '%penney%' AND LOWER(c.storename) NOT LIKE '%kohl%')
              OR (c.ChainName_Coresight = 'Lululemon Athletica' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Finish Line' AND LOWER(c.storename) NOT LIKE '%macy%' AND LOWER(c.storename) NOT LIKE '%jd sports%')
              AND (c.ChainName_Coresight = 'Finish Line' AND COALESCE(c.storetype, '') NOT IN ('JD Sports'))
              OR (c.ChainName_Coresight = 'Sunglass Hut' AND LOWER(c.storename) NOT LIKE '%macy%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = "Carter's" AND COALESCE(c.storetype, '') <> 'Oshkosh')
              OR (c.ChainName_Coresight = 'Marc Jacobs' AND LOWER(c.storename) NOT LIKE '%macy%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%'
                AND LOWER(storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE 'bookmarc%')
              OR (c.ChainName_Coresight = "Bulgari" AND COALESCE(c.storetype, '') NOT IN ('Official Retailers','Dept. Store (Ds)'))
              OR (c.ChainName_Coresight = "Dick's Sporting Goods" AND LOWER(c.storename) NOT LIKE '%warehouse%' AND LOWER(c.storename) NOT LIKE '%temporary%' AND LOWER(storename) NOT LIKE '%going%')
              OR (c.ChainName_Coresight = "CVS Pharmacy" AND LOWER(c.storename) NOT LIKE '%target%' AND LOWER(c.storename) NOT LIKE '%schnucks%')
              OR (c.ChainName_Coresight = "Circle K" AND LOWER(c.storename) NOT LIKE '%car wash%' AND LOWER(c.storename) NOT LIKE '%holiday station%' AND LOWER(c.storename) NOT LIKE '%gas station%' AND LOWER(c.storename) NOT LIKE '%on the run%' AND LOWER(c.storetype) NOT LIKE '%holiday station%')
              OR (c.ChainName_Coresight = 'Aerie' AND LOWER(c.storename) NOT IN ('american eagle', 'american eagle store', 'offline', 'american eagle , offline store', 'offline store', 'american eagle & offline', 'american eagle outlet', 'american eagle , offline outlet', 'american eagle clearance store', 'american eagle , offline', 'american eagle denim deli'))
              OR (c.ChainName_Coresight = 'American Eagle Outfitters' AND LOWER(c.storename) NOT IN ('aerie - closed boulevard mall', 'offline', 'offline store', 'offline store - closed', 'aerie & offline', 'aerie store', 'aerie clearance store', 'aerie - santa rosa plaza', 'aerie outlet', 'aerie outlet - closed', 'aerie , offline store', 'aerie store - closed', 'unsubscribed', 'offline clearance store', 'aerie , offline', 'aerie streets at southpoint', 'aerie bangor mall', 'aerie crystal mall', 'aerie spring street', 'aerie lakeline mall', 'aerie south side works', 'aerie - closed spring street', 'aerie northlake mall', 'aerie los cerritos center', 'aerie annapolis mall', 'aerie green acres mall', 'aerie exton square mall', 'aerie anchorage fifth avenue mall', 'aerie fox river mall', 'aerie staten island mall', 'aerie charleston town center', 'aerie san francisco center', 'aerie the mall @ johnson city', 'aerie - west town mall', 'aerie the oaks', 'aerie - closed crystal mall', 'aerie park plaza mall', 'offline - mall of georgia', 'offline - natick mall'))
              OR (c.ChainName_Coresight = '7-Eleven' AND COALESCE(c.storetype, '') NOT IN ('Stripes','Speedway'))
              OR (c.ChainName_Coresight = 'Anthropologie' AND COALESCE(c.storetype, '') NOT IN ('Temp'))
              OR (c.ChainName_Coresight = 'Babies"R"Us' AND LOWER(c.storename) NOT LIKE '%kohl%')
              OR (c.ChainName_Coresight = 'Balenciaga' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Bottega Veneta' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Burberry' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Cartier' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Century 21' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Chanel' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Christian Dior' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = "Claire’s" AND LOWER(c.storename) NOT LIKE '%walmart%')
              OR (c.ChainName_Coresight = 'Coach' AND COALESCE(c.storetype, '') NOT IN ('Coffee Shop'))
              OR (c.ChainName_Coresight = "Conn's" AND LOWER(c.storename) NOT LIKE '%belk%')
              OR (c.ChainName_Coresight = 'Converse' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Deciem' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Dolce & Gabbana' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Fendi' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'Finish Line' AND COALESCE(c.storetype, '') NOT IN ('JD Sports'))
              OR (c.ChainName_Coresight = 'Giant Eagle' AND COALESCE(c.storetype, '') NOT IN ('WetGo'))
              OR (c.ChainName_Coresight = 'Givenchy' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'Gucci' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'Hallmark' AND LOWER(c.storename) NOT LIKE '%ace%' AND LOWER(c.storename) NOT LIKE '%pharmacy%' AND LOWER(c.storename) NOT LIKE '%health%')
              OR (c.ChainName_Coresight = 'Hy-Vee' AND COALESCE(c.storetype, '') NOT IN ('Pharmacy'))
              OR (c.ChainName_Coresight = 'James Avery Artisan Jewelry' AND LOWER(c.storename) NOT LIKE '%dillard%')
              OR (c.ChainName_Coresight = "L'Occitane" AND COALESCE(c.storetype, '') NOT IN ('NON_OWNED'))
              OR (c.ChainName_Coresight = 'LensCrafters' AND LOWER(c.storename) NOT LIKE '%macy%')
              OR (c.ChainName_Coresight = "Levi's" AND COALESCE(c.storetype, '') NOT IN ('Retail Partner'))
              OR (c.ChainName_Coresight = 'Lord & Taylor' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Louis Vuitton' AND LOWER(c.storename) NOT LIKE '%pop%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%' AND LOWER(c.storename) NOT LIKE '%temporary%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%saks%')
              OR (c.ChainName_Coresight = 'LUSH' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = "Marc Jacobs" AND COALESCE(c.storetype, '') NOT IN ('Authorized retailer'))
              OR (c.ChainName_Coresight = 'Massimo Dutti' AND LOWER(c.storename) NOT LIKE '%pop%')
              OR (c.ChainName_Coresight = 'Moncler' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%' AND LOWER(c.storename) NOT LIKE '%neiman%')
              OR (c.ChainName_Coresight = 'Pandora' AND LOWER(c.storename) NOT LIKE '%macy%')
              OR (c.ChainName_Coresight = 'Prada' AND LOWER(c.storename) NOT LIKE '%nordstrom%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%neiman%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'Ralph Lauren' AND LOWER(c.storename) NOT LIKE '%Ralph''s Coffee%')
              OR (c.ChainName_Coresight = 'Stop & Shop' AND COALESCE(c.storetype, '') NOT IN ('Pharmacy'))
              OR (c.ChainName_Coresight = 'Ted Baker' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.ChainName_Coresight = 'The North Face' AND LOWER(c.storename) NOT LIKE '%dick%' AND LOWER(c.storename) NOT LIKE '%oshkosh%')
              OR (c.ChainName_Coresight = 'Under Armour' AND COALESCE(c.storetype, '') NOT IN ('Brand House'))
              OR (c.ChainName_Coresight = 'Williams Sonoma' AND COALESCE(c.storetype, '') NOT IN ('STORE_IN_STORE'))
              OR (c.ChainName_Coresight = 'Yves Saint laurent' AND LOWER(c.storename) NOT LIKE '%bloomingdale%')
              OR (c.Chainname_Coresight = 'Free People' AND COALESCE(c.storetype, '') NOT IN ('FP Movement'))
              AND (c.ChainName_Coresight = 'Free People' AND LOWER(c.storename) NOT LIKE '%movement%')
            )
            AND p.is_active = 1
            AND Period BETWEEN '{query_start_date}' AND '{query_end_date}'
        """

        # Step 4: Parallel fetch
        def fetch_query(query):
            local_conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_NAME, ssl_ca=SSL_CA)
            df = pd.read_sql(query, local_conn)
            local_conn.close()
            return df

        start_parallel = time.time()
        with ThreadPoolExecutor() as executor:
            future_cy = executor.submit(fetch_query, query_cy)
            future_py = executor.submit(fetch_query, query_py)
            future_acq = executor.submit(fetch_query, query_acq)

            df_cy = future_cy.result()
            df_py = future_py.result()
            df_acq = future_acq.result()

        logging.info(f"[fetch_data] Parallel fetch took {time.time() - start_parallel:.2f} seconds")

        # Step 5: Combine and clean
        active_df = pd.concat([df_cy, df_py, df_acq], ignore_index=True)
        active_df['Period'] = pd.to_datetime(active_df['Period'], errors='coerce').dt.date
        active_df['UpdateCycle'] = active_df['UpdateCycle'].fillna(30).astype(int)
        
        # Store the actual date ranges in session state for synchronization
        st.session_state['database_date_range'] = (twelve_months_back, max_period)
        st.session_state['query_date_range'] = (query_start_date, query_end_date)
        st.session_state['display_date_range'] = (display_start_date, display_end_date)
        
        logging.info(f"[fetch_data] Database has data from: {active_df['Period'].min()} to {active_df['Period'].max()}")
        logging.info(f"[fetch_data] Display range: {display_start_date} to {display_end_date}")
        logging.info(f"[fetch_data] TOTAL execution time: {time.time() - start_total:.2f} seconds")
        
        return active_df

    @st.cache_data(show_spinner=False)
    def fetch_data_population():
        conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_NAME)
        
        connection_params = {
            'user': DB_USER,
            'password': DB_PASSWORD,
            'host': DB_HOST,
            'database': DB_NAME
        }

        if os.getenv('ENABLE_SSL', 'false').lower() == 'true' and os.getenv('SSL_CA'):
            connection_params['ssl_ca'] = os.getenv('SSL_CA')
            logging.info("SSL connection enabled")
        else:
            logging.info("SSL connection not enabled")

        conn = mysql.connector.connect(**connection_params)
        cursor = conn.cursor()

        pop_sql = """
        SELECT
          TRIM(usps_state_name) AS usps_state_name,
          CAST(zip_code AS CHAR(5)) AS zip_code,
          estimate_total_population
        FROM population_data_by_age_and_sex
        """
        pop_df = pd.read_sql(pop_sql, conn)

        return pop_df

    col1, col2 = st.columns([1, 1])

    with col1:
        # Title of the app
        st.markdown("<h1 style='font-size: 40px; text-align: left; padding-top: 0px; padding-bottom: 0px;'>Store Intelligence Platform</h1>", unsafe_allow_html=True)
    with col2:
        st.markdown("""
        <style>
        .tab-links {
            display: flex;
            justify-content: flex-end;
            gap: 2px;
        }
        .tab-links a {
            text-decoration: none;
            padding: 8px 16px;
            background-color: #d0d0d0;
            color: black;
            border-radius: 5px;
            font-size: 16px;
        }
        .tab-links a:hover {
            background-color: #767779;
        }
        .tab-links a.active {
            background-color: #D62E2F; /* Active tab color (dark) */
            color: white; /* Active tab text color */
        }.button-container {
            display: flex;
            flex-direction: column;
            width: 100%;
        }
        </style>
        <div class="button-container">
            <div class="tab-links">
                <a href="/net#store-intelligence-platform" target="_self">Net Openings</a>
                <a href="/opening#store-intelligence-platform" target="_self">Store Openings</a>
                <a href="/closing#store-intelligence-platform" target="_self">Store Closures</a>
                <a href="/active#store-intelligence-platform" target="_self" class="active">Active Stores</a>
            </div>
        </div>
        """, unsafe_allow_html=True)


    # Fetch the data once and cache it
    # Single spinner that transforms itself
    transforming_spinner_html = """
    <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 40px;">
        <div id="spinner" style="
            border: 4px solid #f3f3f3;
            border-top: 4px solid #FF4444;
            border-radius: 50%;
            width: 60px;
            height: 60px;
            animation: spin 1s linear infinite;
            transition: all 0.8s cubic-bezier(0.4, 0, 0.2, 1);
        "></div>
        <div id="text" style="margin-top: 20px; font-family: sans-serif; color: #666; font-size: 16px; text-align: center; transition: all 0.5s ease;">
            Loading Dashboard – this may take 2-3 minutes, please wait…
        </div>
    </div>

    <style>
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }

    @keyframes finalSpin {
        0% { 
            transform: rotate(0deg);
            border-top-color: #FF4444;
            border-right-color: #f3f3f3;
            border-bottom-color: #f3f3f3;
            border-left-color: #f3f3f3;
        }
        25% {
            border-top-color: #4CAF50;
            border-right-color: #f3f3f3;
            border-bottom-color: #f3f3f3;
            border-left-color: #f3f3f3;
        }
        50% {
            border-top-color: #4CAF50;
            border-right-color: #4CAF50;
            border-bottom-color: #f3f3f3;
            border-left-color: #f3f3f3;
        }
        75% {
            border-top-color: #4CAF50;
            border-right-color: #4CAF50;
            border-bottom-color: #4CAF50;
            border-left-color: #f3f3f3;
        }
        100% { 
            transform: rotate(360deg);
            border: 4px solid #4CAF50;
            box-shadow: 0 0 20px rgba(76, 175, 80, 0.3);
        }
    }

    .complete {
        animation: finalSpin 1.5s ease-in-out forwards !important;
    }

    .complete-text {
        color: #4CAF50 !important;
        font-weight: 500 !important;
    }
    </style>

    <script>
    // Function to complete the spinner
    function completeSpinner() {
        const spinner = document.getElementById('spinner');
        const text = document.getElementById('text');
        
        spinner.classList.add('complete');
        
        setTimeout(() => {
            text.textContent = 'Platform Ready!';
            text.classList.add('complete-text');
        }, 800);
    }

    // Listen for completion message
    window.addEventListener('message', function(event) {
        if (event.data === 'complete-spinner') {
            completeSpinner();
        }
    });
    </script>
    """

    # Create placeholder
    placeholder = st.empty()

    # Show spinner
    with placeholder.container():
        st.markdown("<br><br>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([2, 3, 2])
        with col2:
            html(transforming_spinner_html, height=200, scrolling=False)

    # Load data
    active_data = fetch_data()

    st.components.v1.html("""
    <script>
    parent.postMessage('complete-spinner', '*');
    </script>
    """, height=0)


    # Sidebar filters

    st.markdown("<div style='margin-top: 10px;'></div>", unsafe_allow_html=True)
    #st.markdown("<h4 style='font-size: 20px; text-align: left;'>Filters</h4>", unsafe_allow_html=True)
    #st.header('Filters')

    latest_ts = active_data["Period"].max()
    latest_ts = latest_ts.strftime("%B %Y") 

    top_col1, top_col2, top_col3 = st.columns([78, 10, 12]) # Adjust sizes for alignment as you wish

    with top_col1:
        tabs = ["Base Dashboard", "Compare Retailers", "Compare Sectors"]
        if "selected_tab" not in st.session_state or not st.session_state["selected_tab"]:
            st.session_state["selected_tab"] = "Base Dashboard"

        # Optionally: also clear tab param so URL never has a value at start!
        if "tab" in st.query_params:
            del st.query_params["tab"]

        selected_tab = st.radio(
            "Analysis", 
            tabs, 
            index=tabs.index(st.session_state["selected_tab"]),  # Set current tab from session state
            horizontal=True, 
            label_visibility="collapsed",
            # captions=[
            #     "For Individual Compare",
            #     "For Retailers Compare",
            #     "For Sectors Compare",
            # ],
        )

        # Update session state and query params when tab changes
        if selected_tab != st.session_state["selected_tab"]:
            st.session_state["selected_tab"] = selected_tab
            st.query_params["tab"] = selected_tab
            st.rerun()

    with top_col3:
        # Ensure both keys exist in session state
        if "sector_comparison_expand_filters" not in st.session_state:
            st.session_state["sector_comparison_expand_filters"] = True
        if "retailer_comparison_expand_filters" not in st.session_state:
            st.session_state["retailer_comparison_expand_filters"] = True
        if "expand_filters" not in st.session_state:
            st.session_state["expand_filters"] = True

        # Button label logic: If either is open, say "Close All", else "Expand All"
        all_expanded = st.session_state["sector_comparison_expand_filters"] and st.session_state["retailer_comparison_expand_filters"] and st.session_state["expand_filters"]
        toggle_label = "Close Filters" if all_expanded else "Expand Filters"

        if st.button(toggle_label, key="all_filters_btn", use_container_width=True):
            new_state = not all_expanded
            st.session_state["sector_comparison_expand_filters"] = new_state
            st.session_state["retailer_comparison_expand_filters"] = new_state
            st.session_state["expand_filters"] = new_state
            st.rerun()

    with top_col2:
        if st.button("Reset Filters", key="clear_all_filters_btn", use_container_width=True, type="secondary"):
            logging.info("Reset Filters button clicked - resetting all filters to default")
            
            # Calculate default date range from opened_data (12 months back from max date)
            max_period = active_data['Period'].max()
            
            # Handle both datetime and date objects
            if hasattr(max_period, 'date'):
                max_date = max_period.date()
            else:
                max_date = max_period
                
            default_end_date = max_date
            
            # Calculate 12 months back from max date
            if hasattr(max_date, 'replace'):
                start_date_temp = max_date.replace(day=1)
                default_start_date = (pd.to_datetime(start_date_temp) - pd.DateOffset(months=11)).date()
            else:
                default_start_date = (pd.to_datetime(max_date) - pd.DateOffset(months=11)).date()
            
            logging.info(f"Calculated default date range: {default_start_date} to {default_end_date}")
            
            # Detect which tab is selected
            selected_tab = st.session_state.get("selected_tab", "Base Dashboard")
            logging.info(f"Resetting filters for tab: {selected_tab}")
            
            # Clear common date-related session state keys
            common_date_keys = [
                "start_month", "start_year", "end_month", "end_year",
                "start_month_select", "start_year_select", 
                "end_month_select", "end_year_select",
                "selected_date_range"
            ]
            
            # Clear Compare Retailers specific date keys
            compare_retailers_date_keys = [
                "compare_start_month", "compare_start_year",
                "compare_end_month", "compare_end_year",
                "compare_start_month_select", "compare_start_year_select",
                "compare_end_month_select", "compare_end_year_select"
            ]
            
            # Clear Sector Compare specific date keys
            sector_compare_date_keys = [
                "sector_compare_start_month", "sector_compare_start_year",
                "sector_compare_end_month", "sector_compare_end_year",
                "sector_compare_start_month_select", "sector_compare_start_year_select",
                "sector_compare_end_month_select", "sector_compare_end_year_select",
                "sector_compare_date_range"
            ]
            
            # Clear all date keys
            for key in common_date_keys + compare_retailers_date_keys + sector_compare_date_keys:
                if key in st.session_state:
                    del st.session_state[key]
            
            # Reset based on selected tab
            if selected_tab == "Base Dashboard":
                logging.info("Resetting Base Dashboard filters")
                
                # Reset date range
                st.session_state["start_month"] = default_start_date.month
                st.session_state["start_year"] = default_start_date.year
                st.session_state["end_month"] = default_end_date.month
                st.session_state["end_year"] = default_end_date.year
                st.session_state["selected_date_range"] = (default_start_date, default_end_date)
                
                user_filters["selected_sector_name"] = "All"
                user_filters["selected_chain_name"] = ["All"]
                user_filters["parent_chain_name"] = ["All"]
                
                user_filters["selected_state_name"] = ["All"]
                user_filters["selected_msa_name"] = ["All"]
                user_filters["selected_zip_code_active"] = ["All"]
                
                # Reset sync flags
                if "retailers_synced" in st.session_state:
                    del st.session_state["retailers_synced"]
                
                # Clear widget keys
                widget_keys = [
                    "sector_selectbox", "multiselect_1", "multiselect_selected_chain",
                    "multiselect_state", "multiselect_msa"
                ]
                
                for key in widget_keys:
                    if key in st.session_state:
                        del st.session_state[key]
                
                # Update query parameters
                st.query_params.update({
                    "start_date": default_start_date.isoformat(),
                    "end_date": default_end_date.isoformat()
                })
                
            elif selected_tab == "Compare Retailers":
                logging.info("Resetting Compare Retailers filters")
                
                # Reset date range for Compare Retailers
                st.session_state["compare_start_month"] = default_start_date.month
                st.session_state["compare_start_year"] = default_start_date.year
                st.session_state["compare_end_month"] = default_end_date.month
                st.session_state["compare_end_year"] = default_end_date.year
                
                # Reset retailers to [] (empty)
                st.session_state["selected_parent_names"] = []
                st.session_state["selected_chain_names"] = []
                
                # Reset location to ["All"]
                st.session_state["selected_state_active_v2"] = ["All"]
                st.session_state["selected_msa_active_v2"] = ["All"]
                
                # Clear widget keys for Compare Retailers (except sector_filter)
                widget_keys = [
                    "selected_parent_names_v2", 
                    "selected_chain_names_v2",
                    "selected_state_active_v2",
                    "selected_msa_active_v2"
                ]
                
                for key in widget_keys:
                    if key in st.session_state:
                        del st.session_state[key]
                
                # Reset sector to "All" AFTER clearing other widget keys
                st.session_state["sector_filter"] = "All"
                
                # Update query parameters for Compare Retailers
                st.query_params.update({
                    "start_date": default_start_date.isoformat(),
                    "end_date": default_end_date.isoformat(),
                    "sector_filter": "All",
                    "selected_parent_names": [],
                    "selected_chain_names": [],
                    "selected_state": ["All"],
                    "selected_msa": ["All"]
                })
                
            else:  # Compare Sectors
                logging.info("Resetting Compare Sectors filters")
                
                # Reset date range for sector comparison
                st.session_state["sector_compare_start_month"] = default_start_date.month
                st.session_state["sector_compare_start_year"] = default_start_date.year
                st.session_state["sector_compare_end_month"] = default_end_date.month
                st.session_state["sector_compare_end_year"] = default_end_date.year
                st.session_state["sector_compare_date_range"] = (default_start_date, default_end_date)
                
                # Reset sectors to [] (empty)
                st.session_state["sector_comparison_selected_sectors"] = []
                
                # Reset retailers to [] (empty)
                st.session_state["sector_compare_parent_chain_name"] = []
                st.session_state["sector_compare_selected_chain_name"] = []
                
                # Reset location to ["All"]
                st.session_state["sector_compare_selected_state_name"] = ["All"]
                st.session_state["sector_compare_selected_msa_name"] = ["All"]
                
                # Reset sync flags
                if "sector_compare_synced" in st.session_state:
                    del st.session_state["sector_compare_synced"]
                
                # Clear widget keys for sector comparison
                widget_keys = [
                    "sector_multiselect",
                    "sector_compare_parent_chain_name_select",
                    "sector_compare_selected_chain_name_select",
                    "sector_compare_multiselect_state",
                    "sector_compare_multiselect_msa"
                ]
                
                for key in widget_keys:
                    if key in st.session_state:
                        del st.session_state[key]
                
                # Update query parameters for sector comparison
                st.query_params.update({
                    "start_date": default_start_date.isoformat(),
                    "end_date": default_end_date.isoformat(),
                    "compare_sectors": [],
                    "parent_chain": [],
                    "chain": [],
                    "state": ["All"],
                    "msa": ["All"]
                })
            
            logging.info(f"All filters reset to default values for {selected_tab} - Date range: {default_start_date} to {default_end_date}")

    # Now set up the appropriate filters based on the current tab
    if st.session_state["selected_tab"] == "Base Dashboard":
        # Initialize base dashboard filters if they don't exist
        if "base_filters_initialized" not in st.session_state:
            user_filters["selected_chain_name"] = user_filters.get("selected_chain_name", ["All"])
            user_filters["parent_chain_name"] = user_filters.get("parent_chain_name", ["All"])
            user_filters["selected_state_name"] = user_filters.get("selected_state_name", ["All"])
            user_filters["selected_msa_name"] = user_filters.get("selected_msa_name", ["All"])
            user_filters["selected_sector_name"] = user_filters.get("selected_sector_name", ["All"])
            st.session_state["base_filters_initialized"] = True

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


    if selected_tab == "Base Dashboard":

        st.markdown(
            """
            <style>
            /* Hide Streamlit cookies controller wrapper if it has 0 height */
            .st-emotion-cache-8atqhb.e1mlolmg0[style*="height:0"], 
            .st-emotion-cache-1tvzk6f[data-testid="stCustomComponentV1"][height="0"] {
                display: none !important;
                visibility: hidden !important;
                padding: 0 !important;
                margin: 0 !important;
                height: 0 !important;
                min-height: 0 !important;
                max-height: 0 !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        def update_options(selected_options, session_key):
            if "All" in selected_options and len(selected_options) > 1:
                if "All" not in st.session_state[session_key]:
                    return ["All"]
                return [opt for opt in selected_options if opt != "All"]
            return selected_options

        # Create a horizontal layout with columns for filters
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            with st.expander("Date Range", expanded=st.session_state["expand_filters"]):
                logging.info("--- DATE RANGE SECTION START ---")
                logging.info(f"Current session state before date range: { {k: v for k, v in st.session_state.items() if not k.startswith('_')} }")
                
                selected_date_range, min_date, max_date = get_synchronized_date_range(
                    active_data, default_months_back=12, label="active.py"
                )
                start_date, end_date = selected_date_range

                qparams = st.query_params
                start_date_param = qparams.get("start_date", [None])[0]
                end_date_param = qparams.get("end_date", [None])[0]
                logging.info(f"Received query params - start_date: {start_date_param}, end_date: {end_date_param}")

                # Initialize session state from query params or defaults
                try:
                    # Handle list parameters and ensure they're strings
                    start_param = str(start_date_param[0]) if isinstance(start_date_param, list) else str(start_date_param)
                    end_param = str(end_date_param[0]) if isinstance(end_date_param, list) else str(end_date_param)
                    
                    logging.info(f"Raw date params - start: '{start_param}' (type: {type(start_param)}), end: '{end_param}' (type: {type(end_param)})")
                    
                    # Validate date strings
                    if (not isinstance(start_param, str)) or (not isinstance(end_param, str)):
                        raise ValueError("Date parameters must be strings")
                    if len(start_param) < 6 or len(end_param) < 6:
                        raise ValueError("Date string too short")
                    
                    param_start = pd.to_datetime(start_param, format='%Y-%m-%d', errors='strict').date()
                    param_end = pd.to_datetime(end_param, format='%Y-%m-%d', errors='strict').date()
                    
                    logging.info(f"Successfully parsed query params - param_start: {param_start}, param_end: {param_end}")
                    
                    st.session_state.setdefault("start_month", param_start.month)
                    st.session_state.setdefault("start_year", param_start.year)
                    st.session_state.setdefault("end_month", param_end.month)
                    st.session_state.setdefault("end_year", param_end.year)
                except Exception as e:
                    logging.warning(f"Failed to parse query params, using defaults - start_date: {start_date}, end_date: {end_date}. Error: {e}")
                    st.session_state.setdefault("start_month", start_date.month)
                    st.session_state.setdefault("start_year", start_date.year)
                    st.session_state.setdefault("end_month", end_date.month)
                    st.session_state.setdefault("end_year", end_date.year)

                logging.info(f"Initialized session state - start_month: {st.session_state['start_month']}, start_year: {st.session_state['start_year']}, end_month: {st.session_state['end_month']}, end_year: {st.session_state['end_year']}")

                min_year, max_year = min_date.year, max_date.year
                all_months = list(range(1, 13))
                all_years = list(range(min_year, max_year + 1))

                # --- START DATE PICKERS ---
                sm_col, sy_col = st.columns([1.3, 1])
                with sm_col:
                    # Valid start months depending on selected year
                    if st.session_state["start_year"] == min_year:
                        valid_start_months = list(range(min_date.month, 13))
                    elif st.session_state["start_year"] == max_year:
                        valid_start_months = list(range(1, max_date.month + 1))
                    else:
                        valid_start_months = all_months

                    logging.info(f"Valid start months: {valid_start_months} for year {st.session_state['start_year']}")
                    
                    st.selectbox(
                        "Start Month",
                        options=valid_start_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_start_months.index(st.session_state["start_month"]),
                        key="start_month_select"
                    )
                with sy_col:
                    st.selectbox(
                        "Start Year",
                        options=all_years,
                        index=all_years.index(st.session_state["start_year"]),
                        key="start_year_select"
                    )
                st.session_state["start_month"] = st.session_state["start_month_select"]
                st.session_state["start_year"] = st.session_state["start_year_select"]
                logging.info(f"After start date selection - month: {st.session_state['start_month']}, year: {st.session_state['start_year']}")

                # --- END DATE PICKERS ---
                em_col, ey_col = st.columns([1.3, 1])

                # Valid end years should be >= start year and <= max year
                valid_end_years = [y for y in all_years if st.session_state["start_year"] <= y <= max_year]
                if st.session_state["end_year"] not in valid_end_years:
                    logging.info(f"Adjusting end_year from {st.session_state['end_year']} to {valid_end_years[0]}")
                    st.session_state["end_year"] = valid_end_years[0]

                # Valid end months depend on selected start and end years
                if st.session_state["end_year"] == st.session_state["start_year"]:
                    valid_end_months = list(range(st.session_state["start_month"], 13))
                elif st.session_state["end_year"] == max_year:
                    valid_end_months = list(range(1, max_date.month + 1))
                else:
                    valid_end_months = all_months

                if st.session_state["end_month"] not in valid_end_months:
                    logging.info(f"Adjusting end_month from {st.session_state['end_month']} to {valid_end_months[0]}")
                    st.session_state["end_month"] = valid_end_months[0]

                with em_col:
                    st.selectbox(
                        "End Month",
                        options=valid_end_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_end_months.index(st.session_state["end_month"]),
                        key="end_month_select"
                    )
                with ey_col:
                    st.selectbox(
                        "End Year",
                        options=valid_end_years,
                        index=valid_end_years.index(st.session_state["end_year"]),
                        key="end_year_select"
                    )
                st.session_state["end_month"] = st.session_state["end_month_select"]
                st.session_state["end_year"] = st.session_state["end_year_select"]
                logging.info(f"After end date selection - month: {st.session_state['end_month']}, year: {st.session_state['end_year']}")

                # Final date construction with boundary clamping
                new_start_date = max(date(st.session_state["start_year"], st.session_state["start_month"], 1), min_date)
                end_day = calendar.monthrange(st.session_state["end_year"], st.session_state["end_month"])[1]
                new_end_date = min(date(st.session_state["end_year"], st.session_state["end_month"], end_day), max_date)

                logging.info(f"Calculated new dates - start: {new_start_date}, end: {new_end_date}")
                logging.info(f"Previous dates - start: {start_date}, end: {end_date}")

                if (new_start_date != start_date or new_end_date != end_date):
                    logging.info("Dates changed - updating session state and query params")
                    st.session_state["selected_date_range"] = (new_start_date, new_end_date)
                    st.query_params.update({
                        "start_date": new_start_date.isoformat(),
                        "end_date": new_end_date.isoformat()
                    })
                    logging.info(f"Updated query params: {st.query_params}")
                    st.rerun()

                st.session_state.update({
                    "selected_date_range": (new_start_date, new_end_date)
                })
                logging.info("--- DATE RANGE SECTION END ---")

        with col2:
            logging.info("--- SECTOR SECTION START ---")
            
            filtered_sector_data = active_data
            logging.info(f"Initial sector data shape: {filtered_sector_data.shape}")

            start_date, end_date = st.session_state["selected_date_range"]

            start_timestamp = pd.Timestamp(start_date)
            end_timestamp = pd.Timestamp(end_date)
            filtered_sector_data = filtered_sector_data[
                (filtered_sector_data['Period'] >= start_timestamp) &
                (filtered_sector_data['Period'] <= end_timestamp)
            ]

            # Get unique sector names and sort them properly
            sector_names = filtered_sector_data["Sector_Coresight"].dropna().unique().tolist()
            
            # Separate "Others" from the rest (case-insensitive)
            others_items = [s for s in sector_names if s.lower() in ["other", "others"]]
            regular_items = [s for s in sector_names if s.lower() not in ["other", "others"]]
            
            # Sort regular items alphabetically (case-insensitive)
            regular_items = sorted(regular_items, key=lambda x: x.lower())
            sector_names = ["All"] + regular_items + others_items
            logging.info(f"Available sectors: {sector_names}")

            # --- COOKIFIED FILTER STATE ---
            current_sector = user_filters.get("selected_sector_name", "All")
            if current_sector not in sector_names:
                current_sector = "All"
                user_filters["selected_sector_name"] = "All"
                save_auth_cookie()

            current_index = sector_names.index(current_sector) if current_sector in sector_names else 0
            with st.expander("Sector", expanded=st.session_state["expand_filters"]):
                selected_sector_name = st.selectbox(
                    "Select Sector",
                    sector_names,
                    index=current_index,
                    key="sector_selectbox"
                )
                logging.info(f"User selected sector: {selected_sector_name}")

                # Update cookie if user changed selection, reset children, rerun
                if selected_sector_name != current_sector:
                    user_filters["selected_sector_name"] = selected_sector_name
                    user_filters["parent_chain_name"] = ["All"]
                    user_filters["selected_chain_name"] = ["All"]
                    user_filters["selected_state_name"] = ["All"]
                    user_filters["selected_msa_name"] = ["All"]
                    user_filters["selected_zip_code_active"] = ["All"]
                    save_auth_cookie()
                    st.rerun()

            logging.info("--- SECTOR SECTION END ---")

        with col3:
            logging.info("--- RETAILERS SECTION START ---")

            start_timestamp = pd.Timestamp(start_date)
            end_timestamp = pd.Timestamp(end_date)
            active_data = active_data[
                (active_data['Period'] >= start_timestamp) &
                (active_data['Period'] <= end_timestamp)
            ]
            if "All" not in selected_sector_name:
                active_data = active_data[
                    active_data["Sector_Coresight"].isin([selected_sector_name])
            ]
            active_data['ParentName_Coresight'] = active_data['ParentName_Coresight'].where(
                pd.notna(active_data['ParentName_Coresight']), "No Parent Retailer")
            parent_names = active_data['ParentName_Coresight'].unique().tolist()
            parent_names.sort()
            if "No Parent Retailer" in parent_names:
                parent_names.remove("No Parent Retailer")
                parent_names.append("No Parent Retailer")
            parent_names = ["All"] + parent_names
            
            current_parent = user_filters.get("parent_chain_name", ["All"])
            if "All" in current_parent:
                current_parent = ["All"]
            else:
                valid_selected = [p for p in current_parent if p in parent_names]
                current_parent = valid_selected if valid_selected else ["All"]

            with st.expander("Retailers", expanded=st.session_state.get("expand_filters", True)):
                parent_chain_name = st.multiselect(
                    "Select Company", 
                    parent_names, 
                    default=current_parent,
                    max_selections=10, 
                    help="Parent company that owns one or more store banners.", 
                    key="parent_multiselect_1"
                )

                # If "All" with more than one, remove "All"
                if len(parent_chain_name) > 1 and "All" in parent_chain_name:
                    if parent_chain_name[-1] == "All":
                        parent_chain_name = ["All"]
                    else:
                        parent_chain_name = [p for p in parent_chain_name if p != "All"]
                elif not parent_chain_name:
                    parent_chain_name = ["All"]
                # If user changed selection, process "All" logic & update cookie
                if parent_chain_name != current_parent:

                    user_filters["parent_chain_name"] = parent_chain_name
                    
                    # Reset dependent filters if needed (these still in session_state for now)
                    user_filters["selected_chain_name"] = ["All"]
                    user_filters["selected_state_name"] = ["All"]
                    user_filters["selected_msa_name"] = ["All"]
                    user_filters["selected_zip_code_active"] = ["All"]
                    save_auth_cookie()
                    st.rerun()
                    
                    
                # For downstream use
                # parent_chain_name = user_filters.get("parent_chain_name", ["All"])

                st.markdown(
                    """
                    <div style='text-align: center; font-size: 0.9rem; font-weight: 500; margin-top: 0.4rem; margin-bottom: 0.4rem; color: #444;'>or</div>
                    """,
                    unsafe_allow_html=True
                )

                # Filtering by parent_chain_name ONLY from cookie
                if "All" in parent_chain_name or not parent_chain_name:
                    filtered_data = active_data
                else:
                    filtered_data = active_data[active_data["ParentName_Coresight"].isin(parent_chain_name)]

                # ---------------- Banner/secondary filters remains unchanged ----------------
                start_date, end_date = st.session_state["selected_date_range"]
                start_timestamp = pd.Timestamp(start_date)
                end_timestamp = pd.Timestamp(end_date)
                filtered_data = filtered_data[
                    (filtered_data['Period'] >= start_timestamp) &
                    (filtered_data['Period'] <= end_timestamp)
                ]
                if "All" not in selected_sector_name:
                    filtered_data = filtered_data[
                        filtered_data["Sector_Coresight"].isin([selected_sector_name])
                ]
                    
                chain_names = sorted(
                    [x for x in filtered_data['ChainName_Coresight'].unique().tolist() if x is not None], 
                    key=lambda x: str(x)
                )
                chain_names = ["All"] + chain_names    

                current_chain = user_filters.get("selected_chain_name", ["All"])
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
                    help="The specific retail banner or storefront name customers see.",
                    key="multiselect_selected_chain"
                )

                if len(selected_chain_name) > 1 and "All" in selected_chain_name:
                    if selected_chain_name[-1] == "All":
                        selected_chain_name = ["All"]
                    else:
                        selected_chain_name = [c for c in selected_chain_name if c != "All"]
                elif not selected_chain_name:
                    selected_chain_name = ["All"]
                if selected_chain_name != current_chain:
                    user_filters["selected_chain_name"] = selected_chain_name
                    user_filters["selected_state_name"] = ["All"]
                    user_filters["selected_msa_name"] = ["All"]
                    user_filters["selected_zip_code_active"] = ["All"]
                    save_auth_cookie()
                    st.rerun()


                if "All" in selected_chain_name:
                    selected_chain_name = chain_names

                if "All" in parent_chain_name:
                    parent_chain_name = parent_names

            logging.info("--- RETAILERS SECTION END ---")

        with col4:
            logging.info("--- LOCATION SECTION START ---")
            with st.expander("Location", expanded=st.session_state["expand_filters"]):
                filtered_location_data = active_data.copy()

                start_date, end_date = st.session_state["selected_date_range"]
                start_timestamp = pd.Timestamp(start_date)
                end_timestamp = pd.Timestamp(end_date)
                filtered_location_data = filtered_location_data[
                    (filtered_location_data['Period'] >= start_timestamp) &
                    (filtered_location_data['Period'] <= end_timestamp)
                ]

                if "All" not in parent_chain_name and parent_chain_name != []:
                    filtered_location_data = filtered_location_data[
                        filtered_location_data["ParentName_Coresight"].isin(parent_chain_name)]

                if "All" not in selected_chain_name and selected_chain_name != []:
                    filtered_location_data = filtered_location_data[
                        filtered_location_data["ChainName_Coresight"].isin(selected_chain_name)]

                if "All" not in selected_sector_name:
                    filtered_location_data = filtered_location_data[
                        filtered_location_data["Sector_Coresight"].isin([selected_sector_name])
                    ]

                # -----------------
                # STATE FILTER
                # -----------------
                states = sorted(filtered_location_data["State"].dropna().unique().tolist())
                states = ["All"] + states

                # get current state from cookie, only allow valid choices
                current_state = user_filters.get("selected_state_name", ["All"])
                if "All" in current_state:
                    current_state = ["All"]
                else:
                    valid_states = [s for s in current_state if s in states]
                    current_state = valid_states if valid_states else ["All"]

                selected_state_name = st.multiselect(
                    "Select State", 
                    states, 
                    default=current_state, 
                    help="U.S. state where the store is located.",
                    key="multiselect_state"
                )

                # Ensure All is not mixed with specifics
                if len(selected_state_name) > 1 and "All" in selected_state_name:
                    if selected_state_name[-1] == "All":
                        selected_state_name = ["All"]
                    else:
                        selected_state_name = [s for s in selected_state_name if s != "All"]
                elif not selected_state_name:
                    selected_state_name = ["All"]

                if selected_state_name != current_state:
                    user_filters["selected_state_name"] = selected_state_name
                    user_filters["selected_msa_name"] = ["All"]
                    user_filters["selected_zip_code_active"] = ["All"]
                    save_auth_cookie()
                    st.rerun()


                # For downstream, always pull from cookie
                selected_state_name = user_filters.get("selected_state_name", ["All"])

                if "All" in selected_state_name or not selected_state_name:
                    selected_state_name = states
                    filtered_data_msa = filtered_location_data
                else:
                    filtered_data_msa = filtered_location_data[filtered_location_data['State'].isin(selected_state_name)]

                # -----------------
                # MSA FILTER
                # -----------------
                # -----------------
                # LOCATION FILTER (MSA/ZIP CODE)
                # -----------------
                st.markdown("""
                <style>
                .st-emotion-cache-wfksaw {
                    gap: 0.2rem !important; /* Reduce the gap to a smaller value */
                }
                </style>
                """, unsafe_allow_html=True)
                # _, center_col, _ = st.columns([20, 80, 10])
                # with center_col:
                    # Make location type persistent using cookies
                    # if "location_type_base_active" not in user_filters:
                    #     user_filters["location_type_base_active"] = st.session_state.get("location_type_base_active", "MSA")
                    # if "location_type_base_active" not in st.session_state:
                    #     st.session_state["location_type_base_active"] = user_filters.get("location_type_base_active", "MSA")

                    # # location_type_base_active = st.radio(
                    # #     "Filter by location",   
                    # #     ["MSA", "Zip Code"],
                    # #     horizontal=True,
                    # #     key="location_type_base_active",
                    # #     label_visibility="collapsed"
                    # # )
                    # location_type_base_active = st.session_state.get("location_type_base_active", "MSA")

                    # # Update cookies if location type changed
                    # if location_type_base_active != user_filters.get("location_type_base_active", "MSA"):
                    #     user_filters["location_type_base_active"] = location_type_base_active
                    #     cookie_controller.set(
                    #         "auth_data",
                    #         json.dumps(auth_cookie),                    # must be a string
                    #         expires=datetime.utcnow() + timedelta(days=30),
                    #         path="/",
                    #         domain=domain,
                    #     )

                # Get available MSA names for later use
                msa_names = sorted(filtered_data_msa['MsaName'].dropna().unique().tolist())
                msa_names = ["All"] + msa_names

                # Get current selections from cookies
                current_msa = user_filters.get("selected_msa_name", ["All"])
                if "All" in current_msa:
                    current_msa = ["All"]
                else:
                    # Get available MSA names for validation
                    temp_msa_names = sorted(filtered_data_msa['MsaName'].dropna().unique().tolist())
                    temp_msa_names = ["All"] + temp_msa_names
                    valid_msa = [m for m in current_msa if m in temp_msa_names]
                    current_msa = valid_msa if valid_msa else ["All"]

                # Get current zip code selection from cookies
                # current_zip = user_filters.get("selected_zip_code_active", ["All"])
                # if "All" in current_zip:
                #     current_zip = ["All"]
                # else:
                #     # Get available zip codes for validation
                #     temp_zip_names = sorted(filtered_data_msa['PostalCode'].dropna().astype(str).unique().tolist())
                #     temp_zip_names = ["All"] + temp_zip_names
                #     valid_zip = [z for z in current_zip if z in temp_zip_names]
                #     current_zip = valid_zip if valid_zip else ["All"]


                    # ------------- MSA OPTIONS -------------
                multiselect_label = "Select Metropolitan Statistical Area (MSA)"
                selected_msa_name = st.multiselect(
                    multiselect_label,
                    msa_names,
                    default=current_msa,
                    help="A metro region centered on a large city plus its economically linked suburbs.",
                    key="multiselect_msa_base"
                )

                # Clean selection
                if len(selected_msa_name) > 1 and "All" in selected_msa_name:
                    if selected_msa_name[-1] == "All":
                        selected_msa_name = ["All"]
                    else:
                        selected_msa_name = [m for m in selected_msa_name if m != "All"]
                elif not selected_msa_name:
                        selected_msa_name = ["All"]

                # Update cookies if MSA selection changed
                if selected_msa_name != current_msa:
                    user_filters["selected_msa_name"] = selected_msa_name
                    user_filters["selected_zip_code_active"] = ["All"]
                    save_auth_cookie()
                    st.rerun()

                    # For downstream use
                # selected_zip_code_active = ["All"]


                # ------------- ZIP CODE OPTIONS -------------
                # Get available zip codes from filtered data
                zip_filtered = filtered_data_msa.copy()
                current_zip = user_filters.get("selected_zip_code_active", ["All"])
                # Apply MSA filter if any MSA is selected
                if "All" not in selected_msa_name:
                    zip_filtered = zip_filtered[zip_filtered["MsaName"].isin(selected_msa_name)]
                
                zip_names = ["All"] + sorted(
                    z for z in (
                            zip_filtered["PostalCode"]
                            .dropna()
                            .astype(str)
                            .map(str.strip)
                            .unique()
                            .tolist()
                        )
                        if z not in {"0", "0.0", ""}   # remove zeros/empty
                    )

                if "All" not in current_zip:
                    current_zip = [z for z in current_zip if z in zip_names]
                if not current_zip:
                    current_zip = ["All"]
                    user_filters["selected_zip_code_active"] = current_zip
                multiselect_label = "Select Zip Code"
                selected_zip_code_active = st.multiselect(
                        multiselect_label,
                        zip_names,
                        default=current_zip,
                        help="U.S. postal area where the store is located.",
                        key="multiselect_zip_base"
                )

                    # Clean selection
                if len(selected_zip_code_active) > 1 and "All" in selected_zip_code_active:
                    if selected_zip_code_active[-1] == "All":
                        selected_zip_code_active = ["All"]
                    else:
                        selected_zip_code_active = [z for z in selected_zip_code_active if z != "All"]
                elif not selected_zip_code_active:
                    selected_zip_code_active = ["All"]

                # Update cookies if zip code selection changed
                if selected_zip_code_active != current_zip:
                    user_filters["selected_zip_code_active"] = selected_zip_code_active
                    cookie_controller.set(
                        "auth_data",
                        json.dumps(auth_cookie),                    # must be a string
                        expires=datetime.utcnow() + timedelta(days=30),
                        path="/",
                        domain=domain,
                    )
                    st.rerun()

                    # For downstream use
                selected_msa_name = ["All"]

                # Final output (for further downstream filtering, always get from cookie)
                selected_msa_name = user_filters.get("selected_msa_name", ["All"])
                selected_zip_code_active = user_filters.get("selected_zip_code_active", ["All"])

                if "All" in selected_msa_name:
                    selected_msa_name = msa_names

                if "All" in selected_zip_code_active:
                    selected_zip_code_active = zip_names 

            logging.info("--- LOCATION SECTION END ---")


        # Apply filters to closed data

        filtered_active = active_data.copy()
        filtered_active_previous = active_data.copy()

        if parent_chain_name:
            filtered_active = filtered_active[filtered_active['ParentName_Coresight'].isin(parent_chain_name)]

        if selected_chain_name:
            filtered_active = filtered_active[filtered_active['ChainName_Coresight'].isin(selected_chain_name)]

        # Apply the date range filter
        selected_date_range = st.session_state.get("selected_date_range")
        if "selected_date_range" in st.session_state and isinstance(st.session_state["selected_date_range"], tuple):
            start_date, end_date = st.session_state["selected_date_range"]
            start_timestamp = pd.Timestamp(start_date)
            end_timestamp = pd.Timestamp(end_date)
            filtered_active = filtered_active[
                (filtered_active['Period'] >= start_timestamp) &
                (filtered_active['Period'] <= end_timestamp)
            ]
        else:
            selected_date_range, _, _ = get_synchronized_date_range(
                active_data,
                default_months_back=12,
                label="active.py"
            )
            start_date, end_date = selected_date_range
            start_timestamp = pd.Timestamp(start_date)
            end_timestamp = pd.Timestamp(end_date)
            filtered_active = filtered_active[
                (filtered_active['Period'] >= start_timestamp) &
                (filtered_active['Period'] <= end_timestamp)
            ]
            
            st.session_state["selected_date_range"] = (start_date, end_date)

        if selected_sector_name != "All":
            if selected_sector_name == None:
                filtered_active = filtered_active[filtered_active['Sector_Coresight'].isnull()]
            else:
                filtered_active = filtered_active[
                filtered_active['Sector_Coresight'] == selected_sector_name
                ]

        if selected_state_name:
            filtered_active = filtered_active[filtered_active['State'].isin(selected_state_name)]
        if selected_msa_name:
            filtered_active = filtered_active[filtered_active['MsaName'].isin(selected_msa_name)]

        # Apply Zip Code filter if using Zip Code location type
        if selected_zip_code_active and "All" not in selected_zip_code_active:
            # Handle NaN and type conversion for PostalCode
            filtered_active['PostalCode'] = filtered_active['PostalCode'].astype(str).fillna('')
            filtered_active = filtered_active[filtered_active['PostalCode'].isin([str(z) for z in selected_zip_code_active])]

        #Previous Period calculations for each filters

        if parent_chain_name:
            filtered_active_previous = filtered_active_previous[filtered_active_previous['ParentName_Coresight'].isin(parent_chain_name)]

        if selected_chain_name:
            filtered_active_previous = filtered_active_previous[filtered_active_previous['ChainName_Coresight'].isin(selected_chain_name)]

        # Calculate the previous period date range
        if "selected_date_range" in st.session_state and isinstance(st.session_state["selected_date_range"], tuple):
            selected_date_range = st.session_state["selected_date_range"]
            start_date = pd.Timestamp(start_date)
            end_date = pd.Timestamp(end_date)
            period_duration = end_date - start_date
            # st.write(f"Perioud Duration: {period_duration}")
            previous_start_date = start_date - period_duration
            # st.write(f"Previous Period Start Date: {previous_start_date}")
            previous_end_date = start_date - pd.Timedelta(days=1)
            # st.write(f"Previous Period End Date: {previous_end_date}")

                # Filter data for the previous period
            filtered_active_previous = filtered_active_previous[
                (filtered_active_previous['Period'] >= previous_start_date) &
                (filtered_active_previous['Period'] <= previous_end_date)
            ]

        if selected_sector_name != "All":
            if selected_sector_name == None:
                filtered_active_previous = filtered_active_previous[filtered_active_previous['Sector_Coresight'].isnull()]
            else:
                filtered_active_previous = filtered_active_previous[
                filtered_active_previous['Sector_Coresight'] == selected_sector_name
                ]

        if selected_state_name != 'All':
            filtered_active_previous = filtered_active_previous[filtered_active_previous['State'].isin(selected_state_name)]
        if selected_msa_name != 'All':
            filtered_active_previous = filtered_active_previous[filtered_active_previous['MsaName'].isin(selected_msa_name)]

        # Apply Zip Code filter to previous period data if using Zip Code location type
        if selected_zip_code_active != ["All"]:
            filtered_active_previous['PostalCode'] = filtered_active_previous['PostalCode'].astype(str).fillna('')
            filtered_active_previous = filtered_active_previous[filtered_active_previous['PostalCode'].isin([str(z) for z in selected_zip_code_active])]

        st.markdown("""<style>.custom-hr1 {border: none;
        border-top: 1px solid #808080; /* Medium grey color */
        width: 100%; /* Adjust width to your preference */
        margin-top: 0px; /* Adjust the top margin to pull the line closer */
        margin-bottom: 10px; /* Optional: Adjust space below the line */
        padding: 0; /* Ensure no padding */
        margin-left: auto; /* Center align the line */
        margin-right: auto; /* Center align the line */
        }
        </style>
        """,
        unsafe_allow_html=True
        )

        # Add the horizontal line with the custom class
        st.markdown('<hr class="custom-hr1">', unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])


        active_data['Period'] = pd.to_datetime(active_data['Period'])

        # Get the most recent date in the data
        max_date = filtered_active['Period'].max()

        # Extract the most recent month and year
        current_month = max_date.month
        current_year = max_date.year

        # Filter for rows in the last calendar month only
        filtered_data = filtered_active[
            (active_data['Period'].dt.month == current_month) &
            (active_data['Period'].dt.year == current_year)
        ]

        # Count active stores
        filtered_active_sum = active_data.copy()
        start_date_plot = filtered_active['Period'].min()
        end_date_plot = filtered_active['Period'].max()
        filtered_Recent = filtered_active_sum[
                (active_data['Period'].dt.month == current_month) &
                (active_data['Period'].dt.year == current_year) &
                (active_data['ChainName_Coresight'].notnull())
        ]
        filtered_active_sum = filtered_active_sum[
                (filtered_active_sum['Period'] >= start_date_plot) &
                (filtered_active_sum['Period'] <= end_date_plot) &
                (filtered_active_sum['ChainName_Coresight'].notnull())
        ]
        if "All" not in parent_chain_name and parent_chain_name != []:
            filtered_active_sum = filtered_active_sum[
                filtered_active_sum["ParentName_Coresight"].isin(parent_chain_name)
            ]
            filtered_Recent = filtered_Recent[
                filtered_Recent["ParentName_Coresight"].isin(parent_chain_name)
            ]
        if "All" not in selected_chain_name and selected_chain_name != []:
            filtered_active_sum = filtered_active_sum[
                filtered_active_sum["ChainName_Coresight"].isin(selected_chain_name)
            ]
            filtered_Recent = filtered_Recent[
                filtered_Recent["ChainName_Coresight"].isin(selected_chain_name)
            ]
        if selected_sector_name != "All":
            if selected_sector_name is None:
                filtered_active_sum = filtered_active_sum[filtered_active_sum["Sector_Coresight"].isnull()]
                filtered_Recent = filtered_Recent[filtered_Recent["Sector_Coresight"].isnull()]
            else:
                filtered_active_sum = filtered_active_sum[filtered_active_sum["Sector_Coresight"] == selected_sector_name]
                filtered_Recent = filtered_Recent[filtered_Recent["Sector_Coresight"] == selected_sector_name]

        if selected_state_name and "All" not in selected_state_name:
            filtered_active_sum = filtered_active_sum[filtered_active_sum["State"].isin(selected_state_name)]
            filtered_Recent = filtered_Recent[filtered_Recent["State"].isin(selected_state_name)]

        if selected_msa_name and "All" not in selected_msa_name:
            filtered_active_sum = filtered_active_sum[filtered_active_sum["MsaName"].isin(selected_msa_name)]
            filtered_Recent = filtered_Recent[filtered_Recent["MsaName"].isin(selected_msa_name)]

        # Apply Zip Code filter to sum calculations if using Zip Code location type
        if selected_zip_code_active and "All" not in selected_zip_code_active:
            filtered_active_sum['PostalCode'] = filtered_active_sum['PostalCode'].astype(str).fillna('')
            filtered_active_sum = filtered_active_sum[filtered_active_sum['PostalCode'].isin([str(z) for z in selected_zip_code_active])]
            filtered_Recent['PostalCode'] = filtered_Recent['PostalCode'].astype(str).fillna('')
            filtered_Recent = filtered_Recent[filtered_Recent['PostalCode'].isin([str(z) for z in selected_zip_code_active])]

        sum_active_stores1 = filtered_active_sum['ChainName_Coresight'].count()
        sum_active_stores = filtered_Recent['ChainName_Coresight'].count()
        # Display total active stores
        with col1:
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{sum_active_stores:,}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>Total Active Stores</h6>
            """, unsafe_allow_html=True)

        with col2:
            total_retailers = filtered_active_sum['ChainName_Coresight'].nunique()
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{total_retailers}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>Total Affected Retailers</h6>
            """, unsafe_allow_html=True)
        with col3:
            total_categories = filtered_active_sum['Sector_Coresight'].nunique()
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{total_categories}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>Total Affected Sectors</h6>
            """, unsafe_allow_html=True)

        # sum_active_stores_previous = filtered_active_previous['ChainName'].count()
        # if sum_active_stores_previous > 0:  # Avoid division by zero
        #     percent_difference = ((sum_active_stores - sum_active_stores_previous) / sum_active_stores_previous) * 100
        # else:
        #     percent_difference = None  # Handle cases where previous period data is zero or unavailable

        # with col4:
        #     st.markdown(f"""
        #     <h1 style='font-size: 40px; text-align: center; margin-bottom: 0px;'>{percent_difference:.1f}%</h1>
        #     <h6 style='text-align: center; margin-top: 0px;'>% Change Compared to Previous Period</h6>
        #     """, unsafe_allow_html=True)


        total_population = filtered_active_sum['Population'].sum()
        active_stores_per_10k = (sum_active_stores1 / total_population) * 10000 if total_population > 0 else None
        with col4:
            display_value = f"{active_stores_per_10k:.3f}" if active_stores_per_10k else "N/A"
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{display_value}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>Active Stores per 10,000 people</h6>
            """, unsafe_allow_html=True)

        st.markdown("""<style>.custom-hr {border: none;border-top: 1px solid #808080; width: 100%; margin: -10px; padding: 0; margin-left: auto; margin-right: auto;
        }
        </style>
        """,
        unsafe_allow_html=True
        )

        st.markdown('<hr class="custom-hr">', unsafe_allow_html=True)

        # Helper function to calculate the next highest rounded limit
        def calculate_yaxis_limit(value):
            try:
                if pd.isna(value):  # Check for NaN values
                    st.warning("No data available to display the active Stores Over Time. Please select broader date range.")
                if value <= 10:
                    return 10
                increased_value = value + (value * 0.1)
                return math.ceil(increased_value / 10) * 10
            except ValueError as e:
                print("")
                return None  # Return None if invalid value




        # Create columns with 70%-30% width distribution
        col1, col2 = st.columns([7, 3])


        try:
            filtered_active_sum['PeriodMonth'] = filtered_active_sum['Period'].dt.to_period('M').dt.to_timestamp()

            active_stores_over_time = filtered_active_sum.groupby('PeriodMonth').size().reset_index(name='ActiveCount')
            active_stores_over_time.rename(columns={'PeriodMonth': 'MonthStart'}, inplace=True)

            # Create lists to store metadata for each data point
            num_sectors_list = []
            num_banners_list = []
            num_states_list = []
            num_msa_list = []

            # Calculate metadata for each month
            for month in active_stores_over_time['MonthStart']:
                month_data = filtered_active_sum[filtered_active_sum['PeriodMonth'] == month]
                num_sectors_list.append(month_data['Sector_Coresight'].nunique())
                num_banners_list.append(month_data['ChainName_Coresight'].nunique())
                num_states_list.append(month_data['State'].nunique())
                num_msa_list.append(month_data['MsaName'].nunique())

            # Create the figure with visible counts
            fig_active_line = go.Figure()

            # Add trace with both permanent labels and hover info
            fig_active_line.add_trace(go.Scatter(
                x=active_stores_over_time['MonthStart'],
                y=active_stores_over_time['ActiveCount'],
                name='Active Stores',
                mode='lines+markers+text',
                text=[f"{x:,}" for x in active_stores_over_time['ActiveCount']],
                textposition='top center',
                line=dict(color='#CBCACA', width=2),
                marker=dict(size=7, color='#CBCACA'),
                hovertemplate=(
                    "%{x|%b %Y}<br>"
                    "<span style='color:#CBCACA'>●</span> "
                    "<b>Active Store Count:</b> <b>%{y:,}</b><br><br>"
                    # "<span style='color:#CBCACA'>•</span> "
                    # "<span style='color:black'>Date Period: %{x|%b %Y}</span><br>"
                    "<span style='color:#CBCACA'>•</span> "
                    "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                    "<span style='color:#CBCACA'>•</span> "
                    "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                    "<span style='color:#CBCACA'>•</span> "
                    "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                    "<span style='color:#CBCACA'>•</span> "
                    "<span style='color:black'>Number of MSA: %{customdata[3]}</span><br>"
                    "<extra></extra>"
                ),
                customdata=list(zip(num_sectors_list, num_banners_list, num_states_list, num_msa_list)),
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor="#CBCACA",
                    font=dict(size=13, color="black", family="Arial")
                )
            ))

            # Update layout with text styling
            fig_active_line.update_layout(
                yaxis_title="Active Stores",
                margin=dict(l=10, r=10, t=60, b=40),
                height=400,
                xaxis=dict(
                    showline=True,
                    zeroline=False,
                    title="",
                    tickformat="%b %Y"
                ),
                yaxis=dict(
                    showline=True,
                    zeroline=False,
                    automargin=True
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                uniformtext_minsize=8,
                uniformtext_mode='hide'
            )

            # Same config as before
            config = {
                'displayModeBar': True,
                'displaylogo': False,
                'toImageButtonOptions': {
                    'format': 'png',
                    'filename': 'active_stores_over_time',
                    'height': 400,
                    'width': 700,
                    'scale': 1
                }
            }

            # Display the chart
            with col1:
                st.markdown("<h4 style='font-size: 20px;text-align: center;'>Active Stores Over Time</h4>", unsafe_allow_html=True)
                st.plotly_chart(fig_active_line, use_container_width=True, config=config)

        except Exception as e:
            st.write(f"Error creating chart: {e}")

        # --- Active Chains Bar Chart (Recent Month Only) ---
        try:
            # Get the most recent period from the filtered data
            recent_period = filtered_active_sum['Period'].max()

            # Filter for only the most recent period's data
            recent_active_data = filtered_active_sum[filtered_active_sum['Period'] == recent_period]

            # Count stores by chain for the recent period only
            active_store_counts = recent_active_data['ChainName_Coresight'].value_counts().head(15)

            if not active_store_counts.empty:
                y_max_active = calculate_yaxis_limit(active_store_counts.max())

                # Calculate metadata for the recent period
                recent_period_str = recent_period.strftime('%b %Y')
                num_states = recent_active_data['State'].nunique()
                num_msa = recent_active_data['MsaName'].nunique()

                # Create figure with consistent styling
                fig_active_bar = go.Figure()

                fig_active_bar.add_trace(go.Bar(
                    x=active_store_counts.index,
                    y=active_store_counts.values,
                    marker_color='#CBCACA',  # Using your specified gray color
                    text=[f"{x:,}" for x in active_store_counts.values],  # Formatted with thousands separator
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"  # Banner name
                        "<span style='color:#CBCACA'>●</span> "
                        "<b>Active Stores:</b> %{y:,}<br><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Date Period: {recent_period_str}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Number of States: {num_states:,}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Number of MSA: {num_msa:,}</span><br>"
                        "<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor='#CBCACA',
                        font=dict(size=13, color="black", family="Arial")
                    )
                ))

                # Calculate dynamic height based on number of banners
                dynamic_height = 400 + (20 * (len(active_store_counts) - 10)) if len(active_store_counts) > 10 else 400

                fig_active_bar.update_layout(
                    xaxis_title="Banner Name",
                    yaxis_title="Active Stores",
                    yaxis=dict(
                        range=[0, y_max_active],
                        autorange=False,
                        showline=True,
                        zeroline=False
                    ),
                    xaxis=dict(
                        showline=True,
                        zeroline=False,
                        tickangle=45 if len(active_store_counts) > 5 else 0,
                        tickfont=dict(size=11)  # Smaller font for long banner names
                    ),
                    height=dynamic_height,
                    margin=dict(l=20, r=20, t=60, b=40 + (10 * len(active_store_counts))),  # Dynamic bottom margin
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )

                # Adjust for many banners
                if len(active_store_counts) > 8:
                    fig_active_bar.update_layout(
                        xaxis=dict(tickangle=60),
                        margin=dict(b=60 + (10 * len(active_store_counts)))  # Extra bottom margin
                    )

                # Display the chart
                with col2:
                    num_banners = min(15, active_store_counts.shape[0])
                    st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {num_banners} Banners</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_active_bar, use_container_width=True)
            else:
                st.warning("No active store data available for the most recent period.")

        except Exception as e:
            logging.error(f"Error generating active stores bar chart: {e}")
            st.warning("Could not display active stores chart.")


        # Dictionary to convert full state names to two-letter abbreviations
        state_abbreviations = {
            'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
            'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
            'District of Columbia': 'DC', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI',
            'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
            'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
            'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
            'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
            'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
            'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
            'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
            'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
            'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
            'Wisconsin': 'WI', 'Wyoming': 'WY', 'Puerto Rico': 'PR'
        }

        # Reverse dictionary to convert abbreviations back to full names
        state_names = {v: k for k, v in state_abbreviations.items()}

        # Approximate center coordinates for each state
        state_centers = {
            'AL': (-86.7, 32.7), 'AK': (-152.4, 64.2), 'AZ': (-111.9, 34.3), 'AR': (-92.4, 34.8),
            'CA': (-119.4, 37.3), 'CO': (-105.5, 39.0), 'CT': (-72.7, 41.6), 'DE': (-75.5, 39.0),
            'DC': (-77.0, 38.9), 'FL': (-82.5, 27.8), 'GA': (-83.4, 32.6), 'HI': (-157.5, 20.9),
            'ID': (-114.4, 44.2), 'IL': (-89.4, 40.0), 'IN': (-86.2, 40.3), 'IA': (-93.5, 42.1),
            'KS': (-98.3, 38.5), 'KY': (-85.0, 37.5), 'LA': (-91.1, 30.9), 'ME': (-69.4, 45.3),
            'MD': (-76.6, 39.0), 'MA': (-71.8, 42.3), 'MI': (-84.5, 44.2), 'MN': (-94.3, 46.3),
            'MS': (-89.7, 32.7), 'MO': (-92.6, 38.5), 'MT': (-110.0, 46.9), 'NE': (-99.9, 41.5),
            'NV': (-116.6, 39.3), 'NH': (-71.6, 43.7), 'NJ': (-74.4, 40.1), 'NM': (-106.0, 34.5),
            'NY': (-75.5, 43.0), 'NC': (-79.0, 35.6), 'ND': (-100.5, 47.6), 'OH': (-82.8, 40.4),
            'OK': (-97.5, 35.5), 'OR': (-120.6, 43.8), 'PA': (-77.2, 40.9), 'RI': (-71.5, 41.7),
            'SC': (-80.9, 33.9), 'SD': (-100.4, 44.4), 'TN': (-86.6, 35.7), 'TX': (-99.9, 31.5),
            'UT': (-111.6, 39.3), 'VT': (-72.7, 44.0), 'VA': (-78.9, 37.8), 'WA': (-120.7, 47.5),
            'WV': (-80.6, 38.5), 'WI': (-89.9, 44.5), 'WY': (-107.6, 43.1), 'PR': (-66.5, 18.2)
        }

        # Function to map full state names to abbreviations
        def map_state_names(df, state_col='State'):
            df[state_col] = df[state_col].map(state_abbreviations)
            return df

        # Apply the mapping to filtered_closed and filtered_active
        filtered_data = map_state_names(filtered_data)
        filtered_data_for_State = map_state_names(filtered_active_sum)


        # --- Active Stores by State Map ---
        col1, col2 = st.columns([7, 3])

        #st.subheader('Active Stores by State')


        # --- Active Stores by State Map (Recent Month Only) ---
        try:
            # Get the most recent period from the filtered data
            recent_period = filtered_data_for_State['Period'].max()
            
            # Filter for only the most recent period's data
            recent_active_data = filtered_data_for_State[filtered_data_for_State['Period'] == recent_period]
            
            # Group recent month's data by state
            active_by_state = recent_active_data.groupby('State').size().reset_index(name='Active Stores')
            active_by_state = active_by_state.dropna()  # Drop any rows with unmapped states

            if active_by_state.empty or active_by_state['State'].isna().all():
                # If no data, display a message and do not plot the graph
                with col1:
                    st.markdown(
                        f"<h4 style='font-size: 20px;text-align: center; color: red;'>No active store data available by state for {recent_period.strftime('%b %Y')}</h4>",
                        unsafe_allow_html=True
                    )
            else:
                def calculate_distance(lat1, lon1, lat2, lon2):
                    """Calculate distance between two points on map"""
                    return math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)

                def detect_overlapping_states_by_active_count(active_by_state, state_centers, min_distance=1.5):
                    """
                    Detect states that are too close to each other and would cause overlapping text
                    Returns a list of states to exclude from annotations
                    (Modified version for active store counts)
                    """
                    states_to_exclude = set()
                    
                    # Known problematic clusters in the Northeast and small states
                    northeast_clusters = [
                        ['CT', 'RI', 'MA'],  # Connecticut, Rhode Island, Massachusetts cluster
                        ['VT', 'NH', 'ME'],  # Vermont, New Hampshire, Maine cluster
                        ['NJ', 'DE', 'MD'],  # New Jersey, Delaware, Maryland cluster
                    ]
                    
                    # Additional states to always exclude (small or problematic positioning)
                    always_exclude = ['ME', 'MA', 'DC', 'WV']
                    
                    # Add always excluded states
                    for state in always_exclude:
                        if state in active_by_state['State'].values:
                            states_to_exclude.add(state)
                    
                    # Check each cluster and keep only the state with highest value
                    for cluster in northeast_clusters:
                        cluster_states = [state for state in cluster if state in active_by_state['State'].values]
                        if len(cluster_states) > 1:
                            # Find the state with highest active store count in this cluster
                            cluster_data = active_by_state[active_by_state['State'].isin(cluster_states)]
                            max_state = cluster_data.loc[cluster_data['Active Stores'].idxmax(), 'State']
                            
                            # Exclude all others in the cluster
                            for state in cluster_states:
                                if state != max_state:
                                    states_to_exclude.add(state)
                    
                    # Additional proximity check for any remaining states
                    remaining_states = [state for state in active_by_state['State'] if state not in states_to_exclude]
                    
                    for i, state1 in enumerate(remaining_states):
                        if state1 in states_to_exclude:
                            continue
                            
                        for state2 in remaining_states[i+1:]:
                            if state2 in states_to_exclude:
                                continue
                                
                            if state1 in state_centers and state2 in state_centers:
                                distance = calculate_distance(
                                    state_centers[state1][1], state_centers[state1][0],
                                    state_centers[state2][1], state_centers[state2][0]
                                )
                                
                                if distance < min_distance:
                                    # Keep the state with higher active store count
                                    state1_value = active_by_state[active_by_state['State'] == state1]['Active Stores'].iloc[0]
                                    state2_value = active_by_state[active_by_state['State'] == state2]['Active Stores'].iloc[0]
                                    
                                    if state1_value < state2_value:
                                        states_to_exclude.add(state1)
                                    else:
                                        states_to_exclude.add(state2)
                    
                    return list(states_to_exclude)

                # Create reverse dictionary to convert abbreviations to full names
                abbreviation_to_full = {v: k for k, v in state_abbreviations.items()}

                # Add full state name column to the dataframe
                active_by_state['State_Full'] = active_by_state['State'].map(abbreviation_to_full)

                # Calculate metadata for each state
                state_metadata = {}
                for state in active_by_state['State']:
                    state_data = recent_active_data[recent_active_data['State'] == state]
                    state_metadata[state] = {
                        'num_sectors': state_data['Sector_Coresight'].nunique() if 'Sector_Coresight' in state_data.columns else 0,
                        'num_banners': state_data['ChainName_Coresight'].nunique() if 'ChainName_Coresight' in state_data.columns else 0,
                        'num_msa': state_data['MsaName'].nunique() if 'MsaName' in state_data.columns else 0
                    }

                # Get the date period (single month for active stores)
                date_period = recent_period.strftime('%B %Y')

                # Create the choropleth map
                fig_active_map = px.choropleth(
                    active_by_state,
                    locations="State",
                    locationmode="USA-states",
                    color="Active Stores",
                    color_continuous_scale=["#ffffff", "#CBCACA"],  # White to gray color scale
                    scope="usa",
                    hover_name="State_Full",
                    hover_data={"State": False, "State_Full": False}  # Only show what we specify in hovertemplate
                )

                # Customize hover template with state-specific metadata
                fig_active_map.update_traces(
                    hovertemplate=(
                        "<b>%{hovertext}</b><br>"  # State name
                        "<span style='color:#CBCACA'>●</span> "  # Gray bullet
                        "<b>Active Stores:</b> %{z:,}<br><br>"  # Formatted count
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Date Period: {date_period}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        "<span style='color:black'>Number of Sectors: %{customdata[0]:,}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        "<span style='color:black'>Number of Banners: %{customdata[1]:,}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        "<span style='color:black'>Number of MSA: %{customdata[2]:,}</span><br>"
                        "<extra></extra>"
                    ),
                    customdata=[[
                        state_metadata[state]['num_sectors'],
                        state_metadata[state]['num_banners'],
                        state_metadata[state]['num_msa']
                    ] for state in active_by_state['State']],
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor="#CBCACA",  # Gray border
                        font=dict(size=13, color="black", family="Arial")
                    )
                )

                # Customize layout
                fig_active_map.update_layout(
                    margin=dict(l=0, r=0, t=0, b=0),
                    geo=dict(
                        projection_scale=1.2,
                        center=dict(lat=37.5, lon=-95),
                        showlakes=True,
                        lakecolor='rgb(255, 255, 255)',
                        showframe=False,
                        showcoastlines=False,
                        bgcolor='rgba(0,0,0,0)'  # Transparent background
                    )
                )

                # State annotations (with hover disabled)
                excluded_states = detect_overlapping_states_by_active_count(active_by_state, state_centers)
                states_to_annotate = active_by_state[~active_by_state['State'].isin(excluded_states)]

                annotations_active = go.Scattergeo(
                    locationmode='USA-states',
                    lon=[state_centers[state][0] for state in states_to_annotate['State'] if state in state_centers],
                    lat=[state_centers[state][1] for state in states_to_annotate['State'] if state in state_centers],
                    text=[f"{state}<br>{count:,}" 
                        for state, count in zip(states_to_annotate['State'], states_to_annotate['Active Stores'])
                        if state in state_centers],
                    mode='text',
                    showlegend=False,
                    textfont=dict(size=11, color="black"),
                    hoverinfo='skip'  # Disables hover for annotations
                )
                fig_active_map.add_trace(annotations_active)

                # Display the map
                with col1:
                    st.markdown(
                        f"<h4 style='font-size: 20px;text-align: center;'>Active Stores by State ({recent_period.strftime('%b %Y')})</h4>",
                        unsafe_allow_html=True
                    )
                    st.plotly_chart(fig_active_map, use_container_width=True)

        except Exception as e:
            logging.error(f"Error generating active stores by state map: {e}")
            with col1:
                st.markdown(
                    "<h4 style='font-size: 20px;text-align: center; color: red;'>Error loading state data</h4>",
                    unsafe_allow_html=True
                )

        # --- Cities Bar Chart (Recent Month Only) ---
        try:
            # Get the most recent period from the filtered data
            recent_period = filtered_active_sum['Period'].max()

            # Filter for only the most recent period's data
            recent_active_data = filtered_active_sum[filtered_active_sum['Period'] == recent_period]

            # Count stores by city for the recent period only
            active_store_counts = recent_active_data['City'].value_counts().head(15)

            if not active_store_counts.empty:
                y_max_active = calculate_yaxis_limit(active_store_counts.max())
                
                # Calculate metadata for each city
                city_metadata = {}
                for city in active_store_counts.index:
                    city_data = recent_active_data[recent_active_data['City'] == city]
                    city_metadata[city] = {
                        'num_sectors': city_data['Sector_Coresight'].nunique(),
                        'num_banners': city_data['ChainName_Coresight'].nunique(),
                        'num_states': city_data['State'].nunique(),
                        'num_msa': city_data['MsaName'].nunique()
                    }
                
                # Calculate date period (single month for active stores)
                date_period = recent_period.strftime('%B %Y')

                # Create figure with consistent styling
                fig_active_bar = go.Figure()

                fig_active_bar.add_trace(go.Bar(
                    x=active_store_counts.index,
                    y=active_store_counts.values,
                    marker_color='#CBCACA',  # Using your specified gray color
                    text=[f"{x:,}" for x in active_store_counts.values],  # Formatted with thousands separator
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"  # City name
                        "<span style='color:#CBCACA'>●</span> "
                        "<b>Active Stores:</b> %{y:,}<br><br>"  # Formatted with commas
                        "<span style='color:#CBCACA'>•</span> "
                        "<span style='color:black'>Date Period: " + f"{date_period}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        "<span style='color:black'>Number of MSA: %{customdata[3]}</span><br>"
                        "<extra></extra>"
                    ),
                    customdata=[[
                        city_metadata[city]['num_sectors'],
                        city_metadata[city]['num_banners'],
                        city_metadata[city]['num_states'],
                        city_metadata[city]['num_msa']
                    ] for city in active_store_counts.index],
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor='#CBCACA',
                        font=dict(size=13, color="black", family="Arial")
                    )
                ))

                # Calculate dynamic height based on number of cities
                dynamic_height = 450 + (20 * (len(active_store_counts) - 10)) if len(active_store_counts) > 10 else 450

                fig_active_bar.update_layout(
                    xaxis_title="City",
                    yaxis_title="Active Stores",
                    yaxis=dict(
                        range=[0, y_max_active],
                        autorange=False,
                        showline=True,
                        zeroline=False
                    ),
                    xaxis=dict(
                        showline=True,
                        zeroline=False,
                        tickangle=45 if len(active_store_counts) > 5 else 0,
                        tickfont=dict(size=11)  # Smaller font for long city names
                    ),
                    height=dynamic_height,
                    margin=dict(l=20, r=20, t=60, b=40 + (10 * len(active_store_counts))),  # Dynamic bottom margin
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )

                # Adjust for many cities
                if len(active_store_counts) > 8:
                    fig_active_bar.update_layout(
                        xaxis=dict(tickangle=60),
                        margin=dict(b=60 + (10 * len(active_store_counts)))  # Extra bottom margin
                    )

                # Display the chart
                with col2:
                    st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {active_store_counts.shape[0]} Cities</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_active_bar, use_container_width=True)
            else:
                with col2:
                    st.markdown(
                        f"<h4 style='font-size: 20px;text-align: center; color: red;'>No city data available for {recent_period.strftime('%b %Y')}</h4>",
                        unsafe_allow_html=True
                    )

        except Exception as e:
            logging.error(f"Error generating cities chart: {e}")
            with col2:
                st.markdown(
                    "<h4 style='font-size: 20px;text-align: center; color: red;'>Error loading city data</h4>",
                    unsafe_allow_html=True
                )

        #Stores per capita by state
        col1, col2 = st.columns([7, 3])

        # --- Active Stores per Capita by State (Recent Month Only) ---
        try:
            # Get the most recent period from the filtered data
            recent_period = filtered_active_sum['Period'].max()

            # Filter only the most recent period’s data
            recent_active_data = filtered_active_sum[filtered_active_sum['Period'] == recent_period].copy()
            active_by_state = (
                recent_active_data.dropna(subset=['State'])
                    .assign(State=lambda df: df['State'].astype(str).str.strip())
                    .groupby('State', as_index=False)
                    .size()
                    .rename(columns={'size': 'Active_Stores'})
            )
            active_by_state['Active_Stores'] = active_by_state['Active_Stores'].astype(int)
            pop_df = fetch_data_population()
            pop_df['usps_state_name'] = pop_df['usps_state_name'].astype(str).str.strip()
            pop_df['zip_code'] = pop_df['zip_code'].astype(str).str.strip()
            pop_df['estimate_total_population'] = pd.to_numeric(pop_df['estimate_total_population'], errors='coerce')

            state_pop_lookup = (
                pop_df.dropna(subset=['usps_state_name','zip_code','estimate_total_population'])
                      .query('estimate_total_population > 0 and zip_code not in ["0","00000",""]')
                      .drop_duplicates(subset=['usps_state_name','zip_code'])
                      .groupby('usps_state_name', as_index=False)['estimate_total_population']
                      .sum()
                      .rename(columns={
                          'usps_state_name': 'State',
                          'estimate_total_population': 'State_Population'
                      })
            )
            active_by_state = active_by_state.merge(state_pop_lookup, on='State', how='left')
            active_by_state = active_by_state[
                active_by_state['State_Population'].notna() & (active_by_state['State_Population'] > 0)
            ].copy()

            active_by_state['Active Stores per Capita'] = (
                active_by_state['Active_Stores'] / active_by_state['State_Population'] * 1_000_000
            ).astype(float)

            # Drop NaNs if any
            active_by_state = active_by_state.dropna(subset=['Active Stores per Capita'])
            active_by_state['Population'] = active_by_state['State_Population']

            if active_by_state.empty or active_by_state['State'].isna().all():
                with col1:
                    st.markdown(
                        f"<h4 style='font-size: 20px;text-align: center; color: red;'>No per capita data available for {recent_period.strftime('%b %Y')}</h4>",
                        unsafe_allow_html=True
                    )
            else:
                def calculate_distance(lat1, lon1, lat2, lon2):
                    """Calculate distance between two points on map"""
                    return math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)

                def detect_overlapping_states_by_active_per_capita(active_by_state, state_centers, min_distance=1.5):
                    """
                    Detect states that are too close to each other and would cause overlapping text
                    Returns a list of states to exclude from annotations
                    (Modified version for active stores per capita)
                    """
                    states_to_exclude = set()
                    
                    # Known problematic clusters in the Northeast and small states
                    northeast_clusters = [
                        ['CT', 'RI', 'MA'],  # Connecticut, Rhode Island, Massachusetts cluster
                        ['VT', 'NH', 'ME'],  # Vermont, New Hampshire, Maine cluster
                        ['NJ', 'DE', 'MD'],  # New Jersey, Delaware, Maryland cluster
                    ]
                    
                    # Additional states to always exclude (small or problematic positioning)
                    always_exclude = ['ME', 'MA', 'NJ', 'DC', 'WV']
                    
                    # Add always excluded states
                    for state in always_exclude:
                        if state in active_by_state['State'].values:
                            states_to_exclude.add(state)
                    
                    # Check each cluster and keep only the state with highest value
                    for cluster in northeast_clusters:
                        cluster_states = [state for state in cluster if state in active_by_state['State'].values]
                        if len(cluster_states) > 1:
                            # Find the state with highest active stores per capita in this cluster
                            cluster_data = active_by_state[active_by_state['State'].isin(cluster_states)]
                            max_state = cluster_data.loc[cluster_data['Active Stores per Capita'].idxmax(), 'State']
                            
                            # Exclude all others in the cluster
                            for state in cluster_states:
                                if state != max_state:
                                    states_to_exclude.add(state)
                    
                    # Additional proximity check for any remaining states
                    remaining_states = [state for state in active_by_state['State'] if state not in states_to_exclude]
                    
                    for i, state1 in enumerate(remaining_states):
                        if state1 in states_to_exclude:
                            continue
                            
                        for state2 in remaining_states[i+1:]:
                            if state2 in states_to_exclude:
                                continue
                                
                            if state1 in state_centers and state2 in state_centers:
                                distance = calculate_distance(
                                    state_centers[state1][1], state_centers[state1][0],
                                    state_centers[state2][1], state_centers[state2][0]
                                )
                                
                                if distance < min_distance:
                                    # Keep the state with higher active stores per capita
                                    state1_value = active_by_state[active_by_state['State'] == state1]['Active Stores per Capita'].iloc[0]
                                    state2_value = active_by_state[active_by_state['State'] == state2]['Active Stores per Capita'].iloc[0]
                                    
                                    if state1_value < state2_value:
                                        states_to_exclude.add(state1)
                                    else:
                                        states_to_exclude.add(state2)
                    
                    return list(states_to_exclude)

                # Create reverse dictionary to convert abbreviations to full names
                abbreviation_to_full = {v: k for k, v in state_abbreviations.items()}

                # Add full state name column to the dataframe
                active_by_state['State_Full'] = active_by_state['State'].map(abbreviation_to_full)

                # Calculate metadata for each state
                state_metadata = {}
                for state in active_by_state['State']:
                    state_data = recent_active_data[recent_active_data['State'] == state]
                    state_metadata[state] = {
                        'num_sectors': state_data['Sector_Coresight'].nunique() if 'Sector_Coresight' in state_data.columns else 0,
                        'num_banners': state_data['ChainName_Coresight'].nunique() if 'ChainName_Coresight' in state_data.columns else 0,
                        'num_msa': state_data['MsaName'].nunique() if 'MsaName' in state_data.columns else 0
                    }

                # Get the date period (single month for active stores)
                date_period = recent_period.strftime('%B %Y')

                # Create the choropleth map
                fig_active_map = px.choropleth(
                    active_by_state,
                    locations="State",
                    locationmode="USA-states",
                    color="Active Stores per Capita",
                    color_continuous_scale=["#ffffff", "#CBCACA"],
                    scope="usa",
                    hover_name="State_Full",
                    hover_data={
                        "Active_Stores": True,
                        "Active Stores per Capita": ":.2f",
                        "Population": True,
                        "State": False,
                        "State_Full": False
                    }
                )

                # Customize hover template with enhanced formatting and state-specific metadata
                fig_active_map.update_traces(
                    hovertemplate=(
                        "<b>%{hovertext}</b><br>"
                        "<span style='color:#CBCACA'>●</span> <b>Stores per Million:</b> %{customdata[1]:.2f}<br>"
                        "<span style='color:#CBCACA'>●</span> <b>Total Active Stores:</b> %{customdata[0]:,}<br>"
                        "<span style='color:#CBCACA'>●</span> <b>Population:</b> %{customdata[2]:,}<br><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Date Period: {date_period}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        "<span style='color:black'>Number of Sectors: %{customdata[3]:,}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        "<span style='color:black'>Number of Banners: %{customdata[4]:,}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        "<span style='color:black'>Number of MSA: %{customdata[5]:,}</span><br>"
                        "<extra></extra>"
                    ),
                    customdata=[[
                        row['Active_Stores'],  # Total active stores
                        row['Active Stores per Capita'],  # Per capita value
                        row['Population'],  # Population
                        state_metadata[row['State']]['num_sectors'],  # Number of sectors
                        state_metadata[row['State']]['num_banners'],  # Number of banners
                        state_metadata[row['State']]['num_msa']  # Number of MSA
                    ] for _, row in active_by_state.iterrows()],
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor="#CBCACA",
                        font=dict(size=13, color="black", family="Arial")
                    )
                )

                # Update color bar with proper formatting
                fig_active_map.update_coloraxes(
                    colorbar_title="Stores per Million",
                    colorbar_tickformat=".2f"
                )

                # Customize layout with clean styling
                fig_active_map.update_layout(
                    margin=dict(l=0, r=0, t=0, b=0),
                    geo=dict(
                        projection_scale=1.2,
                        center=dict(lat=37.5, lon=-95),
                        showlakes=True,
                        lakecolor='rgb(255, 255, 255)',
                        showframe=False,
                        showcoastlines=False,
                        bgcolor='rgba(0,0,0,0)'
                    )
                )

                # State annotations with improved formatting
                excluded_states = detect_overlapping_states_by_active_per_capita(active_by_state, state_centers)
                states_to_annotate = active_by_state[~active_by_state['State'].isin(excluded_states)]

                annotations_active = go.Scattergeo(
                    locationmode='USA-states',
                    lon=[state_centers[state][0] for state in states_to_annotate['State'] if state in state_centers],
                    lat=[state_centers[state][1] for state in states_to_annotate['State'] if state in state_centers],
                    text=[
                        f"{state}<br>{active_per_capita:,.2f}" 
                        for state, active_per_capita in zip(
                            states_to_annotate['State'],
                            states_to_annotate['Active Stores per Capita']
                        )
                        if state in state_centers
                    ],
                    mode='text',
                    showlegend=False,
                    textfont=dict(size=11, color="black"),
                    hoverinfo='skip'
                )
                fig_active_map.add_trace(annotations_active)

                # Display the map with responsive container
                with col1:
                    st.markdown(
                        f"<h4 style='font-size: 20px;text-align: center;'>Active Stores per Capita by State (per million) ({recent_period.strftime('%b %Y')})</h4>",
                        unsafe_allow_html=True
                    )
                    st.plotly_chart(fig_active_map, use_container_width=True)

        except Exception as e:
            logging.error(f"Error generating active stores per capita map: {e}")
            with col1:
                st.markdown(
                    "<h4 style='font-size: 20px;text-align: center; color: red;'>Error loading per capita data</h4>",
                    unsafe_allow_html=True
                )

        # --- Sector-wise Active Stores Bar Chart (Recent Month Only) ---
        try:
            recent_period = filtered_data_for_State['Period'].max()
            
            recent_active_data = filtered_data_for_State[filtered_data_for_State['Period'] == recent_period]
            
            active_store_counts = recent_active_data['Sector_Coresight'].value_counts().head(15).dropna()
            
            if active_store_counts.empty:
                with col2:
                    st.markdown(
                        "<h4 style='font-size: 20px;text-align: center; top-align: center; color: red;'>Sector-wise data not available.</h4>",
                        unsafe_allow_html=True
                    )
            else:
                # Calculate the maximum value for the y-axis
                y_max_active = calculate_yaxis_limit(active_store_counts.max())

                # Calculate metadata for the recent period
                recent_period_str = recent_period.strftime('%b %Y')
                num_banners = recent_active_data['ChainName_Coresight'].nunique()
                num_states = recent_active_data['State'].nunique()
                num_msa = recent_active_data['MsaName'].nunique()

                # Create figure with consistent styling
                fig_active_bar = go.Figure()

                fig_active_bar.add_trace(go.Bar(
                    x=active_store_counts.index,
                    y=active_store_counts.values,
                    marker_color='#CBCACA',  # Using your specified gray color
                    text=[f"{x:,}" for x in active_store_counts.values],  # Formatted with thousands separator
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"  # Sector name
                        "<span style='color:#CBCACA'>●</span> "
                        "<b>Active Stores:</b> %{y:,}<br><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Date Period: {recent_period_str}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Number of Banners: {num_banners:,}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Number of States: {num_states:,}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Number of MSA: {num_msa:,}</span><br>"
                        "<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor='#CBCACA',
                        font=dict(size=13, color="black", family="Arial")
                    )
                ))

                # Calculate dynamic height based on number of sectors
                dynamic_height = 400 + (20 * (len(active_store_counts) - 10)) if len(active_store_counts) > 10 else 400

                fig_active_bar.update_layout(
                    xaxis_title="Sector",
                    yaxis_title="Active Stores",
                    yaxis=dict(
                        range=[0, y_max_active],
                        autorange=False,
                        showline=True,
                        zeroline=False
                    ),
                    xaxis=dict(
                        showline=True,
                        zeroline=False,
                        tickangle=45 if len(active_store_counts) > 5 else 0,
                        tickfont=dict(size=11)  # Smaller font for long sector names
                    ),
                    height=dynamic_height,
                    margin=dict(l=20, r=20, t=60, b=40 + (10 * len(active_store_counts))),  # Dynamic bottom margin
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )

                # Adjust for many sectors
                if len(active_store_counts) > 8:
                    fig_active_bar.update_layout(
                        xaxis=dict(tickangle=60),
                        margin=dict(b=60 + (10 * len(active_store_counts)))  # Extra bottom margin
                    )

                # Display the chart
                with col2:
                    st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {active_store_counts.shape[0]} Sectors</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_active_bar, use_container_width=True)

        except Exception as e:
            logging.error(f"Error generating sector-wise active stores chart: {e}")
            with col2:
                st.markdown(
                    "<h4 style='font-size: 20px;text-align: center; color: red;'>Error loading sector data</h4>",
                    unsafe_allow_html=True
                )

        # --- Active Stores Tables ---
        filtered_data['Year'] = filtered_data['Period'].dt.year
        filtered_data['Month'] = filtered_data['Period'].dt.month
        filtered_data['Active Month/Year'] =  filtered_data['Month'].astype(str) + '-' + filtered_data['Year'].astype(str)
        filtered_data = filtered_data[['ChainName_Coresight','Label','ParentName_Coresight','Address','Address2','City','MsaName','PostalCode','State','Country','Sector_Coresight','Active Month/Year']]

        filtered_data = filtered_data.rename(columns={
            'ChainName_Coresight': 'Banner/Brand Name',
            'ParentName_Coresight': 'Company Name',
            'Label': 'Store Label',
            'Address': 'Address Line 1',
            'Address2': 'Address Line 2',
            'City': 'City',
            'MsaName': 'MSA',
            'PostalCode': 'Postal Code',
            'State': 'State',
            'Country': 'Country',
            'Sector_Coresight': 'Sector',
            'Closing Month/Year': 'Active Date'
        })
        filtered_data = filtered_data.reset_index(drop=True)
        col1, col4  = st.columns([7, 1.05])
        with col1:
            st.subheader('Active Stores Table')

        with col4:
            if not filtered_data.empty:
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                    filtered_data.to_excel(writer, index=False, sheet_name='Active_Stores')  # ← changed
                    worksheet = writer.sheets['Active_Stores']  # ← changed
                    for i, col in enumerate(filtered_data.columns):
                        column_len = max(
                            filtered_data[col].astype(str).map(len).max(),
                            len(col)
                        )
                        worksheet.set_column(i, i, column_len + 2)
                formatted_start_date = start_date.strftime("%Y-%m-%d")
                formatted_end_date = end_date.strftime("%Y-%m-%d")
                final_filename = f"Active_Stores_{formatted_start_date}_to_{formatted_end_date}.xlsx"
                
                st.markdown("""
                    <style>
                        .stDownloadButton>button {
                            background-color: #CBCACA;
                            color: white;
                            border: none;
                        }
                        .stDownloadButton>button:hover {
                            border: 2px solid #CBCACA;
                            background-color: white;
                            color: #CBCACA;
                        }
                    </style>
                """, unsafe_allow_html=True)

                # Your download button (gated)
                if not is_free_trial:
                    excel_buffer.seek(0)
                    st.download_button(
                        label="Download Data",
                        data=excel_buffer,  # This buffer now contains the autofitted Excel file
                        file_name=final_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_Active_xlsx",
                        use_container_width=True
                    )
                else:
                    if st.button("Download Data", key="download_active_blocked", use_container_width=True):
                        _trial_modal_active()

        st.markdown('</div>', unsafe_allow_html=True)
        GRID_H = 600  # must match your AgGrid height
        # Always show real data in the grid
        df_for_grid = filtered_active
        MAX_ROWS = 2000
        if(len(filtered_active) > MAX_ROWS):
            df_for_grid = filtered_active.head(MAX_ROWS)

        # Lock icon
        ICON = pathlib.Path(__file__).resolve().parent.parent / "assets" / "icons" / "lock.png"
        lock_b64 = base64.b64encode(ICON.read_bytes()).decode("utf-8")

        gb = GridOptionsBuilder.from_dataframe(df_for_grid)
        grid_options = gb.build()

        # Render the grid
        AgGrid(
            df_for_grid,
            gridOptions=grid_options,
            height=GRID_H,
            key="active_grid",
        )

        # Overlay (only for trial users)
        if is_free_trial:
            st.markdown(f"""
            <style>
              /* Overlay panel covers the grid completely */
              .grid-overlay-flag[data-target="active"] {{
                position: relative;
                height: {GRID_H}px;
                margin-top: -{GRID_H}px;   /* pull on top of grid */
                display: flex;
                align-items: center;
                justify-content: center;
                flex-direction: column;
                gap: 20px;
                background: rgba(255,255,255,0.55);  /* translucent white wash */
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

            <div class="grid-overlay-flag" data-target="active">
              <img src="data:image/png;base64,{lock_b64}" class="lock" alt="Locked"/>
              <div class="title">For SIP Members Only</div>
              <p style="margin:8px 0 18px 0;">
                Individual Store data is exclusively accessible to SIP members and is not available for trial users.
              </p>
              <a class="cta" href="https://coresight.com/contact/" target="_blank">Contact Us</a>
            </div>
            """, unsafe_allow_html=True)


        st.markdown("---")  # Horizontal line for separation
        try:
            # Get the current URL to determine environment
            current_url = st.context.headers.get("host", "")
            is_staging = "stage" in current_url.lower()
        except:
            # Fallback method
            is_staging = False

        # Set the appropriate URL based on environment
        if is_staging:
            overview_url = "https://stage3.coresight.com/store-intelligence-platform-overview/"
        else:
            overview_url = "https://www.coresight.com/store-intelligence-platform-overview/"

        st.markdown(f"<i>Data available through <b>{latest_ts}</b></i>",unsafe_allow_html=True)
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
        placeholder.empty()

        # sif st.button("Refresh Data"):
        #     st.cache_data.clear()

        # # fetch_and_process_data.clear()
    elif selected_tab == "Compare Retailers":
      # Initialize user_filters if not present
        if "filters" not in auth_cookie:
            auth_cookie["filters"] = {}
        user_filters = auth_cookie["filters"]

        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            with st.expander("Date Range", expanded=st.session_state["retailer_comparison_expand_filters"]):
                active_data['Period'] = pd.to_datetime(active_data['Period'])
                min_date = active_data['Period'].min().date()
                max_date = active_data['Period'].max().date()
                default_start_date = max(min_date, (active_data['Period'].max() - pd.DateOffset(months=12)).date())

                # Get current date range from cookie
                current_start_month = user_filters.get("compare_start_month", default_start_date.month)
                current_start_year = user_filters.get("compare_start_year", default_start_date.year)
                current_end_month = user_filters.get("compare_end_month", max_date.month)
                current_end_year = user_filters.get("compare_end_year", max_date.year)

                min_year, max_year = min_date.year, max_date.year
                all_months = list(range(1, 13))
                all_years = list(range(min_year, max_year + 1))

                # Ensure current years are within valid range
                if current_start_year not in all_years:
                    current_start_year = default_start_date.year
                if current_end_year not in all_years:
                    current_end_year = max_date.year

                sm_col, sy_col = st.columns([1.3, 1])
                with sm_col:
                    if current_start_year == min_year:
                        valid_start_months = list(range(min_date.month, 13))
                    elif current_start_year == max_year:
                        valid_start_months = list(range(1, max_date.month + 1))
                    else:
                        valid_start_months = all_months

                    # Ensure current month is valid
                    if current_start_month not in valid_start_months:
                        current_start_month = valid_start_months[0]

                    selected_start_month = st.selectbox(
                        "Start Month",
                        options=valid_start_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_start_months.index(current_start_month),
                        key="compare_start_month_select"
                    )

                with sy_col:
                    selected_start_year = st.selectbox(
                        "Start Year",
                        options=all_years,
                        index=all_years.index(current_start_year),
                        key="compare_start_year_select"
                    )

                em_col, ey_col = st.columns([1.3, 1])
                valid_end_years = [y for y in all_years if selected_start_year <= y <= max_year]

                # Ensure current end year is valid
                if current_end_year not in valid_end_years:
                    current_end_year = valid_end_years[0] if valid_end_years else max_year

                if selected_start_year == current_end_year:
                    valid_end_months = list(range(selected_start_month, 13))
                elif current_end_year == max_year:
                    valid_end_months = list(range(1, max_date.month + 1))
                else:
                    valid_end_months = all_months

                # Ensure current end month is valid
                if current_end_month not in valid_end_months:
                    current_end_month = valid_end_months[0]

                with em_col:
                    selected_end_month = st.selectbox(
                        "End Month",
                        options=valid_end_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_end_months.index(current_end_month),
                        key="compare_end_month_select"
                    )

                with ey_col:
                    selected_end_year = st.selectbox(
                        "End Year",
                        options=valid_end_years,
                        index=valid_end_years.index(current_end_year),
                        key="compare_end_year_select"
                    )

                # Handle date range changes
                if (selected_start_month != current_start_month or
                    selected_start_year != current_start_year or
                    selected_end_month != current_end_month or
                    selected_end_year != current_end_year):
                    user_filters["compare_start_month"] = selected_start_month
                    user_filters["compare_start_year"] = selected_start_year
                    user_filters["compare_end_month"] = selected_end_month
                    user_filters["compare_end_year"] = selected_end_year
                    cookie_controller.set(
                        "auth_data",
                        json.dumps(auth_cookie),                    # must be a string
                        expires=datetime.utcnow() + timedelta(days=30),
                        path="/",
                        domain=domain,
                    )
                    st.rerun()

                compare_start_date = max(date(selected_start_year, selected_start_month, 1), min_date)
                compare_end_day = calendar.monthrange(selected_end_year, selected_end_month)[1]
                compare_end_date = min(date(selected_end_year, selected_end_month, compare_end_day), max_date)

                selected_date_range = (compare_start_date, compare_end_date)
                start_date, end_date = pd.Timestamp(compare_start_date), pd.Timestamp(compare_end_date)

        data_dr = active_data[(active_data['Period'] >= start_date) & (active_data['Period'] <= end_date)]
        
        # --- Sector Filter ---
        with col2:
            with st.expander("Sector", expanded=st.session_state["retailer_comparison_expand_filters"]):
                sector_names = data_dr["Sector_Coresight"].dropna().unique().tolist()
                
                # Separate "Other" or "Others" from the rest (case-insensitive)
                others_items = [s for s in sector_names if s.lower() in ["other", "others"]]
                regular_items = [s for s in sector_names if s.lower() not in ["other", "others"]]
                
                # Sort regular items alphabetically (case-insensitive)
                regular_items = sorted(regular_items, key=lambda x: x.lower())
                
                # Combine: regular items first, then "Other"/"Others" at the end
                sector_names = regular_items + others_items
                sector_names = ["All"] + sector_names
                
                # Get current selection from cookie
                current_sector = user_filters.get("sector_filter", "All")
                if current_sector not in sector_names:
                    current_sector = "All"
                
                selected_sector_name = st.selectbox("Select Sector Name", sector_names, 
                                                  index=sector_names.index(current_sector),
                                                  key="sector_filter_selectbox")

                # Handle sector change
                if selected_sector_name != current_sector:
                    user_filters["sector_filter"] = selected_sector_name
                    # Reset downstream filters (Retailers and Location)
                    user_filters["selected_parent_names_v2"] = []
                    user_filters["selected_chain_names_v2"] = []
                    user_filters["selected_state_active_v2"] = ["All"]
                    user_filters["selected_msa_active_v2"] = ["All"]
                    user_filters["location_type_retailers_active"] = "MSA"
                    cookie_controller.set(
                        "auth_data",
                        json.dumps(auth_cookie),                    # must be a string
                        expires=datetime.utcnow() + timedelta(days=30),
                        path="/",
                        domain=domain,
                    )
                    st.rerun()

            if selected_sector_name and selected_sector_name != "All":
                data_sector = data_dr[data_dr['Sector_Coresight'] == selected_sector_name]
            else:
                data_sector = data_dr.copy()

        # --- Retailer Multi ---
        with col3:
            with st.expander("Retailers", expanded=st.session_state["retailer_comparison_expand_filters"]):
                if not data_sector.empty:
                    data_sector['ParentName_Coresight'] = data_sector['ParentName_Coresight'].where(
                        pd.notna(data_sector['ParentName_Coresight']), "No Parent Retailer"
                    )
                    parent_names = sorted(data_sector['ParentName_Coresight'].unique().tolist())
                else:
                    parent_names = []

                # Get current parent selection from cookie
                current_parent_selection = user_filters.get("selected_parent_names_v2", [])
                valid_parent_selection = [p for p in current_parent_selection if p in parent_names]
                
                selected_parent_names = st.multiselect(
                    "Select Company",
                    parent_names,
                    default=valid_parent_selection,
                    key="selected_parent_names_v2_multiselect",
                    help="Parent company that owns one or more store banners."
                )

                if "selected_parent_names_v2_multiselect" not in st.session_state:
                    st.session_state["selected_parent_names_v2_multiselect"] = selected_parent_names
                # Handle parent change
                if selected_parent_names != valid_parent_selection:
                    user_filters["selected_parent_names_v2"] = selected_parent_names
                    user_filters["selected_chain_names_v2"] = []  # Reset chains
                    # Reset Location filters
                    user_filters["selected_state_active_v2"] = ["All"]
                    user_filters["selected_msa_active_v2"] = ["All"]
                    cookie_controller.set(
                        "auth_data",
                        json.dumps(auth_cookie),                    # must be a string
                        expires=datetime.utcnow() + timedelta(days=30),
                        path="/",
                        domain=domain,
                    )
                    st.rerun()

                # Case 1: Parents selected → banners for those parents only
                if len(selected_parent_names) >= 1:
                    data_parent = data_sector[data_sector['ParentName_Coresight'].isin(selected_parent_names)]
                else:
                    # Case 2: NO parents chosen → banners for ALL data in this sector
                    data_parent = data_sector.copy()
                    
                st.markdown(f"""<h6 style='text-align: center;'>or</h6>""", unsafe_allow_html=True)
                
                # Always populate banners from the (possibly whole) sector...
                if not data_parent.empty:
                    chain_names = sorted(data_parent['ChainName_Coresight'].dropna().unique().tolist())
                else:
                    chain_names = []
                    
                banner_label = "Select Banner/Brand" if chain_names else "No Banner/Brand Available"
                
                # Get current chain selection from cookie
                current_chain_selection = user_filters.get("selected_chain_names_v2", [])
                valid_chain_selection = [c for c in current_chain_selection if c in chain_names]
                
                selected_chain_names = st.multiselect(
                    banner_label,
                    chain_names,
                    default=valid_chain_selection,
                    key="selected_chain_names_v2_multiselect",
                    help="The specific retail banner or storefront name customers see."
                )
                if "selected_chain_names_v2_multiselect" not in st.session_state:
                    st.session_state["selected_chain_names_v2_multiselect"] = selected_chain_names
                # Handle chain change
                if selected_chain_names != valid_chain_selection:
                    user_filters["selected_chain_names_v2"] = selected_chain_names
                    # Reset Location filters
                    user_filters["selected_state_active_v2"] = ["All"]
                    user_filters["selected_msa_active_v2"] = ["All"]
                    cookie_controller.set(
                        "auth_data",
                        json.dumps(auth_cookie),                    # must be a string
                        expires=datetime.utcnow() + timedelta(days=30),
                        path="/",
                        domain=domain,
                    )
                    st.rerun()

                # Filter by banners if at least one picked; else all (possibly all for sector, or filtered by parent if applies)
                if len(selected_chain_names) >= 1:
                    data_chain = data_parent[data_parent['ChainName_Coresight'].isin(selected_chain_names)]
                else:
                    data_chain = data_parent.copy() if not data_parent.empty else pd.DataFrame(columns=data_parent.columns)

        def mutually_exclusive_all(selected, all_option, valid_options):
            # Drop out-of-options due to upstream filter changes
            selected = [s for s in selected if s == all_option or s in valid_options]
            if all_option in selected and len(selected) > 1:
                # If user clicked 'All' last, keep only 'All'
                if selected[-1] == all_option:
                    return [all_option]
                else:
                    return [s for s in selected if s != all_option]
            elif not selected:
                return [all_option]
            else:
                return selected

        with col4:
        #     with st.expander("Location", expanded=st.session_state["retailer_comparison_expand_filters"]):

        #         # ------- STATE FILTER -------
        #         valid_states = sorted(data_chain['State'].dropna().unique().tolist()) if not data_chain.empty else []
        #         all_option = "All"
        #         state_options = [all_option] + valid_states
        #         has_parent_or_chain = bool(st.session_state.get("selected_parent_names_v2_multiselect", [])) or bool(st.session_state.get("selected_chain_names_v2_multiselect", []))
        #         if not has_parent_or_chain:
        #             st.session_state["selected_state_active_v2"] = ["All"]
        #             st.session_state["selected_msa_active_v2"] = ["All"]
        #         # Get current state selection from cookie
        #         current_state_selection = user_filters.get("selected_state_active_v2", ["All"])
        #         # Validate current selection
        #         current_state_selection = [s for s in current_state_selection if s == all_option or s in valid_states]
        #         if not current_state_selection and valid_states:
        #             current_state_selection = ["All"]
        #         elif not current_state_selection:
        #             current_state_selection = []
        #         # --- Sync session state for State filter ---
                
               

        #         selected_states = st.multiselect(
        #             "Select State",
        #             options=state_options,
        #             default=current_state_selection,
        #             help="U.S. state where the store is located.",
        #             key="selected_state_v2_multiselect",
        #             disabled=not has_parent_or_chain
        #         )
        #         # Ensure "All" is mutually exclusive in States
        #         if "selected_state_v2_multiselect" not in st.session_state:
        #             st.session_state["selected_state_v2_multiselect"] = selected_states
        #         if len(selected_states) > 1 and "All" in selected_states:
        #             if selected_states[-1] == "All":
        #                 selected_states = ["All"]
        #             else:
        #                 selected_states = [s for s in selected_states if s != "All"]
        #         elif not selected_states and valid_states:
        #             selected_states = ["All"]
        #         elif not selected_states:
        #             selected_states = []


        #         # Handle state change - reset Location (MSA/Zip Code)
        #         if selected_states != current_state_selection:
        #             user_filters["selected_state_active_v2"] = selected_states
        #             user_filters["selected_msa_active_v2"] = ["All"]  # Reset MSA/Zip selection
        #             cookie_controller.set(
        #                 "auth_data",
        #                 json.dumps(auth_cookie),                    # must be a string
        #                 expires=datetime.utcnow() + timedelta(days=30),
        #                 path="/",
        #                 domain=domain,
        #             )
        #             st.rerun()
        #         st.markdown("""
        #         <style>
        #         .st-emotion-cache-wfksaw {
        #             gap: 0.2rem !important; /* Reduce the gap to a smaller value */
        #         }
        #         </style>
        #         """, unsafe_allow_html=True)
                
        #         _, center_col, _ = st.columns([20, 80, 10])
        #         with center_col:
        #             # Get current location type from cookie
        #             current_location_type = user_filters.get("location_type_retailers_active", "MSA")
        #             location_type_options = ["MSA", "Zip Code"]
        #             if current_location_type not in location_type_options:
        #                 current_location_type = "MSA"
                    
        #             # selected_location_type = st.radio(
        #             #     "",
        #             #     location_type_options,
        #             #     horizontal=True,
        #             #     index=location_type_options.index(current_location_type),
        #             #     key="location_type_retailers_active_radio",
        #             #     label_visibility="collapsed"
        #             # )
        #             selected_location_type = st.session_state.get("location_type_retailers_active", "MSA")

        #             # Handle location type change
        #             if selected_location_type != current_location_type:
        #                 user_filters["location_type_retailers_active"] = selected_location_type
        #                 cookie_controller.set(
        #                     "auth_data",
        #                     json.dumps(auth_cookie),                    # must be a string
        #                     expires=datetime.utcnow() + timedelta(days=30),
        #                     path="/",
        #                     domain=domain,
        #                 )
        #                 st.rerun()

        #         # ------- LOCATION FILTER (MSA/ZIP CODE) -------
        #         # Filter data based on state selection
        #         if "All" in selected_states:
        #             location_state_filtered = data_chain
        #         else:
        #             location_state_filtered = data_chain[data_chain["State"].isin(selected_states)]

        #         # Get data for MSA/Zip Code based on toggle
        #         if location_state_filtered.empty:
        #             valid_locations = []
        #         else:
        #             if selected_location_type == "MSA":
        #                 valid_locations = sorted(location_state_filtered['MsaName'].dropna().unique().tolist())
        #             # else:  # "Zip Code"
        #             #     valid_locations = sorted(location_state_filtered['PostalCode'].dropna().unique().tolist())
                
        #         location_options = [all_option] + valid_locations

        #         # Get current MSA/Zip selection from cookie
        #         current_location_selection = user_filters.get("selected_msa_active_v2", ["All"])
        #         # Validate current selection - keep only selections that are still valid
        #         current_location_selection = [l for l in current_location_selection if l == all_option or l in valid_locations]
                
        #         # Default to "All" ONLY if there are valid locations AND no current selection
        #         if not current_location_selection and valid_locations:
        #             current_location_selection = ["All"]
        #         elif not current_location_selection:
        #             current_location_selection = []

        #         # Set label based on toggle
        #         location_label = "Select Metropolitan Statistical Area (MSA)" if selected_location_type == "MSA" else "Select Postal Code"

        #         selected_locations = st.multiselect(
        #             location_label,
        #             options=location_options,
        #             default=current_location_selection,
        #             help="A metro region centered on a large city plus its economically linked suburbs." if selected_location_type == "MSA" else "Postal Code where the store is located.",
        #             key="selected_msa_v2_multiselect",
        #             disabled=not has_parent_or_chain
        #         )

        #         # Handle MSA/Zip change - ensure "All" is mutually exclusive
        #         if len(selected_locations) > 1 and "All" in selected_locations:
        #             if selected_locations[-1] == "All":
        #                 selected_locations = ["All"]
        #             else:
        #                 selected_locations = [l for l in selected_locations if l != "All"]
        #         elif not selected_locations and valid_locations:
        #             selected_locations = ["All"]
        #         elif not selected_locations:
        #             selected_locations = []

        #         if selected_locations != current_location_selection:
        #             user_filters["selected_msa_active_v2"] = selected_locations
        #             cookie_controller.set(
        #                 "auth_data",
        #                 json.dumps(auth_cookie),                    # must be a string
        #                 expires=datetime.utcnow() + timedelta(days=30),
        #                 path="/",
        #                 domain=domain,
        #             )
        #             st.rerun()

        #         # ------- FILTER FINAL DATA FOR ANALYSIS -------
        #         # Apply state filter
        #         if "All" in selected_states:
        #             filtered_by_state = data_chain
        #         else:
        #             filtered_by_state = data_chain[data_chain["State"].isin(selected_states)]
                
        #         # Apply MSA/Zip filter
        #         if "All" in selected_locations:
        #             final_filtered_data = filtered_by_state
        #         else:
        #             # Use appropriate column based on toggle
        #             location_column = 'MsaName' if selected_location_type == "MSA" else 'PostalCode'
        #             final_filtered_data = filtered_by_state[filtered_by_state[location_column].isin(selected_locations)]
# --- Location Expander ---
            with st.expander("Location", expanded=st.session_state["retailer_comparison_expand_filters"]):
                # ------- STATE FILTER -------
                valid_states = sorted(data_chain['State'].dropna().unique().tolist()) if not data_chain.empty else []
                all_option = "All"
                state_options = [all_option] + valid_states
                has_parent_or_chain = bool(st.session_state.get("selected_parent_names_v2_multiselect", [])) or bool(st.session_state.get("selected_chain_names_v2_multiselect", []))
                if not has_parent_or_chain:
                    # Reset State and Location selections when no parent/banner is selected
                    st.session_state["selected_state_v2"] = ["All"]
                    st.session_state["selected_msa_v2"] = ["All"]
                    st.session_state["selected_zip_v2"] = ["All"] # Reset Zip as well

                # Get current state selection from session state (fallback to cookie/user_filters if needed)
                current_state_selection = st.session_state.get("selected_state_v2", user_filters.get("selected_state_active_v2", ["All"]))
                # Validate current selection
                current_state_selection = [s for s in current_state_selection if s == all_option or s in valid_states]
                if not current_state_selection and valid_states:
                    current_state_selection = ["All"]
                elif not current_state_selection:
                    current_state_selection = []

                selected_states = st.multiselect(
                    "Select State",
                    options=state_options,
                    default=current_state_selection,
                    help="U.S. state where the store is located.",
                    key="selected_state_v2_multiselect",
                    disabled=not has_parent_or_chain
                )

                # Ensure "All" is mutually exclusive in States
                if len(selected_states) > 1 and all_option in selected_states:
                    if selected_states[-1] == all_option:
                        selected_states = [all_option]
                    else:
                        selected_states = [s for s in selected_states if s != all_option]
                elif not selected_states and valid_states:
                    selected_states = [all_option]
                elif not selected_states:
                    selected_states = []

                # Handle state change - reset MSA and Zip selections
                if selected_states != current_state_selection:
                    st.session_state["selected_state_v2"] = selected_states
                    st.session_state["selected_msa_v2"] = ["All"]  # Reset MSA
                    st.session_state["selected_zip_v2"] = ["All"]  # Reset Zip
                    # Update cookie as well if needed for persistence across sessions
                    user_filters["selected_state_active_v2"] = selected_states
                    user_filters["selected_msa_active_v2"] = ["All"]
                    user_filters["selected_zip_active_v2"] = ["All"] # Add to user_filters too
                    cookie_controller.set(
                        "auth_data",
                        json.dumps(auth_cookie),
                        expires=datetime.utcnow() + timedelta(days=30),
                        path="/",
                        domain=domain,
                    )
                    st.rerun() # Rerun to update downstream filters and data

                # Filter data based on state selection for downstream filters
                if all_option in selected_states:
                    location_state_filtered = data_chain
                else:
                    location_state_filtered = data_chain[data_chain["State"].isin(selected_states)]

                # --- MSA FILTER ---
                # Get valid MSA options based on the state-filtered data
                valid_msa_locations = sorted(location_state_filtered['MsaName'].dropna().unique().tolist()) if not location_state_filtered.empty else []
                msa_location_options = [all_option] + valid_msa_locations

                # Get current MSA selection from session state (fallback to cookie/user_filters if needed)
                current_msa_selection = st.session_state.get("selected_msa_v2", user_filters.get("selected_msa_active_v2", ["All"]))
                # Validate current selection
                current_msa_selection = [l for l in current_msa_selection if l == all_option or l in valid_msa_locations]
                if not current_msa_selection and valid_msa_locations:
                    current_msa_selection = ["All"]
                elif not current_msa_selection:
                    current_msa_selection = []

                selected_msa_locations = st.multiselect(
                    "Select Metropolitan Statistical Area (MSA)",
                    options=msa_location_options,
                    default=current_msa_selection,
                    help="A metro region centered on a large city plus its economically linked suburbs.",
                    key="selected_msa_v2_multiselect",
                    disabled=not has_parent_or_chain
                )

                # Ensure "All" is mutually exclusive in MSA
                if len(selected_msa_locations) > 1 and all_option in selected_msa_locations:
                    if selected_msa_locations[-1] == all_option:
                        selected_msa_locations = [all_option]
                    else:
                        selected_msa_locations = [l for l in selected_msa_locations if l != all_option]
                elif not selected_msa_locations and valid_msa_locations:
                    selected_msa_locations = [all_option]
                elif not selected_msa_locations:
                    selected_msa_locations = []

                if selected_msa_locations != current_msa_selection:
                    st.session_state["selected_msa_v2"] = selected_msa_locations
                    # Update cookie as well
                    user_filters["selected_msa_active_v2"] = selected_msa_locations
                    user_filters["selected_zip_active_v2"] = ["All"]
                    st.session_state["selected_zip_v2"] = ["All"]
                    cookie_controller.set(
                        "auth_data",
                        json.dumps(auth_cookie),
                        expires=datetime.utcnow() + timedelta(days=30),
                        path="/",
                        domain=domain,
                    )
                    st.rerun()
                    # No need to rerun here unless MSA selection itself affects other parts immediately
                    # The final data filter below will use the new value.

                # --- ZIP CODE FILTER (NEW) ---
                # Get valid Zip Code options based on the state-filtered data
                # valid_zip_locations = sorted(location_state_filtered['PostalCode'].dropna().unique().tolist()) if not location_state_filtered.empty else []
                # zip_location_options = [all_option] + valid_zip_locations
                if "All" in selected_msa_locations:
                    location_state_msa_filtered = location_state_filtered
                else:
                    location_state_msa_filtered = location_state_filtered[location_state_filtered["MsaName"].isin(selected_msa_locations)]

                # --- ZIP CODE FILTER (NEW) ---
                # Get valid Zip Code options based on the state AND MSA filtered data
                valid_zip_locations = sorted(location_state_msa_filtered['PostalCode'].dropna().unique().tolist()) if not location_state_msa_filtered.empty else []
                zip_location_options = [all_option] + valid_zip_locations


                # Get current Zip selection from session state (fallback to cookie/user_filters if needed)
                current_zip_selection = st.session_state.get("selected_zip_v2", user_filters.get("selected_zip_active_v2", ["All"])) # Fallback to user_filters if session_state not set
                # Validate current selection
                current_zip_selection = [l for l in current_zip_selection if l == all_option or l in valid_zip_locations]
                if not current_zip_selection and valid_zip_locations:
                    current_zip_selection = ["All"]
                elif not current_zip_selection:
                    current_zip_selection = []

                selected_zip_locations = st.multiselect(
                    "Select Zip Code", # Label for the new filter
                    options=zip_location_options,
                    default=current_zip_selection,
                    help="U.S. postal area where the store is located.",
                    key="selected_zip_v2_multiselect", # Unique key
                    disabled=not has_parent_or_chain
                )

                # Ensure "All" is mutually exclusive in Zip Codes
                if len(selected_zip_locations) > 1 and all_option in selected_zip_locations:
                    if selected_zip_locations[-1] == all_option:
                        selected_zip_locations = [all_option]
                    else:
                        selected_zip_locations = [l for l in selected_zip_locations if l != all_option]
                elif not selected_zip_locations and valid_zip_locations:
                    selected_zip_locations = [all_option]
                elif not selected_zip_locations:
                    selected_zip_locations = []

                if selected_zip_locations != current_zip_selection:
                    st.session_state["selected_zip_v2"] = selected_zip_locations
                    # Update cookie as well
                    user_filters["selected_zip_active_v2"] = selected_zip_locations # Add to user_filters
                    cookie_controller.set(
                        "auth_data",
                        json.dumps(auth_cookie),
                        expires=datetime.utcnow() + timedelta(days=30),
                        path="/",
                        domain=domain,
                    )
                    st.rerun()
                    # No need to rerun here unless Zip selection itself affects other parts immediately
                    # The final data filter below will use the new value.

                # ------- FILTER FINAL DATA FOR ANALYSIS -------
                # Apply state filter first
                if all_option in selected_states:
                    filtered_by_state = data_chain
                else:
                    filtered_by_state = data_chain[data_chain["State"].isin(selected_states)]

        # Determine which location filter to apply and apply it
        # If "All" is selected in both MSA and Zip, or neither has a specific selection, show all data for the state
        # If MSA is selected (and not "All"), filter by MSA
        # If Zip is selected (and not "All"), filter by Zip
        # If both MSA and Zip are selected (and not "All"), filter by Zip (you can change this logic if needed)
        # The logic assumes that if a user has selected a specific zip, that's the primary filter, even if MSA is also selected.
        # If both have specific selections, you might want to intersect, but typically one takes precedence or both are allowed (union).
        # Let's assume if a specific Zip is selected, it takes precedence over MSA, or if only MSA is selected, use MSA.
        # If both are specific selections, apply Zip filter.
        # If both are "All", apply no location filter beyond state.

        # Check if MSA filter is active (specific selection made)
        msa_filter_active = (len(selected_msa_locations) > 1 or (len(selected_msa_locations) == 1 and selected_msa_locations[0] != all_option))
        # Check if Zip filter is active (specific selection made)
        zip_filter_active = (len(selected_zip_locations) > 1 or (len(selected_zip_locations) == 1 and selected_zip_locations[0] != all_option))

        if zip_filter_active:
            # Apply Zip Code filter
            final_filtered_data = filtered_by_state[filtered_by_state['PostalCode'].isin(selected_zip_locations)]
        elif msa_filter_active:
            # Apply MSA filter
            final_filtered_data = filtered_by_state[filtered_by_state['MsaName'].isin(selected_msa_locations)]
        else:
            # If neither MSA nor Zip has specific selections ("All" or empty), use state-filtered data
            final_filtered_data = filtered_by_state

        # Now 'final_filtered_data' incorporates State, and either MSA or Zip Code filter, or neither beyond state.
        # The rest of the code (graphs, tables) uses 'final_filtered_data'.

        # --> Now use final_filtered_data everywhere in the rest of your dashboard

        # Only show graphs/tables if at least 1 sector and at least 2 retailers are chosen:
        if (selected_sector_name and not final_filtered_data.empty and (len(selected_parent_names) >= 2 or len(selected_chain_names) >= 1)):

            ## [ ALERT: Color logic/metrics code from previous answer goes here, unchanged ]
            # ... Place the color groups, line chart, bar chart, metrics, banner chart, and table code HERE as before ...

            color_groups = final_filtered_data[['ParentName_Coresight', 'ChainName_Coresight']].drop_duplicates()
            group_labels = (
                color_groups['ParentName_Coresight'] + " | " + color_groups['ChainName_Coresight']
            ).tolist()
            palette = px.colors.qualitative.Plotly
            while len(palette) < len(group_labels):  # Loop through palette if not enough colors
                palette += palette
            color_map = {lab: palette[i] for i, lab in enumerate(group_labels)}

            mcol1, mcol2, mcol3 = st.columns([1, 1, 1])
            with mcol1:
                st.markdown(f"""
                    <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{final_filtered_data['ChainName_Coresight'].count():,}</h1>
                    <h6 style='text-align: left; margin-top: 0px;'>Total Active Stores</h6>
                """, unsafe_allow_html=True)
            with mcol2:
                st.markdown(f"""
                    <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{final_filtered_data['ParentName_Coresight'].nunique()}</h1>
                    <h6 style='text-align: left; margin-top: 0px;'>Total Affected Retailers</h6>
                """, unsafe_allow_html=True)
            with mcol3:
                st.markdown(f"""
                    <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{final_filtered_data['Sector_Coresight'].nunique()}</h1>
                    <h6 style='text-align: left; margin-top: 0px;'>Total Affected Sectors</h6>
                """, unsafe_allow_html=True)

            # --- LINE CHART ---
            gcol1, gcol2 = st.columns([4,2])
            with gcol1:
                st.markdown("<h4 style='font-size: 20px;text-align: center;'>Active Stores Over Time<br></h4>", unsafe_allow_html=True)
                
                # Prepare the data
                time_grouped = (
                    final_filtered_data.groupby(['Period', 'ParentName_Coresight', 'ChainName_Coresight'])
                    .size().reset_index(name='ActiveStores')
                )
                
                # Create a mapping of period to metadata for each chain
                chain_metadata = {}
                
                for (parent, chain), grp_df in time_grouped.groupby(["ParentName_Coresight", "ChainName_Coresight"]):
                    chain_key = f"{parent} | {chain}"
                    chain_metadata[chain_key] = {}
                    
                    for period in grp_df['Period']:
                        # Get the data for this specific chain and period
                        period_data = final_filtered_data[
                            (final_filtered_data['ParentName_Coresight'] == parent) &
                            (final_filtered_data['ChainName_Coresight'] == chain) &
                            (final_filtered_data['Period'] == period)
                        ]
                        
                        # Calculate metadata for this specific data point
                        num_sectors = period_data['Sector_Coresight'].nunique()
                        num_banners = period_data['ChainName_Coresight'].nunique()
                        num_states = period_data['State'].nunique()
                        num_msa = period_data['MsaName'].nunique()
                        
                        chain_metadata[chain_key][period] = {
                            'sectors': num_sectors,
                            'banners': num_banners,
                            'states': num_states,
                            'msa': num_msa
                        }
                
                # Create figure with consistent styling
                fig_line = go.Figure()
                
                for (parent, chain), grp_df in time_grouped.groupby(["ParentName_Coresight", "ChainName_Coresight"]):
                    legend_grp = chain
                    color = color_map.get(f"{parent} | {chain}", "#CBCACA")
                    chain_key = f"{parent} | {chain}"
                    
                    # Prepare customdata for this chain
                    customdata_list = []
                    for period in grp_df['Period']:
                        metadata = chain_metadata[chain_key].get(period, {'sectors': 0, 'banners': 0, 'states': 0, 'msa': 0})
                        customdata_list.append([
                            metadata['sectors'],
                            metadata['banners'],
                            metadata['states'],
                            metadata['msa']
                        ])
                    
                    fig_line.add_trace(go.Scatter(
                        x=grp_df['Period'],
                        y=grp_df['ActiveStores'],
                        mode='lines+markers+text',
                        name=legend_grp,
                        line=dict(color=color, width=2),
                        marker=dict(size=7, color=color),
                        text=[f"{x:,}" for x in grp_df['ActiveStores']],
                        textposition='top center',
                        hovertemplate=(
                            f"<b>{chain}</b><br>"
                            "<span style='color:" + color + "'>●</span> "
                            "<b>Active Store Count:</b> <b>%{y:,}</b><br><br>"
                            # "<span style='color:" + color + "'>•</span> "
                            # "<span style='color:black'>Date Period: %{x|%b %Y}</span><br>"
                            "<span style='color:" + color + "'>•</span> "
                            "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                            "<span style='color:" + color + "'>•</span> "
                            "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                            "<span style='color:" + color + "'>•</span> "
                            "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                            "<span style='color:" + color + "'>•</span> "
                            "<span style='color:black'>Number of MSA: %{customdata[3]}</span><br>"
                            "<extra></extra>"
                        ),
                        customdata=customdata_list,
                        hoverlabel=dict(
                            bgcolor="white",
                            bordercolor=color,
                            font=dict(size=13, color="black", family="Arial")
                        )
                    ))

                # Update layout with consistent styling
                fig_line.update_layout(
                    yaxis_title="Active Stores",
                    xaxis_title="Period",
                    height=400,
                    showlegend=True,
                    legend=dict(
                        x=0.99, y=0.99,
                        xanchor="right",
                        yanchor="top",
                        bgcolor='rgba(0,0,0,0)',
                        bordercolor='rgba(0,0,0,0)',
                        font=dict(size=13),
                        orientation="v"
                    ),
                    margin=dict(l=10, r=10, t=60, b=40),
                    xaxis=dict(
                        showline=True,
                        zeroline=False,
                        tickformat="%b %Y"
                    ),
                    yaxis=dict(
                        showline=True,
                        zeroline=False,
                        automargin=True
                    ),
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )

                # Config with updated height
                config = {
                    'displayModeBar': True,
                    'displaylogo': False,
                    'toImageButtonOptions': {
                        'format': 'png',
                        'filename': 'active_stores_by_chain_over_time',
                        'height': 400,
                        'width': 700,
                        'scale': 1
                    }
                }

                st.plotly_chart(fig_line, use_container_width=True, config=config)
            with gcol2:
                st.markdown("<h4 style='font-size: 20px; text-align: center;'>Active Stores Counts</h4>", unsafe_allow_html=True)
                
                # Prepare the data
                bar_grouped = (
                    final_filtered_data.groupby(['ParentName_Coresight', 'ChainName_Coresight'])
                    .size().reset_index(name='ActiveStores')  # Changed from ClosedStores to ActiveStores
                )
                bar_grouped['GroupLabel'] = bar_grouped['ParentName_Coresight'] + " | " + bar_grouped["ChainName_Coresight"]

                # Create figure with consistent styling
                fig_bar = go.Figure()
                
                for _, row in bar_grouped.iterrows():
                    banner = row['ChainName_Coresight']
                    parent = row['ParentName_Coresight']
                    color = color_map.get(row['GroupLabel'], "#CBCACA")  # Gray fallback
                    
                    fig_bar.add_trace(go.Bar(
                        x=[banner],
                        y=[row['ActiveStores']],
                        name=parent,  # Parent company for grouping
                        marker_color=color,
                        text=[f"{row['ActiveStores']:,}"],  # Formatted with thousands separator
                        textposition='outside',
                        hovertemplate=(
                            f"<b>{parent}</b><br>"  # Parent company
                            f"<b>{banner}</b><br>"  # Banner name
                            "<span style='color:" + color + "'>●</span> "
                            "<b>Active Stores:</b> %{y:,}<extra></extra>"
                        ),
                        hoverlabel=dict(
                            bgcolor="white",
                            bordercolor=color,
                            font=dict(size=13, color="black", family="Arial")
                        )
                    ))

                # Calculate dynamic height based on number of banners
                dynamic_height = 350 + (20 * (len(bar_grouped) - 10)) if len(bar_grouped) > 10 else 350

                fig_bar.update_layout(
                    xaxis_title="Banner",
                    yaxis_title="Active Stores",
                    showlegend=False,
                    height=dynamic_height,
                    margin=dict(l=10, r=10, t=60, b=40 + (10 * len(bar_grouped))),  # Dynamic margins
                    xaxis=dict(
                        showline=True,
                        zeroline=False,
                        tickangle=45 if len(bar_grouped) > 5 else 0,
                        tickfont=dict(size=11)  # Smaller font for long names
                    ),
                    yaxis=dict(
                        showline=True,
                        zeroline=False,
                        showticklabels=False  # Cleaner look without y-axis numbers
                    ),
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )

                # Adjust for many banners
                if len(bar_grouped) > 8:
                    fig_bar.update_layout(
                        xaxis=dict(tickangle=60),
                        margin=dict(b=60 + (10 * len(bar_grouped)))  # Extra bottom margin
                    )

                st.plotly_chart(fig_bar, use_container_width=True)

            st.markdown("---")
            st.markdown("<h4 style='font-size: 20px; text-align: left;'>Active Stores by Retailer</h4>", unsafe_allow_html=True)
            retailer_agg = final_filtered_data.groupby(['ParentName_Coresight']).size().reset_index(name='ActiveStores')

            # Generate shades of the approved color (#d62e2f)
            def generate_shades(base_hex, num_shades):
                base_rgb = [int(base_hex[i:i+2], 16)/255. for i in (1, 3, 5)]
                h, l, s = colorsys.rgb_to_hls(*base_rgb)
                # Shades: make some lighter, some darker
                l_values = [min(0.9, max(0.2, l * (0.80 + i*0.20/(max(num_shades-1,1))))) for i in range(num_shades)]
                return [f'#{int(c[0]*255):02x}{int(c[1]*255):02x}{int(c[2]*255):02x}' for c in [colorsys.hls_to_rgb(h, lval, s) for lval in l_values]]

            n = len(retailer_agg)
            my_palette = generate_shades("#CBCACA", n)
            color_map_retailer = {name: my_palette[i] for i, name in enumerate(retailer_agg['ParentName_Coresight'])}

            # Create figure with consistent styling
            fig_retailer = go.Figure()

            for _, row in retailer_agg.iterrows():
                retailer = row['ParentName_Coresight']
                color = color_map_retailer.get(retailer, "#CBCACA")  # Gray fallback for active stores
                
                fig_retailer.add_trace(go.Bar(
                    x=[retailer],
                    y=[row['ActiveStores']],  # Changed from ClosedStores to ActiveStores
                    name=retailer,
                    marker_color=color,
                    text=[f"{row['ActiveStores']:,}"],  # Formatted with thousands separator
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"  # Retailer name
                        "<span style='color:" + color + "'>●</span> "
                        "<b>Active Stores:</b> %{y:,}<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor=color,
                        font=dict(size=13, color="black", family="Arial")
                    )
                ))

            # Calculate dynamic height based on number of retailers
            dynamic_height = 400 + (20 * (len(retailer_agg) - 10)) if len(retailer_agg) > 10 else 400

            fig_retailer.update_layout(
                xaxis_title="Retailer",
                yaxis_title="Active Stores",
                showlegend=False,
                height=dynamic_height,
                margin=dict(l=10, r=10, t=60, b=40 + (10 * len(retailer_agg))),
                xaxis=dict(
                    showline=True,
                    zeroline=False,
                    tickangle=45 if len(retailer_agg) > 5 else 0,
                    tickfont=dict(size=11)  # Smaller font for long retailer names
                ),
                yaxis=dict(
                    showline=True,
                    zeroline=False,
                    showticklabels=False  # Cleaner look without y-axis numbers
                ),
                uniformtext_minsize=8,
                uniformtext_mode='hide'
            )

            # Adjust for many retailers
            if len(retailer_agg) > 8:
                fig_retailer.update_layout(
                    xaxis=dict(tickangle=60),
                    margin=dict(b=60 + (10 * len(retailer_agg)))
                )

            st.plotly_chart(fig_retailer, use_container_width=True)

            # --- Table ---
            final_filtered_data['Closing Month/Year'] = final_filtered_data['Period'].dt.strftime('%b-%Y')
            filtered_active_compare = final_filtered_data[['ChainName_Coresight', 'ParentName_Coresight', 'Address',
                                                        'Address2', 'City', 'MsaName', 'PostalCode', 'State', 'Country',
                                                        'Sector_Coresight', 'Closing Month/Year']]
            filtered_active_compare = filtered_active_compare.rename(columns={
                'ChainName_Coresight': 'Banner/Brand Name',
                'ParentName_Coresight': 'Company Name',
                'Address': 'Address Line 1',
                'Address2': 'Address Line 2',
                'City': 'City',
                'MsaName': 'MSA',
                'PostalCode': 'Postal Code',
                'State': 'State',
                'Country': 'Country',
                'Sector_Coresight': 'Sector',
                'Closing Month/Year': 'Active Date'
            })
            col1, col4  = st.columns([7, 1.05])
            with col1:
                st.subheader('Active Stores Table')

            with col4:
                if not filtered_active_compare.empty:
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        filtered_active_compare.to_excel(writer, index=False, sheet_name='Active_Stores')
                        worksheet = writer.sheets['Active_Stores']
                        for i, col in enumerate(filtered_active_compare.columns):
                            column_len = max(
                                filtered_active_compare[col].astype(str).map(len).max(),
                                len(col)
                            )
                            worksheet.set_column(i, i, column_len + 2)
                    formatted_start_date = start_date.strftime("%Y-%m-%d")
                    formatted_end_date = end_date.strftime("%Y-%m-%d")
                    final_filename = f"Active_Stores_{formatted_start_date}_to_{formatted_end_date}.xlsx"
                    
                    st.markdown("""
                        <style>
                            .stDownloadButton>button {
                                background-color: #CBCACA;
                                color: white;
                                border: none;
                            }
                            .stDownloadButton>button:hover {
                                border: 2px solid #CBCACA;
                                background-color: white;
                                color: #CBCACA;
                            }
                        </style>
                    """, unsafe_allow_html=True)

                    # Your download button
                    st.download_button(
                        label="Download Data",
                        data=excel_buffer, # This buffer now contains the autofitted Excel file
                        file_name=final_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_Active_xlsx",
                        use_container_width=True
                    )
                    st.markdown('</div>', unsafe_allow_html=True)
            # AgGrid(filtered_closed_compare, height=600) # You can adjust the height as needed
            GRID_H = 600  # must match your AgGrid height

            # Always show real data in the grid
            df_for_grid = filtered_active_compare

            # Lock icon
            ICON = pathlib.Path(__file__).resolve().parent.parent / "assets" / "icons" / "lock.png"
            lock_b64 = base64.b64encode(ICON.read_bytes()).decode("utf-8")

            gb = GridOptionsBuilder.from_dataframe(df_for_grid)
            grid_options = gb.build()

            # Render the grid
            AgGrid(
                df_for_grid,
                gridOptions=grid_options,
                height=GRID_H,
                key="active_grid",
            )

            # Overlay (only for trial users)
            if is_free_trial:
                st.markdown(f"""
                <style>
                  /* Overlay panel covers the grid completely */
                  .grid-overlay-flag[data-target="active"] {{
                    position: relative;
                    height: {GRID_H}px;
                    margin-top: -{GRID_H}px;   /* pull on top of grid */
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    flex-direction: column;
                    gap: 20px;
                    background: rgba(255,255,255,0.55);  /* translucent white wash */
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

                <div class="grid-overlay-flag" data-target="active">
                  <img src="data:image/png;base64,{lock_b64}" class="lock" alt="Locked"/>
                  <div class="title">For SIP Members Only</div>
                  <p style="margin:8px 0 18px 0;">
                    Individual Store data is exclusively accessible to SIP members and is not available for trial users.
                  </p>
                  <a class="cta" href="https://coresight.com/contact/" target="_blank">Contact Us</a>
                </div>
                """, unsafe_allow_html=True)

        else:
            st.info("Please select at least two retailers or two banners to start the comparison.")

        st.markdown("---")  # Horizontal line for separation
        try:
            # Get the current URL to determine environment
            current_url = st.context.headers.get("host", "")
            is_staging = "stage" in current_url.lower()
        except:
            # Fallback method
            is_staging = False

        # Set the appropriate URL based on environment
        if is_staging:
            overview_url = "https://stage3.coresight.com/store-intelligence-platform-overview/"
        else:
            overview_url = "https://www.coresight.com/store-intelligence-platform-overview/"

        st.markdown(f"<i>Data available through <b>{latest_ts}</b></i>",unsafe_allow_html=True)
        st.markdown(
            f"""
            <p style='color: gray; font-size: small;'>
            Disclaimer: Certain data are derived from calculations that use data licensed from third parties, including ChainXY. 
            Coresight Research has made substantial efforts to clean the data and identify potential issues. However, changes to retailers' store locators 
            may impact database-sourced data. See our 
            <a href="{overview_url}" target="_blank">Overview</a> 
            document and <a href="/change-logs#store-intelligence-platform" target="_self">Data Release Notes</a> 
            for more details.
            </p>
            """,
            unsafe_allow_html=True,
        )

        placeholder.empty()

    else:
        # ---- ROBUST SYNC QUERY PARAMS ONCE, ON FIRST LOAD ----
        def ensure_list(val):
            if val is None:
                return []
            if isinstance(val, list):
                return val
            else:
                return [val]

        if "sector_compare_synced" not in st.session_state:
            # Set from cookies
            st.session_state["sector_comparison_selected_sectors"] = ensure_list(auth_cookie.get("compare_sectors", []))
            st.session_state["sector_compare_parent_chain_name"] = ensure_list(auth_cookie.get("parent_chain", []))
            st.session_state["sector_compare_selected_chain_name"] = ensure_list(auth_cookie.get("chain", []))
            st.session_state["sector_compare_selected_state_name"] = ensure_list(auth_cookie.get("state", []))
            st.session_state["sector_compare_selected_msa_name"] = ensure_list(auth_cookie.get("msa", []))
            st.session_state["location_type_sector_active"] = auth_cookie.get("location_type_sector", "MSA")
            st.session_state["sector_compare_synced"] = True
            # Never set again in your code, only allow user to override through UI

        def update_cookies():
            user_filters["compare_sectors"] = st.session_state["sector_comparison_selected_sectors"]
            user_filters["parent_chain"] = st.session_state.get("sector_compare_parent_chain_name", [])
            user_filters["chain"] = st.session_state.get("sector_compare_selected_chain_name", [])
            user_filters["state"] = st.session_state.get("sector_compare_selected_state_name", [])
            user_filters["msa"] = st.session_state.get("sector_compare_selected_msa_name", [])
            user_filters["location_type_sector"] = st.session_state.get("location_type_sector_active", "MSA")
            cookie_controller.set(
                "auth_data",
                json.dumps(auth_cookie),                    # must be a string
                expires=datetime.utcnow() + timedelta(days=30),
                path="/",
                domain=domain,
            )

        # Layout for the filters
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            with st.expander("Date Range", expanded=st.session_state["sector_comparison_expand_filters"]):
                logging.info("Initializing date range filter")
                active_data['Period'] = pd.to_datetime(active_data['Period'])
                min_date = active_data['Period'].min().date()
                max_date = active_data['Period'].max().date()
                default_start_date = max(min_date, (active_data['Period'].max() - pd.DateOffset(months=12)).date())
                
                logging.info(f"Date range in data: {min_date} to {max_date}")
                logging.info(f"Default start date: {default_start_date}")

                st.session_state.setdefault("sector_compare_start_month", default_start_date.month)
                st.session_state.setdefault("sector_compare_start_year", default_start_date.year)
                st.session_state.setdefault("sector_compare_end_month", max_date.month)
                st.session_state.setdefault("sector_compare_end_year", max_date.year)
                
                logging.info(f"Initial date state: Start {st.session_state['sector_compare_start_month']}/{st.session_state['sector_compare_start_year']}, End {st.session_state['sector_compare_end_month']}/{st.session_state['sector_compare_end_year']}")

                min_year, max_year = min_date.year, max_date.year
                all_months = list(range(1, 13))
                all_years = list(range(min_year, max_year + 1))

                sm_col, sy_col = st.columns([1.3, 1])
                with sm_col:
                    if st.session_state["sector_compare_start_year"] == min_year:
                        valid_start_months = list(range(min_date.month, 13))
                    elif st.session_state["sector_compare_start_year"] == max_year:
                        valid_start_months = list(range(1, max_date.month + 1))
                    else:
                        valid_start_months = all_months

                    st.selectbox(
                        "Start Month",
                        options=valid_start_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_start_months.index(st.session_state["sector_compare_start_month"]),
                        key="sector_compare_start_month_select"
                    )

                with sy_col:
                    st.selectbox(
                        "Start Year",
                        options=all_years,
                        index=all_years.index(st.session_state["sector_compare_start_year"]),
                        key="sector_compare_start_year_select"
                    )

                st.session_state["sector_compare_start_month"] = st.session_state["sector_compare_start_month_select"]
                st.session_state["sector_compare_start_year"] = st.session_state["sector_compare_start_year_select"]

                em_col, ey_col = st.columns([1.3, 1])
                valid_end_years = [y for y in all_years if st.session_state["sector_compare_start_year"] <= y <= max_year]

                if st.session_state["sector_compare_end_year"] not in valid_end_years:
                    st.session_state["sector_compare_end_year"] = valid_end_years[0]

                if st.session_state["sector_compare_end_year"] == st.session_state["sector_compare_start_year"]:
                    valid_end_months = list(range(st.session_state["sector_compare_start_month"], 13))
                elif st.session_state["sector_compare_end_year"] == max_year:
                    valid_end_months = list(range(1, max_date.month + 1))
                else:
                    valid_end_months = all_months

                if st.session_state["sector_compare_end_month"] not in valid_end_months:
                    st.session_state["sector_compare_end_month"] = valid_end_months[0]

                with em_col:
                    st.selectbox(
                        "End Month",
                        options=valid_end_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_end_months.index(st.session_state["sector_compare_end_month"]),
                        key="sector_compare_end_month_select"
                    )

                with ey_col:
                    st.selectbox(
                        "End Year",
                        options=valid_end_years,
                        index=valid_end_years.index(st.session_state["sector_compare_end_year"]),
                        key="sector_compare_end_year_select"
                    )

                st.session_state["sector_compare_end_month"] = st.session_state["sector_compare_end_month_select"]
                st.session_state["sector_compare_end_year"] = st.session_state["sector_compare_end_year_select"]

                compare_start_date = max(date(st.session_state["sector_compare_start_year"], st.session_state["sector_compare_start_month"], 1), min_date)
                compare_end_day = calendar.monthrange(st.session_state["sector_compare_end_year"], st.session_state["sector_compare_end_month"])[1]
                compare_end_date = min(date(st.session_state["sector_compare_end_year"], st.session_state["sector_compare_end_month"], compare_end_day), max_date)

                st.session_state["sector_compare_date_range"] = (compare_start_date, compare_end_date)
                logging.info(f"Final date range selected: {compare_start_date} to {compare_end_date}")
                update_cookies()

        with col2:
            with st.expander("Sector Comparison", expanded=st.session_state["sector_comparison_expand_filters"]):
                logging.info("Initializing sector comparison filter")
                start_date, end_date = st.session_state["sector_compare_date_range"]
                logging.info(f"Filtering data for date range: {start_date} to {end_date}")
                
                filtered_sector_data = active_data[
                    (active_data['Period'] >= pd.Timestamp(start_date)) & 
                    (active_data['Period'] <= pd.Timestamp(end_date))
                ]
                
                # Get unique sectors and apply the same sorting logic
                all_sectors = filtered_sector_data["Sector_Coresight"].dropna().unique().tolist()
                others_items = [s for s in all_sectors if s.lower() in ["other", "others"]]
                regular_items = [s for s in all_sectors if s.lower() not in ["other", "others"]]
                regular_items = sorted(regular_items, key=lambda x: x.lower())
                sectors = regular_items + others_items
                
                logging.info(f"Available sectors in date range: {sectors}")
                
                if st.session_state["sector_comparison_selected_sectors"]:
                    valid_selected_sectors = [
                        s for s in st.session_state["sector_comparison_selected_sectors"]
                        if (not sectors or s in sectors)
                    ]
                    # If nothing matches, fallback to old selection
                    if not valid_selected_sectors:
                        valid_selected_sectors = st.session_state["sector_comparison_selected_sectors"]
                else:
                    valid_selected_sectors = []

                selected_sectors = st.multiselect(
                    "Select Sectors to Compare (minimum 2)",
                    options=sectors,
                    default=valid_selected_sectors,
                    key="sector_multiselect"
                )
                
                if selected_sectors != st.session_state["sector_comparison_selected_sectors"]:
                    logging.info(f"Sector selection changed from {st.session_state['sector_comparison_selected_sectors']} to {selected_sectors}")
                    st.session_state["sector_comparison_selected_sectors"] = selected_sectors
                    # Reset downstream filters when sector comparison changes
                    st.session_state["sector_compare_parent_chain_name"] = []
                    st.session_state["sector_compare_selected_chain_name"] = []
                    st.session_state["sector_compare_selected_state_name"] = ["All"]
                    st.session_state["sector_compare_selected_msa_name"] = ["All"]
                    st.session_state["sector_compare_selected_zip"] = ["All"]
                    update_cookies()
                    st.rerun()

        with col3:
            with st.expander("Retailers", expanded=st.session_state["sector_comparison_expand_filters"]):
                logging.info("Initializing retailers filter")
                parent_names = []
                chain_names = []
                
                if st.session_state["sector_comparison_selected_sectors"]:
                    selected_sectors = st.session_state["sector_comparison_selected_sectors"]
                    if isinstance(selected_sectors, str):
                        selected_sectors = [selected_sectors]
                        
                    logging.info(f"Processing retailers for sectors: {selected_sectors}")
                    start_date, end_date = st.session_state["sector_compare_date_range"]
                    
                    temp_data = active_data[
                        (active_data['Period'] >= pd.Timestamp(start_date)) &
                        (active_data['Period'] <= pd.Timestamp(end_date)) &
                        (active_data["Sector_Coresight"].isin(selected_sectors))
                    ]
                    logging.info(f"Found {len(temp_data)} records for selected sectors")
                    
                    temp_data['ParentName_Coresight'] = temp_data['ParentName_Coresight'].where(
                        pd.notna(temp_data['ParentName_Coresight']), "No Parent Retailer"
                    )
                    parent_names = sorted(temp_data['ParentName_Coresight'].unique().tolist())
                    if "No Parent Retailer" in parent_names:
                        parent_names.remove("No Parent Retailer")
                        parent_names.append("No Parent Retailer")
                    
                    logging.info(f"Available parent retailers: {parent_names}")
                    
                    current_parent_selection = st.session_state.get("sector_compare_parent_chain_name", [])
                    valid_parent_selection = [p for p in current_parent_selection if p in parent_names]
                    logging.info(f"Current parent selection (valid): {valid_parent_selection}")
                    
                    parent_chain_selection = st.multiselect(
                        "Select Company", 
                        options=parent_names,
                        default=valid_parent_selection,
                        help="Parent company that owns one or more store banners.",
                        key="sector_compare_parent_chain_name_select",
                        disabled=not st.session_state["sector_comparison_selected_sectors"]
                    )
                    
                    if parent_chain_selection != valid_parent_selection:
                        logging.info(f"Parent selection changed from {valid_parent_selection} to {parent_chain_selection}")
                        st.session_state["sector_compare_parent_chain_name"] = parent_chain_selection
                        st.session_state["sector_compare_selected_chain_name"] = []
                        # Reset location filters when retailers change
                        st.session_state["sector_compare_selected_state_name"] = ["All"]
                        st.session_state["sector_compare_selected_msa_name"] = ["All"]
                        st.session_state["sector_compare_selected_zip"] = ["All"]
                        update_cookies()
                        st.rerun()
                    
                    if parent_chain_selection:
                        filtered_data = temp_data[temp_data["ParentName_Coresight"].isin(parent_chain_selection)]
                        chain_names = sorted(filtered_data['ChainName_Coresight'].dropna().unique().tolist())
                    else:
                        chain_names = sorted(temp_data['ChainName_Coresight'].dropna().unique().tolist())
                    
                    logging.info(f"Available chains: {chain_names}")
                    st.markdown(f"""<h6 style='text-align: center; margin-top: -10px; margin-bottom: -100px;'>or</h6>""", unsafe_allow_html=True)
                    current_chain_selection = st.session_state.get("sector_compare_selected_chain_name", [])
                    valid_chain_selection = [c for c in current_chain_selection if c in chain_names]
                    logging.info(f"Current chain selection (valid): {valid_chain_selection}")
                    
                    chain_selection = st.multiselect(
                        "Select Banner/Brand", 
                        options=chain_names,
                        default=valid_chain_selection,
                        help="The specific retail banner or storefront name customers see.",
                        key="sector_compare_selected_chain_name_select",
                        disabled=not st.session_state["sector_comparison_selected_sectors"]
                    )
                    
                    if chain_selection != valid_chain_selection:
                        logging.info(f"Chain selection changed from {valid_chain_selection} to {chain_selection}")
                        st.session_state["sector_compare_selected_chain_name"] = chain_selection
                        # Reset location filters when retailers change
                        st.session_state["sector_compare_selected_state_name"] = ["All"]
                        st.session_state["sector_compare_selected_msa_name"] = ["All"]
                        st.session_state["sector_compare_selected_zip"] = ["All"]
                        update_cookies()
                        st.rerun()
                else:
                    logging.info("No sectors selected - showing disabled retailer filters")
                    st.multiselect(
                        "Select Company", 
                        options=[],
                        default=[],
                        disabled=True,
                        help="Parent company that owns one or more store banners."
                    )
                    
                    st.multiselect(
                        "Select Banner/Brand", 
                        options=[],
                        default=[],
                        disabled=True,
                        help="The specific retail banner or storefront name customers see."
                    )

        with col4:
                with st.expander("Location", expanded=st.session_state["sector_comparison_expand_filters"]):
                    logging.info("Initializing location filters")

                    def mutually_exclusive_all(selected, all_option, valid_options):
                        """
                        Ensures 'All' is mutually exclusive in multi-select: 
                        - If 'All' is picked with others, keep only others.
                        - If 'All' alone picked or nothing selected, return ['All']
                        - If user selects 'All' after selecting others, clear all others so only ['All'] is selected.
                        """
                        # If "All" is in the list and more than one selected, keep only others (remove All)
                        if all_option in selected and len(selected) > 1:
                            # If user clicked 'All' after other options, return ['All'] only
                            if selected[-1] == all_option:
                                return [all_option]
                            else:
                                return [s for s in selected if s != all_option]
                        elif not selected:
                            return [all_option]
                        else:
                            return selected
                    # 1. Filtering logic as before...
                    selected_sectors = st.session_state.get("sector_comparison_selected_sectors", [])
                    if isinstance(selected_sectors, str):
                        selected_sectors = [selected_sectors]
                    current_filtered_data = active_data.copy()
                    start_date, end_date = st.session_state["sector_compare_date_range"]
                    current_filtered_data = current_filtered_data[
                        (current_filtered_data['Period'] >= pd.Timestamp(start_date)) &
                        (current_filtered_data['Period'] <= pd.Timestamp(end_date))
                    ]
                    if selected_sectors:
                        current_filtered_data = current_filtered_data[
                            current_filtered_data["Sector_Coresight"].isin(selected_sectors)
                        ]
                    parent_chain_list = st.session_state["sector_compare_parent_chain_name"]
                    explicit_chain_list = st.session_state["sector_compare_selected_chain_name"]
                    all_selected_chain_names = set()
                    if parent_chain_list or explicit_chain_list:
                        if parent_chain_list:
                            parents_filtered = current_filtered_data[
                                current_filtered_data['ParentName_Coresight'].isin(parent_chain_list)
                            ]
                            parent_derived_chains = parents_filtered['ChainName_Coresight'].dropna().unique().tolist()
                        else:
                            parent_derived_chains = []
                        all_selected_chain_names = set(parent_derived_chains).union(explicit_chain_list)
                        if all_selected_chain_names:
                            current_filtered_data = current_filtered_data[
                                current_filtered_data["ChainName_Coresight"].isin(all_selected_chain_names)
                            ]
                    if current_filtered_data.empty:
                        valid_states = []
                    else:
                        valid_states = sorted(current_filtered_data['State'].dropna().unique().tolist())
                    state_options = ["All"] + valid_states

                    # -------- STATE MULTISELECT --------
                    all_option = "All"
                    ss_state_name = st.session_state.get("sector_compare_selected_state_name", [])
                    ss_state_name = [s for s in ss_state_name if s == all_option or s in valid_states]
                    if not ss_state_name or set(ss_state_name) == set(valid_states):
                        default_states = [all_option]
                    else:
                        default_states = ss_state_name

                    updated_selected_states = st.multiselect(
                        "Select State",
                        options=state_options,
                        default=default_states,
                        help="U.S. state where the store is located.",
                        key="sector_compare_multiselect_state",
                        disabled=not len(st.session_state["sector_comparison_selected_sectors"]) >=2
                    )

                    final_selected_states = mutually_exclusive_all(updated_selected_states, all_option, valid_states)
                    # If change, update and rerun for instant chip/UI correction
                    if final_selected_states != ss_state_name:
                        st.session_state["sector_compare_selected_state_name"] = final_selected_states
                        st.session_state["sector_compare_selected_zip"] = ["All"]
                        st.session_state["sector_compare_selected_msa_name"] = ["All"]
                        # if "All" not in final_selected_states:
                        #     st.session_state["sector_compare_selected_msa_name"] = ["All"]
                        st.rerun()

                    # st.markdown("""
                    # <style>
                    # .st-emotion-cache-wfksaw {
                    #     gap: 0.2rem !important; /* Reduce the gap to a smaller value */
                    # }
                    # </style>
                    # """, unsafe_allow_html=True)
                    # _, center_col, _ = st.columns([20, 80, 10])
                    # with center_col:
                    #     # st.radio(
                    #     #     "Location Filter",
                    #     #     ["MSA", "Zip Code"],
                    #     #     horizontal=True,
                    #     #     key="location_type_sector_active",
                    #     # label_visibility="collapsed"
                    #     # )
                    #     if "location_type_sector_active" not in st.session_state:
                    #         st.session_state["location_type_sector_active"] = "MSA"

                    # # -------- ZIP CODE MULTISELECT ---------
                    # if st.session_state["location_type_sector_active"] == "Zip Code":
                    #     if "All" in st.session_state["sector_compare_selected_state_name"]:
                    #         location_state_filtered = current_filtered_data
                    #     else:
                    #         location_state_filtered = current_filtered_data[
                    #             current_filtered_data["State"].isin(st.session_state["sector_compare_selected_state_name"])
                    #         ]
                    #     if location_state_filtered.empty:
                    #         valid_zip_codes = []
                    #     else:
                    #         valid_zip_codes = sorted(location_state_filtered['PostalCode'].dropna().astype(str).unique().tolist())
                    #     zip_options = ["All"] + valid_zip_codes

                    #     old_ss_selected_zip = st.session_state.get("sector_compare_selected_msa_name", [])
                    #     ss_selected_zip = [z for z in old_ss_selected_zip if z == all_option or z in valid_zip_codes]
                    #     if not ss_selected_zip or set(ss_selected_zip) == set(valid_zip_codes):
                    #         default_zips = [all_option]
                    #     else:
                    #         default_zips = ss_selected_zip

                    #     updated_selected_zips = st.multiselect(
                    #         "Select Postal Code",
                    #         options=zip_options,
                    #         default=default_zips,
                    #         help="U.S. postal code where the store is located.",
                    #         key="sector_compare_multiselect_zip",
                    #         disabled=not st.session_state["sector_comparison_selected_sectors"]
                    #     )

                    #     final_selected_zips = mutually_exclusive_all(updated_selected_zips, all_option, valid_zip_codes)
                    #     if final_selected_zips != ss_selected_zip:
                    #         st.session_state["sector_compare_selected_msa_name"] = final_selected_zips
                    #         st.rerun()

                    # # -------- MSA MULTISELECT ---------
                    # else:
                    #     if "All" in st.session_state["sector_compare_selected_state_name"]:
                    #         location_state_filtered = current_filtered_data
                    #     else:
                    #         location_state_filtered = current_filtered_data[
                    #             current_filtered_data["State"].isin(st.session_state["sector_compare_selected_state_name"])
                    #         ]
                    #     if location_state_filtered.empty:
                    #         valid_msas = []
                    #     else:
                    #         valid_msas = sorted(location_state_filtered['MsaName'].dropna().unique().tolist())
                    #     msa_options = ["All"] + valid_msas

                    #     old_ss_selected_msa = st.session_state.get("sector_compare_selected_msa_name", [])
                    #     ss_selected_msa = [m for m in old_ss_selected_msa if m == all_option or m in valid_msas]
                    #     if not ss_selected_msa or set(ss_selected_msa) == set(valid_msas):
                    #         default_msas = [all_option]
                    #     else:
                    #         default_msas = ss_selected_msa

                    #     multiselect_label = "Select Metropolitan Statistical Area (MSA)"

                    #     updated_selected_msas = st.multiselect(
                    #         multiselect_label,
                    #         options=msa_options,
                    #         default=default_msas,
                    #         help="A metro region centered on a large city plus its economically linked suburbs.",
                    #         key="sector_compare_multiselect_msa",
                    #         disabled=not st.session_state["sector_comparison_selected_sectors"]
                    #     )

                    #     final_selected_msas = mutually_exclusive_all(updated_selected_msas, all_option, valid_msas)
                    #     if final_selected_msas != ss_selected_msa:
                    #         st.session_state["sector_compare_selected_msa_name"] = final_selected_msas
                    #         st.rerun()
                    # In the location expander, after the state filter:

                    # 1. Filter data based on selected states for MSA filter
                    if "All" in st.session_state["sector_compare_selected_state_name"]:
                        msa_state_filtered = current_filtered_data
                    else:
                        msa_state_filtered = current_filtered_data[
                            current_filtered_data["State"].isin(st.session_state["sector_compare_selected_state_name"])
                        ]

                    # 2. Get valid MSAs based on filtered data
                    valid_msas = sorted(msa_state_filtered['MsaName'].dropna().unique().tolist()) if not msa_state_filtered.empty else []
                    msa_options = ["All"] + valid_msas

                    # 3. Get current MSA selection from session state
                    old_ss_selected_msa = st.session_state.get("sector_compare_selected_msa_name", ["All"])
                    ss_selected_msa = [m for m in old_ss_selected_msa if m == "All" or m in valid_msas]
                    if not ss_selected_msa :
                        default_msas = ["All"]
                    else:
                        default_msas = ss_selected_msa

                    # 4. MSA Multiselect
                    updated_selected_msas = st.multiselect(
                        "Select Metropolitan Statistical Area (MSA)",
                        options=msa_options,
                        default=default_msas,
                        help="A metro region centered on a large city plus its economically linked suburbs.",
                        key="sector_compare_multiselect_msa",
                        disabled=not len(st.session_state["sector_comparison_selected_sectors"]) >=2
                    )

                    # 5. Handle MSA selection change
                    final_selected_msas = mutually_exclusive_all(updated_selected_msas, "All", valid_msas)
                    if final_selected_msas != ss_selected_msa:
                        st.session_state["sector_compare_selected_msa_name"] = final_selected_msas
                        # Reset zip code selection when MSA changes
                        if "sector_compare_selected_zip" in st.session_state:
                            st.session_state["sector_compare_selected_zip"] = ["All"]
                        st.rerun()

                    # 6. Filter data for zip codes based on selected states and MSAs
                    zip_state_msa_filtered = msa_state_filtered
                    if "All" not in final_selected_msas:
                        zip_state_msa_filtered = zip_state_msa_filtered[
                            zip_state_msa_filtered["MsaName"].isin(final_selected_msas)
                        ]

                    # 7. Get valid zip codes based on filtered data
                    valid_zip_codes = sorted(zip_state_msa_filtered['PostalCode'].dropna().astype(str).unique().tolist()) if not zip_state_msa_filtered.empty else []
                    zip_options = ["All"] + valid_zip_codes

                    # 8. Get current zip code selection from session state
                    if "sector_compare_selected_zip" not in st.session_state:
                        st.session_state["sector_compare_selected_zip"] = ["All"]
                        
                    old_ss_selected_zip = st.session_state["sector_compare_selected_zip"]
                    ss_selected_zip = [z for z in old_ss_selected_zip if z == "All" or z in valid_zip_codes]
                    # if not ss_selected_zip or set(ss_selected_zip) == set(valid_zip_codes):
                    #     default_zips = ["All"]
                    # else:
                    #     default_zips = ss_selected_zip
                    if not ss_selected_zip:
                        default_zips = ["All"]
                    else:
                        default_zips = ss_selected_zip


                    # 9. Zip Code Multiselect
                    updated_selected_zips = st.multiselect(
                        "Select Zip Code",
                        options=zip_options,
                        default=default_zips,
                        help="U.S. postal area where the store is located.",
                        key="sector_compare_multiselect_zip",
                        disabled=not len(st.session_state["sector_comparison_selected_sectors"]) >=2
                    )
                    # 10. Handle Zip Code selection change
                    final_selected_zips = mutually_exclusive_all(updated_selected_zips, "All", valid_zip_codes)

                    if final_selected_zips != ss_selected_zip:
                        st.session_state["sector_compare_selected_zip"] = final_selected_zips
                        # st.session_state["sector_compare_multiselect_zip"] = final_selected_zips
                        st.rerun()
    

                    # Apply MSA filter if specific MSAs are selected
                    if "All" not in final_selected_msas:
                        current_filtered_data = current_filtered_data[
                            current_filtered_data["MsaName"].isin(final_selected_msas)
                        ]

                    # Apply Zip Code filter if specific zip codes are selected
                    if "All" not in final_selected_zips:
                        current_filtered_data = current_filtered_data[
                            current_filtered_data["PostalCode"].astype(str).isin(final_selected_zips)
                        ]
                    # --- logging
                    logging.info(f"Current state selection (valid): {st.session_state['sector_compare_selected_state_name']}")
                    logging.info(f"Current MSA selection (valid): {st.session_state['sector_compare_selected_msa_name']}")

        if not st.session_state["sector_comparison_selected_sectors"]:
            st.info("Please select at least two sectors to start the sector comparison")
        else:
            selected_sectors = st.session_state["sector_comparison_selected_sectors"]
            
            # Convert to list if it's a string (single selection)
            if isinstance(selected_sectors, str):
                selected_sectors = [selected_sectors]
            
            if len(selected_sectors) < 2:
                st.info("Please select at least two sectors to start the sector comparison")
            else:
                st.write(f"Comparing sectors: {' vs '.join(selected_sectors)}")
                
                try:
                    # Build a set of chain names from selected parents + chain-level picks
                    selected_chain_names_union = set()

                    if st.session_state.get("sector_compare_parent_chain_name"):
                        parent_filtered_data = active_data[
                            active_data['ParentName_Coresight'].isin(st.session_state["sector_compare_parent_chain_name"])
                        ]
                        parent_chain_list = parent_filtered_data['ChainName_Coresight'].dropna().unique().tolist()
                        selected_chain_names_union.update(parent_chain_list)

                    if st.session_state.get("sector_compare_selected_chain_name"):
                        selected_chain_names_union.update(st.session_state["sector_compare_selected_chain_name"])

                    # Filter active_data for selected sectors and date range
                    start_date, end_date = st.session_state["sector_compare_date_range"]
                    filtered_active_compare_for_sectors = active_data[
                        (active_data['Period'] >= pd.Timestamp(start_date)) &
                        (active_data['Period'] <= pd.Timestamp(end_date)) &
                        (active_data["Sector_Coresight"].isin(selected_sectors))
                    ]

                    # Apply state filter only if "All" is NOT selected
                    selected_states = st.session_state.get("sector_compare_selected_state_name", ["All"])
                    if "All" not in selected_states:
                        filtered_active_compare_for_sectors = filtered_active_compare_for_sectors[
                            filtered_active_compare_for_sectors["State"].isin(selected_states)
                        ]

                    # Apply MSA filter only if "All" is NOT selected
                    selected_msas = st.session_state.get("sector_compare_selected_msa_name", ["All"])
                    if "All" not in selected_msas :
                        filtered_active_compare_for_sectors = filtered_active_compare_for_sectors[
                            filtered_active_compare_for_sectors["MsaName"].isin(selected_msas)
                        ]
                    selected_zips = st.session_state.get("sector_compare_selected_zip", ["All"])
                    # Apply Zip Code filter only if "All" is NOT selected and location type is Zip Code
                    if "All" not in selected_zips:
                        filtered_active_compare_for_sectors = filtered_active_compare_for_sectors[
                            filtered_active_compare_for_sectors["PostalCode"].astype(str).isin([str(z) for z in selected_zips])
                        ]

                    # Create a copy for retailer-specific filtering
                    filtered_active_compare_for_retailers = filtered_active_compare_for_sectors.copy()
                    if selected_chain_names_union:
                        filtered_active_compare_for_retailers = filtered_active_compare_for_retailers[
                            filtered_active_compare_for_retailers["ChainName_Coresight"].isin(selected_chain_names_union)
                        ]

                    if filtered_active_compare_for_sectors.empty:
                        st.warning("No data available for the selected filters.")
                        st.stop()

                    # Define a consistent color map for sectors
                    color_map = {
                        sector: color for sector, color in zip(
                            selected_sectors,
                            px.colors.qualitative.Plotly  # Using Plotly's qualitative color palette
                        )
                    }

                    # Visualization logic - Line and Bar charts
                    line_col, bar_col = st.columns([4, 2])

                    with line_col:
                        # Prepare the data
                        monthly_data = (
                            filtered_active_compare_for_sectors
                            .groupby(['Period', 'Sector_Coresight'])
                            .size()
                            .reset_index(name='ActiveStores')
                        )
                        all_periods = pd.date_range(start=start_date, end=end_date, freq='MS')
                        full_data = pd.DataFrame()

                        for sector in selected_sectors:
                            sector_only = monthly_data[monthly_data['Sector_Coresight'] == sector]
                            sector_only = sector_only.set_index('Period').reindex(all_periods, fill_value=0).reset_index()
                            sector_only['Sector_Coresight'] = sector
                            full_data = pd.concat([full_data, sector_only])

                        # Create a mapping of period to metadata for each sector
                        sector_metadata = {}
                        
                        for sector, grp_df in full_data.groupby('Sector_Coresight'):
                            sector_metadata[sector] = {}
                            
                            for period in grp_df['index']:
                                # Get the data for this specific sector and period
                                period_data = filtered_active_compare_for_sectors[
                                    (filtered_active_compare_for_sectors['Sector_Coresight'] == sector) &
                                    (filtered_active_compare_for_sectors['Period'] == period)
                                ]
                                
                                # Calculate metadata for this specific data point
                                num_banners = period_data['ChainName_Coresight'].nunique()
                                num_states = period_data['State'].nunique()
                                num_msa = period_data['MsaName'].nunique()
                                num_chains = period_data['ChainName_Coresight'].nunique()
                                
                                sector_metadata[sector][period] = {
                                    'banners': num_banners,
                                    'states': num_states,
                                    'msa': num_msa,
                                    'chains': num_chains
                                }

                        # Create figure with consistent styling
                        fig_line = go.Figure()
                        
                        for sector, grp_df in full_data.groupby('Sector_Coresight'):
                            color = color_map.get(sector, "#CBCACA")  # Gray fallback for active stores
                            
                            # Prepare customdata for this sector
                            customdata_list = []
                            for period in grp_df['index']:
                                metadata = sector_metadata[sector].get(period, {'banners': 0, 'states': 0, 'msa': 0, 'chains': 0})
                                customdata_list.append([
                                    metadata['banners'],
                                    metadata['states'],
                                    metadata['msa'],
                                    metadata['chains']
                                ])
                            
                            fig_line.add_trace(go.Scatter(
                                x=grp_df['index'],
                                y=grp_df['ActiveStores'],
                                mode='lines+markers+text',
                                name=sector,
                                line=dict(color=color, width=2),
                                marker=dict(size=7, color=color),
                                text=[f"{x:,}" for x in grp_df['ActiveStores']],
                                textposition='top center',
                                hovertemplate=(
                                    "<b>%{x|%b %Y}</b><br>"
                                    f"<b>{sector}</b><br>"
                                    "<span style='color:" + color + "'>●</span> "
                                    "<b>Active Store Count:</b> <b>%{y:,}</b><br><br>"
                                    # "<span style='color:" + color + "'>•</span> "
                                    # "<span style='color:black'>Date Period: %{x|%b %Y}</span><br>"
                                    "<span style='color:" + color + "'>•</span> "
                                    "<span style='color:black'>Number of Banners: %{customdata[0]}</span><br>"
                                    "<span style='color:" + color + "'>•</span> "
                                    "<span style='color:black'>Number of States: %{customdata[1]}</span><br>"
                                    "<span style='color:" + color + "'>•</span> "
                                    "<span style='color:black'>Number of MSA: %{customdata[2]}</span><br>"
                                    "<span style='color:" + color + "'>•</span> "
                                    "<span style='color:black'>Number of Chains: %{customdata[3]}</span><br>"
                                    "<extra></extra>"
                                ),
                                customdata=customdata_list,
                                hoverlabel=dict(
                                    bgcolor="white",
                                    bordercolor=color,
                                    font=dict(size=13, color="black", family="Arial")
                                )
                            ))

                        # Update layout with consistent styling
                        fig_line.update_layout(
                            title='Monthly Active Stores by Sector',
                            xaxis_title="Month",
                            yaxis_title="Active Stores",
                            xaxis=dict(
                                tickformat="%b %Y", 
                                tickangle=45,
                                showline=True,
                                zeroline=False
                            ),
                            yaxis=dict(
                                showline=True,
                                zeroline=False
                            ),
                            legend=dict(
                                x=0.99,
                                y=0.99,
                                xanchor='right',
                                yanchor='top',
                                bgcolor='rgba(0,0,0,0)',
                                bordercolor='rgba(0,0,0,0)',
                                font=dict(size=13)
                            ),
                            margin=dict(l=10, r=10, t=60, b=40),
                            height=400,
                            uniformtext_minsize=8,
                            uniformtext_mode='hide'
                        )

                        # Same config with updated height
                        config = {
                            'displayModeBar': True,
                            'displaylogo': False,
                            'toImageButtonOptions': {
                                'format': 'png',
                                'filename': 'monthly_active_stores_by_sector',
                                'height': 400,
                                'width': 700,
                                'scale': 1
                            }
                        }

                        st.plotly_chart(fig_line, use_container_width=True, config=config)

                    with bar_col:
                        total_counts = (
                            filtered_active_compare_for_sectors
                            .groupby('Sector_Coresight')
                            .size()
                            .reset_index(name='TotalActiveStores')  # Changed from TotalClosedStores to TotalActiveStores
                        )

                        # Create figure with consistent styling
                        fig_bar = go.Figure()
                        
                        for _, row in total_counts.iterrows():
                            sector = row['Sector_Coresight']
                            color = color_map.get(sector, "#CBCACA")  # Gray fallback for active stores
                            
                            fig_bar.add_trace(go.Bar(
                                x=[sector],
                                y=[row['TotalActiveStores']],
                                name=sector,
                                marker_color=color,
                                text=[f"{row['TotalActiveStores']:,}"],  # Formatted with thousands separator
                                textposition='outside',
                                hovertemplate=(
                                    "<b>%{x}</b><br>"  # Sector name
                                    "<span style='color:" + color + "'>●</span> "
                                    "<b>Active Stores:</b> %{y:,}<extra></extra>"
                                ),
                                hoverlabel=dict(
                                    bgcolor="white",
                                    bordercolor=color,
                                    font=dict(size=13, color="black", family="Arial")
                                )
                            ))

                        # Update layout with consistent styling
                        fig_bar.update_layout(
                            title='Total Active Stores by Sector',
                            xaxis_title="Sector",
                            yaxis_title="Active Stores",
                            showlegend=False,
                            height=500,
                            margin=dict(l=20, r=20, t=80, b=60),  # Adjusted margins
                            xaxis=dict(
                                showline=True,
                                zeroline=False,
                                tickangle=45 if len(total_counts) > 5 else 0
                            ),
                            yaxis=dict(
                                showline=True,
                                zeroline=False
                            ),
                            uniformtext_minsize=8,
                            uniformtext_mode='hide'
                        )

                        # Adjust for many sectors
                        if len(total_counts) > 8:
                            fig_bar.update_layout(
                                xaxis=dict(tickangle=60),
                                margin=dict(b=80)  # Extra bottom margin
                            )

                        st.plotly_chart(fig_bar, use_container_width=True)

                    # Retailer Performance by Sector section
                    st.subheader("Banner Performance by Sector")
                    
                    # Determine which data to use based on retailer selections
                    display_data = filtered_active_compare_for_retailers if selected_chain_names_union else filtered_active_compare_for_sectors
                    
                    for sector in selected_sectors:
                        sector_data = display_data[display_data['Sector_Coresight'] == sector]
                        if sector_data.empty:
                            continue

                        retailer_counts = (
                            sector_data['ChainName_Coresight']
                            .value_counts()
                            .reset_index()
                        )
                        retailer_counts.columns = ['Banners', 'ActiveStores']  # Changed from ClosedStores to ActiveStores

                        color = color_map.get(sector, "#CBCACA")  # Gray fallback for active stores

                        # Create figure with consistent styling
                        fig_retailer = go.Figure()
                        
                        fig_retailer.add_trace(go.Bar(
                            x=retailer_counts['Banners'],
                            y=retailer_counts['ActiveStores'],
                            name=sector,
                            marker_color=color,
                            text=[f"{x:,}" for x in retailer_counts['ActiveStores']],  # Formatted with thousands separator
                            textposition='outside',
                            hovertemplate=(
                                "<b>%{x}</b><br>"  # Banner name
                                "<span style='color:" + color + "'>●</span> "
                                "<b>Active Stores:</b> %{y:,}<extra></extra>"
                            ),
                            hoverlabel=dict(
                                bgcolor="white",
                                bordercolor=color,
                                font=dict(size=13, color="black", family="Arial")
                            )
                        ))

                        # Calculate dynamic bottom margin based on number of banners
                        bottom_margin = 40 + (10 * len(retailer_counts))
                        
                        fig_retailer.update_layout(
                            title=f'Top Banners in {sector}',
                            xaxis={
                                'categoryorder': 'total descending',
                                'title': None,
                                'tickangle': 45 if len(retailer_counts) > 5 else 0,
                                'tickfont': dict(size=11)
                            },
                            yaxis={
                                'showticklabels': False,
                                'showgrid': False,
                                'title': None
                            },
                            showlegend=False,
                            height=400,
                            margin=dict(t=60, b=bottom_margin, l=10, r=10),
                            uniformtext_minsize=8,
                            uniformtext_mode='hide'
                        )

                        # Adjust for many banners
                        if len(retailer_counts) > 8:
                            fig_retailer.update_layout(
                                xaxis=dict(tickangle=60),
                                margin=dict(b=60 + (10 * len(retailer_counts)))
                            )

                        st.plotly_chart(fig_retailer, use_container_width=True)

                    # Prepare table data
                    table_data = display_data.copy()
                    table_data['Year'] = table_data['Period'].dt.year
                    table_data['Month'] = table_data['Period'].dt.month
                    table_data['Closing Month/Year'] = (table_data['Month'].astype(str) + '-' + table_data['Year'].astype(str))
                    
                    # Select and rename columns
                    table_data = table_data[[
                        'ChainName_Coresight', 'ParentName_Coresight', 'Address', 'Address2',
                        'City', 'MsaName', 'PostalCode', 'State', 'Country', 'Sector_Coresight',
                        'Closing Month/Year'
                    ]]
                    
                    table_data = table_data.rename(columns={
                        'ChainName_Coresight': 'Banner/Brand Name',
                        'ParentName_Coresight': 'Company Name',
                        'Address': 'Address Line 1',
                        'Address2': 'Address Line 2',
                        'City': 'City',
                        'MsaName': 'Metropolitan Statistical Area',
                        'PostalCode': 'Postal Code',
                        'State': 'State',
                        'Country': 'Country',
                        'Sector_Coresight': 'Sector',
                        'Closing Month/Year': 'Active Date'
                    })

                    col1, col4  = st.columns([7, 1.05])
                    with col1:
                        st.subheader('Active Stores Table')

                    with col4:
                        if not table_data.empty:
                            excel_buffer = io.BytesIO()
                            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                                table_data.to_excel(writer, index=False, sheet_name='Active_Stores')
                                worksheet = writer.sheets['Active_Stores']
                                for i, col in enumerate(table_data.columns):
                                    column_len = max(
                                        table_data[col].astype(str).map(len).max(),
                                        len(col)
                                    )
                                    worksheet.set_column(i, i, column_len + 2)
                            formatted_start_date = start_date.strftime("%Y-%m-%d")
                            formatted_end_date = end_date.strftime("%Y-%m-%d")
                            final_filename = f"Active_Stores_{formatted_start_date}_to_{formatted_end_date}.xlsx"
                            
                            st.markdown("""
                                <style>
                                    .stDownloadButton>button {
                                        background-color: #CBCACA;
                                        color: white;
                                        border: none;
                                    }
                                    .stDownloadButton>button:hover {
                                        border: 2px solid #CBCACA;
                                        background-color: white;
                                        color: #CBCACA;
                                    }
                                </style>
                            """, unsafe_allow_html=True)

                            if not is_free_trial:
                                # Premium users → real download
                                excel_buffer.seek(0)
                                st.download_button(
                                    label="Download Data",
                                    data=excel_buffer, # This buffer now contains the autofitted Excel file
                                    file_name=final_filename,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_Active_xlsx",
                                    use_container_width=True
                                )
     
                            else:
                                # Trial users → fake download button that opens your modal
                                if st.button("Download Data", key="download_table_blocked", use_container_width=True):
                                    _trial_modal_active()


                            # Your download button
                            st.markdown('</div>', unsafe_allow_html=True)

                    # AgGrid(table_data, height=600) # You can adjust the height as needed

                    GRID_H = 600  # must match your AgGrid height

                    # Always show real data in the grid
                    df_for_grid = table_data

                    # Lock icon
                    ICON = pathlib.Path(__file__).resolve().parent.parent / "assets" / "icons" / "lock.png"
                    lock_b64 = base64.b64encode(ICON.read_bytes()).decode("utf-8")

                    gb = GridOptionsBuilder.from_dataframe(df_for_grid)
                    grid_options = gb.build()

                    # Render the grid
                    AgGrid(
                        df_for_grid,
                        gridOptions=grid_options,
                        height=GRID_H,
                        key="active_grid",
                    )

                    # Overlay (only for trial users)
                    if is_free_trial:
                        st.markdown(f"""
                        <style>
                          /* Overlay panel covers the grid completely */
                          .grid-overlay-flag[data-target="active"] {{
                            position: relative;
                            height: {GRID_H}px;
                            margin-top: -{GRID_H}px;   /* pull on top of grid */
                            display: flex;
                            align-items: center;
                            justify-content: center;
                            flex-direction: column;
                            gap: 20px;
                            background: rgba(255,255,255,0.55);  /* translucent white wash */
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

                        <div class="grid-overlay-flag" data-target="active">
                          <img src="data:image/png;base64,{lock_b64}" class="lock" alt="Locked"/>
                          <div class="title">For SIP Members Only</div>
                          <p style="margin:8px 0 18px 0;">
                            Individual Store data is exclusively accessible to SIP members and is not available for trial users.
                          </p>
                          <a class="cta" href="https://coresight.com/contact/" target="_blank">Contact Us</a>
                        </div>
                        """, unsafe_allow_html=True)

                except Exception as e:
                    st.error(f"An error occurred while processing the data: {str(e)}")
                    st.stop()

        st.markdown("---")  # Horizontal line for separation
        try:
            # Get the current URL to determine environment
            current_url = st.context.headers.get("host", "")
            is_staging = "stage" in current_url.lower()
        except:
            # Fallback method
            is_staging = False

        # Set the appropriate URL based on environment
        if is_staging:
            overview_url = "https://stage3.coresight.com/store-intelligence-platform-overview/"
        else:
            overview_url = "https://www.coresight.com/store-intelligence-platform-overview/"

        st.markdown(f"<i>Data available through <b>{latest_ts}</b></i>",unsafe_allow_html=True)
        st.markdown(
            f"""
            <p style='color: gray; font-size: small;'>
            Disclaimer: Certain data are derived from calculations that use data licensed from third parties, including ChainXY. 
            Coresight Research has made substantial efforts to clean the data and identify potential issues. However, changes to retailers' store locators 
            may impact database-sourced data. See our 
            <a href="{overview_url}" target="_blank">Overview</a> 
            document and <a href="/change-logs#store-intelligence-platform" target="_self">Data Release Notes</a>  for more details.
            </p>
            """,
            unsafe_allow_html=True,
        )
        placeholder.empty()

    include_html("footer.html")

except ValueError as e:
    st.write(f"No data available. Please reset filters or refresh page.",e)
    placeholder.empty()