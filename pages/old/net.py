# Version 2.2.2
from sqlalchemy.sql.functions import user
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
    from datetime import datetime ,timedelta, date
    from time import sleep
    import logging
    from sqlalchemy import create_engine
    from sqlalchemy import create_engine
    import calendar
    from dotenv import load_dotenv
    from auth_utils import logout,restore_session_from_cookie,require_auth,get_current_domain
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
    import time
    from html_utils import include_html
    from streamlit.components.v1 import html
    from sqlalchemy import create_engine
    from datetime import datetime
    from dotenv import load_dotenv
    from st_aggrid import AgGrid
    from streamlit_cookies_controller import CookieController
    from streamlit_extras.switch_page_button import switch_page
    import streamlit as st
    import pandas as pd
    import calendar
    from datetime import date
    from dateutil.relativedelta import relativedelta
    from datetime import datetime
    import plotly.graph_objects as go
    import plotly.express as px
    import streamlit as st
    import calendar
    from datetime import date
    import json
    time.sleep(1)
    restore_session_from_cookie()
    session_id = require_auth()
    time.sleep(1)

    load_dotenv()


    
    st.set_page_config(
        page_title="Net Openings",  # Title of the app
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

    # if not ensure_authenticated_safe(strict=STRICT_AUTH):
    #     st.switch_page("login.py")    
    # if "auth_checked" not in st.session_state:
    #     st.session_state.auth_checked = False

    # if not st.session_state.auth_checked:
    #     st.session_state.auth_checked = True
    #     if not is_authenticated_from_cookie():
    #         st.rerun()  # Give cookie a chance to load
    # else:
    #     st.session_state.is_authenticated = True

    # if not st.session_state.get("is_authenticated", False):
    #     st.switch_page("login.py")

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

    # try:
    #     cookies = CookieController().getAll() or {}
    # except Exception:
    #     cookies = {}

    # try:
    #     auth_cookie = cookies.get("auth_data")
    # except Exception:
    #     auth_cookie = {}
        
    # status = _has_premium(auth)

    # if status is False:
    #     st.error(
    #         "Sorry, Store Intelligence Platform is available for premium subscribers only. "
    #         "Please contact us to know more details."
    #     )
    #     st.stop()

    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    LOG_LEVEL_STR = os.getenv("LOG_LEVEL") or 'ERROR'
    SSL_CA = os.getenv("SSL_CA")  # Full path to your ca.pem file

    log_level = getattr(logging, LOG_LEVEL_STR, logging.ERROR) # default to INFO if invalid level

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

    try:
        auth_cookie = cookie_controller.get("auth_data")
    except Exception:
        auth_cookie = {}

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


    # is_login_needed = check_and_restore_cookie_session()
    # if not is_login_needed:
    #     st.switch_page("login.py")

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

    def make_json_safe(val):
        """Recursively convert pandas Timestamp to iso string in dictionaries/lists."""
        if isinstance(val, pd.Timestamp):
            return val.isoformat()
        if isinstance(val, list):
            return [make_json_safe(x) for x in val]
        if isinstance(val, dict):
            return {k: make_json_safe(v) for k, v in val.items()}
        return val

    # try:
    #     ctrl = CookieController(key="auth_cookies")
    #     get_all = getattr(ctrl, "get_all", None) or getattr(ctrl, "getAll", None)
    #     cookies = get_all() or {}
    #     auth_cookie = st.session_state.get("auth_data") or cookies.get("auth_data")
    # except Exception as e:
    #     logging.warning(f"cookie read failed: {e}")
    #     auth_cookie = None
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
            user_filters["returnPage"] = "net"
            safe_cookie = make_json_safe(auth_cookie)
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

    # st.cache_data.clear()   

    today = dt.date.today()

    if "last_clear_date" not in st.session_state:
        st.session_state.last_clear_date = today

    if st.session_state.last_clear_date != today:
        st.cache_data.clear()
        st.session_state.last_clear_date = today
        st.toast("Cache cleared at midnight")

    @st.cache_data(show_spinner=False)
    def fetch_separate_data():
        connection_params = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME')
        }

        # if os.getenv('ENABLE_SSL', 'false').lower() == 'true' and os.getenv('SSL_CA'):
        #     connection_params['sslcert'] = os.getenv('SSL_CA')
        #     logging.info("SSL connection enabled")
        # else:
        #     logging.info("SSL connection not enabled")

        try:
            # Establish connection
            conn = mysql.connector.connect(**connection_params)

            # Opened stores query
            opened_query = """
            SELECT 
                o.StoreName, 
                o.StoreType, 
                o.ChainName_Coresight, 
                o.ParentName_Coresight, 
                o.Address, 
                o.Address2, 
                o.City, 
                o.MsaName, 
                o.PostalCode, 
                o.State, 
                o.Country, 
                o.Sector_Coresight, 
                o.Period, 
                o.Population, 
                o.UpdateCycle,
                'opened' AS data_from,
                p.FirstAppearedDate_ChainXY
            FROM (
                SELECT
                    StoreName, StoreType, ChainName_Coresight, ParentName_Coresight, 
                    Address, Address2, City, MsaName, PostalCode, State, Country, 
                    Sector_Coresight, Period, Population, status, UpdateCycle, duration_reopening, HashId
                FROM all_opened_py
                UNION ALL
                SELECT 
                    StoreName, StoreType, ChainName_Coresight, ParentName_Coresight, 
                    Address, Address2, City, MsaName, PostalCode, State, Country, 
                    Sector_Coresight, Period, Population, status, UpdateCycle, duration_reopening, HashId
                FROM all_opened_cy
                UNION ALL
                SELECT 
                    StoreName, StoreType, ChainName_Coresight, ParentName_Coresight, 
                    Address, Address2, City, MsaName, PostalCode, State, Country, 
                    Sector_Coresight, Period, Population, status, UpdateCycle, duration_reopening, HashId
                FROM all_opened_acquisition
            ) o
            LEFT JOIN parent_chain_names_data p ON o.ChainName_Coresight = p.ChainName_Coresight
            WHERE (
                    (o.ChainName_Coresight IS NULL OR o.ChainName_Coresight NOT IN ('Hoka', 'Sephora', 'Lululemon Athletica','Finish Line','Sunglass Hut',"Carter's",'Marc Jacobs','Bulgari', "Dick's Sporting Goods", 'CVS Pharmacy', 'Circle K', 'Aerie', 'American Eagle Outfitters',
                    '7-Eleven','Anthropologie','Babies"R"Us','Balenciaga','Bottega Veneta','Burberry','Cartier','Century 21','Chanel','Christian Dior',"Claire’s",'Coach',
                    "Conn's",'Converse','Deciem','Dolce & Gabbana','Fendi','Giant Eagle','Givenchy','Gucci','Hallmark','Hy-Vee','James Avery Artisan Jewelry',
                    "L'Occitane",'LensCrafters',"Levi's",'Lord & Taylor','Louis Vuitton','LUSH','Marc Jacobs','Massimo Dutti','Moncler','Pandora','Prada','Ralph Lauren',
                    'Ted Baker','The North Face','Under Armour','Williams Sonoma','Yves Saint laurent','Free People'))
                    OR (o.ChainName_Coresight = 'Hoka' AND LOWER(o.storename) LIKE '%hoka%')
                    OR (o.ChainName_Coresight = 'Sephora' AND LOWER(o.storename) NOT LIKE '%penney%' AND LOWER(o.storename) NOT LIKE '%kohl%')
                    OR (o.ChainName_Coresight = 'Lululemon Athletica' AND LOWER(o.storename) NOT LIKE '%pop%')
                    OR (o.ChainName_Coresight = 'Finish Line' AND LOWER(o.storename) NOT LIKE '%macy%' AND LOWER(o.storename) NOT LIKE '%jd sports%')
                    AND (o.ChainName_Coresight = 'Finish Line' AND COALESCE(o.storetype, '') NOT IN ('JD Sports'))
                    OR (o.ChainName_Coresight = 'Sunglass Hut' AND LOWER(o.storename) NOT LIKE '%macy%' AND LOWER(o.storename) NOT LIKE '%bloomingdale%')
                    OR (o.ChainName_Coresight = "Carter's" AND COALESCE(o.storetype, '') <> 'Oshkosh')
                    OR (o.ChainName_Coresight = 'Marc Jacobs' AND LOWER(o.storename) NOT LIKE '%macy%' AND LOWER(o.storename) NOT LIKE '%bloomingdale%'
                        AND LOWER(o.storename) NOT LIKE '%saks%' AND LOWER(o.storename) NOT LIKE '%neiman%' AND LOWER(o.storename) NOT LIKE 'bookmarc%')
                    OR (o.ChainName_Coresight = "Bulgari" AND COALESCE(o.storetype, '') NOT IN ('Official Retailers','Dept. Store (Ds)','Mall',NULL,'Airport','Street'))
                    OR (o.ChainName_Coresight = "Dick's Sporting Goods" AND LOWER(o.storename) NOT LIKE '%warehouse%' AND LOWER(o.storename) NOT LIKE '%temporary%' AND LOWER(o.storename) NOT LIKE '%going%')
                    OR (o.ChainName_Coresight = "CVS Pharmacy" AND LOWER(o.storename) NOT LIKE '%target%' AND LOWER(o.storename) NOT LIKE '%schnucks%')
                    OR (o.ChainName_Coresight = "Circle K" AND LOWER(o.storename) NOT LIKE '%car wash%' AND LOWER(o.storename) NOT LIKE '%holiday station%' AND LOWER(o.storename) NOT LIKE '%gas station%' AND LOWER(o.storename) NOT LIKE '%on the run%' AND LOWER(o.storetype) NOT LIKE '%holiday station%')
                    OR (o.ChainName_Coresight = 'Aerie' AND LOWER(o.storename) NOT IN ('american eagle', 'american eagle store', 'offline', 'american eagle , offline store', 'offline store', 'american eagle & offline', 'american eagle outlet', 'american eagle , offline outlet', 'american eagle clearance store', 'american eagle , offline', 'american eagle denim deli'))
                    OR (o.ChainName_Coresight = 'American Eagle Outfitters' AND LOWER(o.storename) NOT IN ('aerie - closed boulevard mall', 'offline', 'offline store', 'offline store - closed', 'aerie & offline', 'aerie store', 'aerie clearance store', 'aerie - santa rosa plaza', 'aerie outlet', 'aerie outlet - closed', 'aerie , offline store', 'aerie store - closed', 'unsubscribed', 'offline clearance store', 'aerie , offline', 'aerie streets at southpoint', 'aerie bangor mall', 'aerie crystal mall', 'aerie spring street', 'aerie lakeline mall', 'aerie south side works', 'aerie - closed spring street', 'aerie northlake mall', 'aerie los cerritos center', 'aerie annapolis mall', 'aerie green acres mall', 'aerie exton square mall', 'aerie anchorage fifth avenue mall', 'aerie fox river mall', 'aerie staten island mall', 'aerie charleston town center', 'aerie san francisco center', 'aerie the mall @ johnson city', 'aerie - west town mall', 'aerie the oaks', 'aerie - closed crystal mall', 'aerie park plaza mall', 'offline - mall of georgia', 'offline - natick mall'))
                    OR (o.ChainName_Coresight = '7-Eleven' AND COALESCE(o.storetype, '') NOT IN ('Stripes','Speedway'))
                    OR (o.ChainName_Coresight = 'Anthropologie' AND COALESCE(o.storetype, '') NOT IN ('Temp'))
                    OR (o.ChainName_Coresight = 'Babies"R"Us' AND LOWER(o.storename) NOT LIKE '%kohl%')
                    OR (o.ChainName_Coresight = 'Balenciaga' AND LOWER(o.storename) NOT LIKE '%saks%' AND LOWER(o.storename) NOT LIKE '%bloomingdale%')
                    OR (o.ChainName_Coresight = 'Bottega Veneta' AND LOWER(o.storename) NOT LIKE '%pop%' AND LOWER(o.storename) NOT LIKE '%saks%' AND LOWER(o.storename) NOT LIKE '%neiman%' AND LOWER(o.storename) NOT LIKE '%bloomingdale%')
                    OR (o.ChainName_Coresight = 'Burberry' AND LOWER(o.storename) NOT LIKE '%bloomingdale%')
                    OR (o.ChainName_Coresight = 'Cartier' AND LOWER(o.storename) NOT LIKE '%pop%')
                    OR (o.ChainName_Coresight = 'Century 21' AND LOWER(o.storename) NOT LIKE '%pop%')
                    OR (o.ChainName_Coresight = 'Chanel' AND LOWER(o.storename) NOT LIKE '%pop%' AND LOWER(o.storename) NOT LIKE '%saks%' AND LOWER(o.storename) NOT LIKE '%neiman%' AND LOWER(o.storename) NOT LIKE '%bloomingdale%')
                    OR (o.ChainName_Coresight = 'Christian Dior' AND LOWER(o.storename) NOT LIKE '%pop%' AND LOWER(o.storename) NOT LIKE '%saks%' AND LOWER(o.storename) NOT LIKE '%neiman%' AND LOWER(o.storename) NOT LIKE '%bloomingdale%')
                    OR (o.ChainName_Coresight = "Claire’s" AND LOWER(o.storename) NOT LIKE '%walmart%')
                    OR (o.ChainName_Coresight = 'Coach' AND COALESCE(o.storetype, '') NOT IN ('Coffee Shop'))
                    OR (o.ChainName_Coresight = "Conn's" AND LOWER(o.storename) NOT LIKE '%belk%')
                    OR (o.ChainName_Coresight = 'Converse' AND LOWER(o.storename) NOT LIKE '%pop%')
                    OR (o.ChainName_Coresight = 'Deciem' AND LOWER(o.storename) NOT LIKE '%pop%')
                    OR (o.ChainName_Coresight = 'Dolce & Gabbana' AND LOWER(o.storename) NOT LIKE '%pop%' AND LOWER(o.storename) NOT LIKE '%saks%' AND LOWER(o.storename) NOT LIKE '%nordstrom%' AND LOWER(o.storename) NOT LIKE '%neiman%' AND LOWER(o.storename) NOT LIKE '%bloomingdale%')
                    OR (o.ChainName_Coresight = 'Fendi' AND LOWER(o.storename) NOT LIKE '%pop%' AND LOWER(o.storename) NOT LIKE '%neiman%' AND LOWER(o.storename) NOT LIKE '%nordstrom%' AND LOWER(o.storename) NOT LIKE '%saks%')
                    OR (o.ChainName_Coresight = 'Finish Line' AND COALESCE(o.storetype, '') NOT IN ('JD Sports'))
                    OR (o.ChainName_Coresight = 'Giant Eagle' AND COALESCE(o.storetype, '') NOT IN ('WetGo'))
                    OR (o.ChainName_Coresight = 'Givenchy' AND LOWER(o.storename) NOT LIKE '%neiman%' AND LOWER(o.storename) NOT LIKE '%nordstrom%' AND LOWER(o.storename) NOT LIKE '%saks%')
                    OR (o.ChainName_Coresight = 'Gucci' AND LOWER(o.storename) NOT LIKE '%neiman%' AND LOWER(o.storename) NOT LIKE '%bloomingdale%' AND LOWER(o.storename) NOT LIKE '%saks%')
                    OR (o.ChainName_Coresight = 'Hallmark' AND LOWER(o.storename) NOT LIKE '%ace%' AND LOWER(o.storename) NOT LIKE '%pharmacy%' AND LOWER(o.storename) NOT LIKE '%health%')
                    OR (o.ChainName_Coresight = 'Hy-Vee' AND COALESCE(o.storetype, '') NOT IN ('Pharmacy'))
                    OR (o.ChainName_Coresight = 'James Avery Artisan Jewelry' AND LOWER(o.storename) NOT LIKE '%dillard%')
                    OR (o.ChainName_Coresight = "L'Occitane" AND COALESCE(o.storetype, '') NOT IN ('NON_OWNED'))
                    OR (o.ChainName_Coresight = 'LensCrafters' AND LOWER(o.storename) NOT LIKE '%macy%')
                    OR (o.ChainName_Coresight = "Levi's" AND COALESCE(o.storetype, '') NOT IN ('Retail Partner'))
                    OR (o.ChainName_Coresight = 'Lord & Taylor' AND LOWER(o.storename) NOT LIKE '%pop%')
                    OR (o.ChainName_Coresight = 'Louis Vuitton' AND LOWER(o.storename) NOT LIKE '%pop%' AND LOWER(o.storename) NOT LIKE '%bloomingdale%' AND LOWER(o.storename) NOT LIKE '%temporary%' AND LOWER(o.storename) NOT LIKE '%neiman%' AND LOWER(o.storename) NOT LIKE '%saks%')
                    OR (o.ChainName_Coresight = 'LUSH' AND LOWER(o.storename) NOT LIKE '%pop%')
                    OR (o.ChainName_Coresight = "Marc Jacobs" AND COALESCE(o.storetype, '') NOT IN ('Authorized retailer'))
                    OR (o.ChainName_Coresight = 'Massimo Dutti' AND LOWER(o.storename) NOT LIKE '%pop%')
                    OR (o.ChainName_Coresight = 'Moncler' AND LOWER(o.storename) NOT LIKE '%saks%' AND LOWER(o.storename) NOT LIKE '%bloomingdale%' AND LOWER(o.storename) NOT LIKE '%neiman%')
                    OR (o.ChainName_Coresight = 'Pandora' AND LOWER(o.storename) NOT LIKE '%macy%')
                    OR (o.ChainName_Coresight = 'Prada' AND LOWER(o.storename) NOT LIKE '%nordstrom%' AND LOWER(o.storename) NOT LIKE '%saks%' AND LOWER(o.storename) NOT LIKE '%neiman%' AND LOWER(o.storename) NOT LIKE '%bloomingdale%')
                    OR (o.ChainName_Coresight = 'Ralph Lauren' AND LOWER(o.storename) NOT LIKE '%Ralph''s Coffee%')
                    OR (o.ChainName_Coresight = 'Stop & Shop' AND COALESCE(o.storetype, '') NOT IN ('Pharmacy'))
                    OR (o.ChainName_Coresight = 'Ted Baker' AND LOWER(o.storename) NOT LIKE '%bloomingdale%')
                    OR (o.ChainName_Coresight = 'The North Face' AND LOWER(o.storename) NOT LIKE '%dick%' AND LOWER(o.storename) NOT LIKE '%oshkosh%')
                    OR (o.ChainName_Coresight = 'Under Armour' AND COALESCE(o.storetype, '') NOT IN ('Brand House'))
                    OR (o.ChainName_Coresight = 'Williams Sonoma' AND COALESCE(o.storetype, '') NOT IN ('STORE_IN_STORE'))
                    OR (o.ChainName_Coresight = 'Yves Saint laurent' AND LOWER(o.storename) NOT LIKE '%bloomingdale%')
                    OR (o.Chainname_Coresight = 'Free People' AND COALESCE(o.storetype, '') NOT IN ('FP Movement'))
                    AND (o.ChainName_Coresight = 'Free People' AND LOWER(o.storename) NOT LIKE '%movement%')
            )
            AND p.is_active = 1
            AND (o.status != 'Reopened' OR o.status IS NULL OR o.duration_reopening > 365 OR o.duration_reopening IS NULL)
            AND NOT (
                DATE_FORMAT(o.Period, '%Y-%m') >= DATE_FORMAT(p.firstappeareddate_chainxy, '%Y-%m')
                AND DATE_FORMAT(o.Period, '%Y-%m') <= DATE_FORMAT(DATE_ADD(p.firstappeareddate_chainxy, INTERVAL 1 MONTH), '%Y-%m')
                    )
            AND DATE_FORMAT(o.Period, '%Y') != 2018
            AND NOT EXISTS (
                SELECT 1
                FROM all_active_acquisition a
                WHERE a.Period = '2024-04-01'
                AND o.Period = '2024-05-01'
                AND LOWER(TRIM(o.Address)) = LOWER(TRIM(a.Address))
                AND LOWER(TRIM(o.City)) = LOWER(TRIM(a.City))
                AND LOWER(TRIM(o.State)) = LOWER(TRIM(a.State))
            )
            """

            # Closed stores query
            closed_query = """
            SELECT 
                c.StoreName, 
                c.StoreType, 
                c.ChainName_Coresight, 
                c.ParentName_Coresight, 
                c.Address, 
                c.Address2, 
                c.City, 
                c.MsaName, 
                c.PostalCode, 
                c.State, 
                c.Country, 
                c.Sector_Coresight, 
                c.Period, 
                c.Population, 
                c.UpdateCycle,
                'closed' AS data_from
            FROM (
                SELECT 
                    StoreName, StoreType, ChainName_Coresight, ParentName_Coresight, 
                    Address, Address2, City, MsaName, PostalCode, State, Country,
                    Sector_Coresight, Period, Population, status, UpdateCycle, duration_closing, HashId
                FROM all_closed_py
                UNION ALL
                SELECT 
                    StoreName, StoreType, ChainName_Coresight, ParentName_Coresight, 
                    Address, Address2, City, MsaName, PostalCode, State, Country,
                    Sector_Coresight, Period, Population, status, UpdateCycle, duration_closing, HashId
                FROM all_closed_cy
                UNION ALL
                SELECT 
                    StoreName, StoreType, ChainName_Coresight, ParentName_Coresight, 
                    Address, Address2, City, MsaName, PostalCode, State, Country,
                    Sector_Coresight, Period, Population, status, UpdateCycle, duration_closing, HashId
                FROM all_closed_acquisition
                UNION ALL
                SELECT 
                    StoreName, StoreType, ChainName_Coresight, ParentName_Coresight, 
                    Address, Address2, City, MsaName, PostalCode, State, Country,
                    Sector_Coresight, Period, Population, status, UpdateCycle, duration_closing, HashId
                FROM all_closed_bankruptcy
            ) c
            LEFT JOIN parent_chain_names_data p ON c.ChainName_Coresight = p.ChainName_Coresight
            where (
                (c.ChainName_Coresight IS NULL OR c.ChainName_Coresight NOT IN ('Hoka', 'Sephora', 'Lululemon Athletica','Finish Line','Sunglass Hut',"Carter's",'Marc Jacobs','Bulgari', "Dick's Sporting Goods", 'CVS Pharmacy', 'Circle K', 'Aerie', 'American Eagle Outfitters', 'Aerie', 'American Eagle Outfitters',
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
                OR (c.ChainName_Coresight = "Bulgari" AND COALESCE(c.storetype, '') NOT IN ('Official Retailers','Dept. Store (Ds)','Mall',NULL,'Airport','Street'))
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
                AND DATE_FORMAT(c.Period, '%Y') != 2018
                AND NOT EXISTS (SELECT 1 FROM (SELECT hashid, status, Period FROM all_opened_py union all SELECT hashid, status, Period FROM all_opened_cy) AS o WHERE o.hashid = c.hashid AND o.status = 'reopened' AND o.Period BETWEEN c.Period AND c.Period + 366)
            """

            # Fetch data
            opened_df = pd.read_sql(opened_query, con=conn)
            closed_df = pd.read_sql(closed_query, con=conn)

            # Convert 'Period' to datetime format
            opened_df['Period'] = pd.to_datetime(opened_df['Period'], errors='coerce').dt.date
            closed_df['Period'] = pd.to_datetime(closed_df['Period'], errors='coerce').dt.date

            return opened_df, closed_df

        except Exception as e:
            logging.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()

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
            background-color: #d62e2f; /* Active tab color (dark) */
            color: white; /* Active tab text color */
        }.button-container {
            display: flex;
            flex-direction: column;
            width: 100%;
        }
        </style>
        <div class="button-container">
            <div class="tab-links">
                <a href="/net#store-intelligence-platform" target="_self" class="active">Net Openings</a>
                <a href="/opening#store-intelligence-platform" target="_self"">Store Openings</a>
                <a href="/closing#store-intelligence-platform" target="_self">Store Closures</a>
                <a href="/active#store-intelligence-platform" target="_self">Active Stores</a>
            </div>
        </div>
        """, unsafe_allow_html=True)

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
            Loading Dashboard...
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

    # st.markdown(f"<i>Data available through <b>{latest_ts}</b></i>",unsafe_allow_html=True)
    # st.markdown(
    #         f"""
    #         <p style='text-align: center; color: gray; font-size: small;'>
    #         Disclaimer: Certain data are derived from calculations that use data licensed from third parties, including ChainXY. 
    #         Coresight Research has made substantial efforts to clean the data and identify potential issues. However, changes to retailers' store locators 
    #         may impact database-sourced data. See our 
    #         <a href="{overview_url}" target="_blank">Overview</a> 
    #         document and <a href="/changelogs" target="_self">Data Release Notes</a>
    #         for more details.
    #         </p>
    #         """,
    #         unsafe_allow_html=True,
    #     )
    placeholder = st.empty()
    with placeholder.container():
        st.markdown("<br><br>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([2, 3, 2])
        with col2:
            html(transforming_spinner_html, height=200, scrolling=False)

    # Load data
    opened_df, closed_df = fetch_separate_data()

    st.components.v1.html("""
    <script>
    parent.postMessage('complete-spinner', '*');
    </script>
    """, height=0)

    # placeholder.empty()

    # time.sleep(0.5)


    st.markdown("<div style='margin-top: 10px;'></div>", unsafe_allow_html=True)
    latest_ts = opened_df["Period"].max()
    latest_ts = latest_ts.strftime("%B %Y")

    top_col1, top_col2, top_col3 = st.columns([78, 10, 12]) # Adjust sizes for alignment as you wish

    with top_col1:
        tabs = ["Base Dashboard", "Compare Retailers", "Compare Sectors"]

        # --- Set default if missing
        if "selected_tab" not in user_filters or not user_filters["selected_tab"]:
            user_filters["selected_tab"] = "Base Dashboard"

        # Optionally: also clear tab param so URL never has a value at start!
        if "tab" in st.query_params:
            del st.query_params["tab"]

        selected_tab = st.radio(
            "Analysis",
            tabs, 
            index=tabs.index(user_filters["selected_tab"]),
            horizontal=True,
            label_visibility="collapsed",
            # captions=[
            #     "For Individual Compare",
            #     "For Retailers Compare",
            #     "For Sectors Compare",
            # ],
        )

        # Update user_filters and query params when tab changes
        if selected_tab != user_filters["selected_tab"]:
            user_filters["selected_tab"] = selected_tab
            st.query_params["tab"] = selected_tab


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

        import streamlit as st
        import pandas as pd
        import calendar
        from datetime import date

        def make_json_safe(val):
            """Recursively convert pandas Timestamp to iso string in dictionaries/lists."""
            if isinstance(val, pd.Timestamp):
                return val.isoformat()
            if isinstance(val, list):
                return [make_json_safe(x) for x in val]
            if isinstance(val, dict):
                return {k: make_json_safe(v) for k, v in val.items()}
            return val

        def determine_valid_start_months(start_year, min_year, max_year, min_date, max_date):
            """Returns valid start months for the chosen start year."""
            if start_year == min_year:
                return list(range(min_date.month, 13))  # From first available month to December
            elif start_year == max_year:
                return list(range(1, max_date.month + 1))  # January to last available month
            else:
                return list(range(1, 13))

        def determine_valid_end_years(start_year, min_year, max_year):
            """End year must be >= start year."""
            return [y for y in range(start_year, max_year + 1)]

        def determine_valid_end_months(start_year, start_month, end_year, max_year, max_date):
            """End month must be >= start_month if end_year==start_year, else all months up to max if last year."""
            if end_year == start_year and end_year == max_year:
                return [m for m in range(start_month, max_date.month + 1)]
            elif end_year == start_year:
                return [m for m in range(start_month, 13)]
            elif end_year == max_year:
                return [m for m in range(1, max_date.month + 1)]
            else:
                return list(range(1, 13))


        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            with st.expander("Date Range", expanded=st.session_state["expand_filters"]):
                combined_data = pd.concat([opened_df, closed_df])

                # Ensure Period is datetime
                combined_data['Period'] = pd.to_datetime(combined_data['Period'])
                opened_df['Period'] = pd.to_datetime(opened_df['Period'])
                closed_df['Period'] = pd.to_datetime(closed_df['Period'])

                # Get min and max available dates
                min_available_date = combined_data['Period'].min()
                max_available_date = combined_data['Period'].max()

                # Calculate default 12-month window
                default_end_date = max_available_date
                default_start_date = (default_end_date - pd.DateOffset(months=11)).replace(day=1)

                # Set defaults or use user filters
                start_year = user_filters.get("start_year", default_start_date.year)
                start_month = user_filters.get("start_month", default_start_date.month)
                end_year = user_filters.get("end_year", default_end_date.year)
                end_month = user_filters.get("end_month", default_end_date.month)

                min_year = min_available_date.year
                max_year = max_available_date.year
                all_years = list(range(min_year, max_year + 1))

                # --- START DATE PICKERS ---
                sm_col, sy_col = st.columns([1.3, 1])
                with sy_col:
                    if start_year not in all_years:
                        start_year = all_years[0]
                    start_year = st.selectbox(
                        "Start Year",
                        options=all_years,
                        index=all_years.index(start_year),
                        key="start_year_select"
                    )
                with sm_col:
                    valid_start_months = determine_valid_start_months(start_year, min_year, max_year, min_available_date, max_available_date)
                    if start_month not in valid_start_months:
                        start_month = valid_start_months[0]
                    start_month = st.selectbox(
                        "Start Month",
                        options=valid_start_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_start_months.index(start_month),
                        key="start_month_select"
                    )
                   

                # --- END DATE PICKERS ---
                em_col, ey_col = st.columns([1.3, 1])
                valid_end_years = determine_valid_end_years(start_year, min_year, max_year)
                if end_year not in valid_end_years:
                    end_year = valid_end_years[0]

                with ey_col:
                    end_year = st.selectbox(
                        "End Year",
                        options=valid_end_years,
                        index=valid_end_years.index(end_year),
                        key="end_year_select"
                    )

                valid_end_months = determine_valid_end_months(start_year, start_month, end_year, max_year, max_available_date)
                if end_month not in valid_end_months:
                    end_month = valid_end_months[0]
                with em_col:
                    end_month = st.selectbox(
                        "End Month",
                        options=valid_end_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_end_months.index(end_month),
                        key="end_month_select"
                    )

                # Check if date range has changed
                date_changed = False
                if (user_filters.get("start_year") != start_year or 
                    user_filters.get("start_month") != start_month or
                    user_filters.get("end_year") != end_year or
                    user_filters.get("end_month") != end_month):
                    date_changed = True
                
                # Save back to user_filters
                user_filters["start_year"] = start_year
                user_filters["start_month"] = start_month
                user_filters["end_year"] = end_year
                user_filters["end_month"] = end_month

                # --- Compute Timestamps and bounds ---
                candidate_start_date = pd.Timestamp(date(start_year, start_month, 1))
                end_day = calendar.monthrange(end_year, end_month)[1]
                candidate_end_date = pd.Timestamp(date(end_year, end_month, end_day))

                # Clamp within min/max available in data
                new_start_date = max(candidate_start_date, min_available_date)
                new_end_date = min(candidate_end_date, max_available_date)

                # Filter opened/closed for range
                date_filtered_opened = opened_df[
                    (opened_df['Period'] >= new_start_date) & (opened_df['Period'] <= new_end_date)
                ]
                date_filtered_closed = closed_df[
                    (closed_df['Period'] >= new_start_date) & (closed_df['Period'] <= new_end_date)
                ]

                # Reset downstream filters if date changed
                if date_changed:
                    user_filters["selected_sector_name"] = "All"
                    user_filters["parent_chain_name"] = ["All"]
                    user_filters["selected_chain_name"] = ["All"]
                    user_filters["selected_state_name"] = ["All"]
                    user_filters["selected_msa_name"] = ["All"]
                    user_filters["selected_postal_codes"] = ["All"]
                    save_auth_cookie()
                    st.rerun()

        with col2:
            sector_filtered_opened = date_filtered_opened
            sector_filtered_closed = date_filtered_closed
            
            # Get unique sector names from combined data and sort them
            combined_data = pd.concat([sector_filtered_opened, sector_filtered_closed])
            sector_names = combined_data["Sector_Coresight"].dropna().unique().tolist()
            
            # Separate "Others" from the rest (case-insensitive)
            others_items = [s for s in sector_names if s.lower() in ["other", "others"]]
            regular_items = [s for s in sector_names if s.lower() not in ["other", "others"]]
            
            # Sort regular items alphabetically (case-insensitive)
            regular_items = sorted(regular_items, key=lambda x: x.lower())
            sector_names = ["All"] + regular_items + others_items

            # --- COOKIFIED FILTER STATE ---
            current_sector = user_filters.get("selected_sector_name", "All")
            if current_sector not in sector_names:
                current_sector = "All"
                user_filters["selected_sector_name"] = "All"
                # Handle json serializability for cookie
                safe_cookie = make_json_safe(auth_cookie)
                # cookie_controller.set(
                #     "auth_data",
                #     json.dumps(auth_cookie),                    # must be a string
                #     expires=datetime.utcnow() + timedelta(days=30),
                #     path="/",
                #     domain="localhost",
                # )
                save_auth_cookie()
                st.rerun()

            current_index = sector_names.index(current_sector) if current_sector in sector_names else 0
            with st.expander("Sector", expanded=st.session_state["expand_filters"]):
                selected_sector_name = st.selectbox(
                    "Select Sector",
                    sector_names,
                    index=current_index,
                    key="sector_selectbox"
                )
                logging.info(f"User selected sector: {selected_sector_name}")

                if selected_sector_name != current_sector:
                    user_filters["selected_sector_name"] = selected_sector_name
                    user_filters["parent_chain_name"] = ["All"]
                    user_filters["selected_chain_name"] = ["All"]
                    user_filters["selected_state_name"] = ["All"]
                    user_filters["selected_msa_name"] = ["All"]
                    user_filters["selected_postal_codes"] = ["All"]
                    # Handle json serializability for cookie
                    safe_cookie = make_json_safe(auth_cookie)
                    # cookie_controller.set(
                    #     "auth_data",
                    #     json.dumps(auth_cookie),                    # must be a string
                    #     expires=datetime.utcnow() + timedelta(days=30),
                    #     path="/",
                    #     domain="localhost",
                    # )
                    save_auth_cookie()
                    st.rerun()

            # Apply sector filter to the opened and closed data
            if "All" not in selected_sector_name:
                sector_filtered_opened = sector_filtered_opened[
                    sector_filtered_opened["Sector_Coresight"].isin([selected_sector_name])
                ]
                sector_filtered_closed = sector_filtered_closed[
                    sector_filtered_closed["Sector_Coresight"].isin([selected_sector_name])
                ]

            logging.info("--- SECTOR SECTION END ---")

        with col3:
            retailer_filtered_opened = sector_filtered_opened
            retailer_filtered_closed = sector_filtered_closed

            retailer_filtered_opened['ParentName_Coresight'] = retailer_filtered_opened['ParentName_Coresight'].where(
                pd.notna(retailer_filtered_opened['ParentName_Coresight']), "No Parent Retailer"
            )

            retailer_filtered_closed['ParentName_Coresight'] = retailer_filtered_closed['ParentName_Coresight'].where(
                pd.notna(retailer_filtered_closed['ParentName_Coresight']), "No Parent Retailer"
            )

            parent_names = pd.concat([
                retailer_filtered_opened['ParentName_Coresight'],
                retailer_filtered_closed['ParentName_Coresight']
            ]).unique().tolist()

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

            with st.expander("Retailers", expanded=st.session_state["expand_filters"]):
                parent_chain_name = st.multiselect(
                    "Select Company", 
                    parent_names, 
                    default=current_parent,
                    max_selections=10, 
                    help="Parent company that owns one or more store banners.", 
                    key="parent_multiselect_1"
                )

                if len(parent_chain_name) > 1 and "All" in parent_chain_name:
                    if parent_chain_name[-1] == "All":
                        parent_chain_name = ["All"]
                    else:
                        parent_chain_name = [p for p in parent_chain_name if p != "All"]
                elif not parent_chain_name:
                    parent_chain_name = ["All"]

                if parent_chain_name != current_parent:
                    user_filters["parent_chain_name"] = parent_chain_name
                    user_filters["selected_state_name"] = ["All"]
                    user_filters["selected_msa_name"] = ["All"]
                    user_filters["selected_postal_codes"] = ["All"]
                    # Handle json serializability for cookie
                    safe_cookie = make_json_safe(auth_cookie)
                    # cookie_controller.set(
                    #     "auth_data",
                    #     json.dumps(auth_cookie),                    # must be a string
                    #     expires=datetime.utcnow() + timedelta(days=30),
                    #     path="/",
                    #     domain="localhost",
                    # )
                    save_auth_cookie()
                    st.rerun()

                st.markdown(
                    """
                    <div style='text-align: center; font-size: 0.9rem; font-weight: 500; margin-top: -0.4rem; margin-bottom: -0.2rem; color: #444;'>or</div>
                    """,
                    unsafe_allow_html=True
                )

                # Filter by parent_chain_name
                if "All" not in parent_chain_name:
                    retailer_filtered_opened = retailer_filtered_opened[retailer_filtered_opened["ParentName_Coresight"].isin(parent_chain_name)]
                    retailer_filtered_closed = retailer_filtered_closed[retailer_filtered_closed["ParentName_Coresight"].isin(parent_chain_name)]

                chain_names = pd.concat([
                    retailer_filtered_opened['ChainName_Coresight'],
                    retailer_filtered_closed['ChainName_Coresight']
                ]).unique().tolist()

                # Remove None values safely before sorting
                chain_names = [c for c in chain_names if c is not None]
                chain_names.sort()
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
                    user_filters["selected_postal_codes"] = ["All"]
                    # Handle json serializability for cookie
                    safe_cookie = make_json_safe(auth_cookie)
                    # cookie_controller.set(
                    #     "auth_data",
                    #     json.dumps(auth_cookie),                    # must be a string
                    #     expires=datetime.utcnow() + timedelta(days=30),
                    #     path="/",
                    #     domain="localhost",
                    # )
                    save_auth_cookie()
                    st.rerun()

                # Finalize retailer_filtered_data
                if "All" not in selected_chain_name:
                    retailer_filtered_opened = retailer_filtered_opened[retailer_filtered_opened["ChainName_Coresight"].isin(selected_chain_name)]
                    retailer_filtered_closed = retailer_filtered_closed[retailer_filtered_closed["ChainName_Coresight"].isin(selected_chain_name)]

            logging.info("--- RETAILERS SECTION END ---")

        with col4:
            logging.info("--- LOCATION SECTION START ---")

            with st.expander("Location", expanded=st.session_state["expand_filters"]):
                # Start with data filtered by retailers
                location_filtered_opened = retailer_filtered_opened
                location_filtered_closed = retailer_filtered_closed
                
                # -----------------
                # STATE FILTER
                # -----------------
                states_opened = location_filtered_opened["State"].dropna().unique().tolist()
                states_closed = location_filtered_closed["State"].dropna().unique().tolist()
                states = sorted(set(states_opened).union(states_closed))
                states = ["All"] + states

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

                # Ensure "All" is not mixed with specific states
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
                    user_filters["selected_postal_codes"] = ["All"]
                    # Handle json serializability for cookie
                    safe_cookie = make_json_safe(auth_cookie)
                    # cookie_controller.set(
                    #     "auth_data",
                    #     json.dumps(auth_cookie),                    # must be a string
                    #     expires=datetime.utcnow() + timedelta(days=30),
                    #     path="/",
                    #     domain="localhost",
                    # )
                    save_auth_cookie()
                    st.rerun()

                # Filter data by selected states
                if "All" not in selected_state_name:
                    location_filtered_opened = location_filtered_opened[location_filtered_opened['State'].isin(selected_state_name)]
                    location_filtered_closed = location_filtered_closed[location_filtered_closed['State'].isin(selected_state_name)]

                # -----------------
                # MSA FILTER
                # -----------------

                
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

                # _, center_col, _ = st.columns([20, 80, 10])
                # with center_col:
                    # saved_location_type = user_filters.get("location_type_base_net", "MSA")
                    # location_type = st.radio(
                    #     "Filter by location",
                    #     ["MSA", "Zip Code"],
                    #     horizontal=True,
                    #     index=0 if saved_location_type == "MSA" else 1,
                    #     key="location_type_base_net",
                    #     label_visibility="collapsed"
                    # )
                    # if location_type != saved_location_type:
                    #     user_filters["location_type_base_net"] = location_type
                    #     safe_cookie = make_json_safe(auth_cookie)
                    #     # cookie_controller.set(
                    #     #     "auth_data",
                    #     #     json.dumps(auth_cookie),                    # must be a string
                    #     #     expires=datetime.utcnow() + timedelta(days=30),
                    #     #     path="/",
                    #     #     domain="localhost",
                    #     # )
                    #     save_auth_cookie()
                    #     st.rerun()
                location_type = "MSA"
                # -----------------
                # MSA OR ZIP FILTER
                # -----------------
                if location_type == "MSA":
                    msa_opened = location_filtered_opened['MsaName'].dropna().unique().tolist()
                    msa_closed = location_filtered_closed['MsaName'].dropna().unique().tolist()
                    filter_values = sorted(set(msa_opened).union(msa_closed))
                    filter_values = ["All"] + filter_values

                    current_filter = user_filters.get("selected_msa_name", ["All"])
                    multiselect_label = "Select Metropolitan Statistical Area (MSA)"
                    filter_key = "multiselect_msa"
                    filter_column = "MsaName"

                else:  # ZIP CODE selected
                    zip_opened = location_filtered_opened['PostalCode'].dropna().unique().tolist()
                    zip_closed = location_filtered_closed['PostalCode'].dropna().unique().tolist()
                    filter_values = sorted(
                        {z for z in zip_opened if str(z) != "0"}.union({z for z in zip_closed if str(z) != "0"})
                    )
                    filter_values = ["All"] + filter_values

                    current_filter = user_filters.get("selected_zip_code", ["All"])
                    multiselect_label = "Select Zip Code"
                    filter_key = "multiselect_zip_base_net"
                    filter_column = "PostalCode"

                # -----------------
                # MULTISELECT FILTER
                # -----------------
                if "All" in current_filter:
                    current_filter = ["All"]
                else:
                    valid_filter = [f for f in current_filter if f in filter_values]
                    current_filter = valid_filter if valid_filter else ["All"]

                selected_filter = st.multiselect(
                    multiselect_label,
                    filter_values,
                    default=current_filter,
                    key=filter_key
                )
                logging.info(f"User selected {location_type}: {selected_filter}")

                # Handle "All"
                if len(selected_filter) > 1 and "All" in selected_filter:
                    if selected_filter[-1] == "All":
                        selected_filter = ["All"]
                    else:
                        selected_filter = [f for f in selected_filter if f != "All"]
                elif not selected_filter:
                    selected_filter = ["All"]

                # Update user_filters
                if location_type == "MSA":
                    if selected_filter != current_filter:
                        user_filters["selected_msa_name"] = selected_filter
                        user_filters["selected_postal_codes"] = ["All"]
                        safe_cookie = make_json_safe(auth_cookie)
                        # cookie_controller.set(
                        #     "auth_data",
                        #     json.dumps(auth_cookie),                    # must be a string
                        #     expires=datetime.utcnow() + timedelta(days=30),
                        #     path="/",
                        #     domain="localhost",
                        # )
                        save_auth_cookie()
                        st.rerun()
                else:
                    if selected_filter != current_filter:
                        user_filters["selected_zip_code"] = selected_filter
                        safe_cookie = make_json_safe(auth_cookie)
                        # cookie_controller.set(
                        #     "auth_data",
                        #     json.dumps(auth_cookie),                    # must be a string
                        #     expires=datetime.utcnow() + timedelta(days=30),
                        #     path="/",
                        #     domain="localhost",
                        # )
                        save_auth_cookie()
                        st.rerun()
                if "All" not in selected_filter:
                    location_filtered_opened = location_filtered_opened[location_filtered_opened[filter_column].isin(selected_filter)]
                    location_filtered_closed = location_filtered_closed[location_filtered_closed[filter_column].isin(selected_filter)]
                                # -----------------
                # POSTAL CODE FILTER
                # -----------------
                st.markdown("<div style='margin-top: 20px;'></div>", unsafe_allow_html=True)
                
                # Get unique postal codes from filtered data
                postal_codes_opened = location_filtered_opened['PostalCode'].dropna().astype(str).unique().tolist()
                postal_codes_closed = location_filtered_closed['PostalCode'].dropna().astype(str).unique().tolist()
                all_postal_codes = sorted(list(set(postal_codes_opened + [p for p in postal_codes_closed if p != "0"])))
                all_postal_codes = ["All"] + all_postal_codes
                
                # Get current postal code selection
                current_postal_codes = user_filters.get("selected_postal_codes", ["All"])
                if "All" in current_postal_codes:
                    current_postal_codes = ["All"]
                else:
                    valid_postal_codes = [p for p in current_postal_codes if p in all_postal_codes]
                    current_postal_codes = valid_postal_codes if valid_postal_codes else ["All"]
                
                # Create the postal code multiselect
                selected_postal_codes = st.multiselect(
                    "Select Zip Code",
                    all_postal_codes,
                    default=current_postal_codes,
                    help="U.S. postal area where the store is located.",
                    key="multiselect_postal_code"
                )
                
                # Handle "All" selection logic
                if len(selected_postal_codes) > 1 and "All" in selected_postal_codes:
                    if selected_postal_codes[-1] == "All":
                        selected_postal_codes = ["All"]
                    else:
                        selected_postal_codes = [p for p in selected_postal_codes if p != "All"]
                elif not selected_postal_codes:
                    selected_postal_codes = ["All"]
                
                # Update user_filters if selection changed
                if selected_postal_codes != current_postal_codes:
                    user_filters["selected_postal_codes"] = selected_postal_codes
                    save_auth_cookie()
                    st.rerun()
                
                # Apply postal code filter if not "All"
                if "All" not in selected_postal_codes:
                    location_filtered_opened = location_filtered_opened[location_filtered_opened['PostalCode'].astype(str).isin(selected_postal_codes)]
                    location_filtered_closed = location_filtered_closed[location_filtered_closed['PostalCode'].astype(str).isin(selected_postal_codes)]

            logging.info("--- LOCATION SECTION END ---")

        # Combine filtered opened and closed data
        location_filtered_opened['data_from'] = 'opened'
        location_filtered_closed['data_from'] = 'closed'
        filtered_data = pd.concat([location_filtered_opened, location_filtered_closed])

        # Calculate openings and closings
        opened_count = location_filtered_opened['ChainName_Coresight'].count()
        closed_count = location_filtered_closed['ChainName_Coresight'].count()

        # Calculate net openings
        net_openings = opened_count - closed_count

        col1, col2, col3, col4, col5, col6 = st.columns([1, 1, 1, 1, 1, 1])

        # Display Net Openings
        with col1:
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{net_openings:,}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>Net Opened Stores</h6>
            """, unsafe_allow_html=True)

        with col2:
            total_retailers = filtered_data['ParentName_Coresight'].nunique()
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{total_retailers}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>Total Affected Banners</h6>
            """, unsafe_allow_html=True)

        with col3:
            total_categories = filtered_data['Sector_Coresight'].nunique()
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{total_categories}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>Total Affected Sectors</h6>
            """, unsafe_allow_html=True)

        num_months = (new_end_date.year - new_start_date.year) * 12 + (new_end_date.month - new_start_date.month) + 1

        # Previous period range -- shift back by `num_months`
        prev_end_date = new_start_date - pd.DateOffset(days=1)
        prev_start_date = prev_end_date - relativedelta(months=num_months-1)

        # Filter opened and closed data for previous period
        prev_period_opened = opened_df[
            (opened_df['Period'] >= prev_start_date) & (opened_df['Period'] <= prev_end_date)
        ]
        prev_period_closed = closed_df[
            (closed_df['Period'] >= prev_start_date) & (closed_df['Period'] <= prev_end_date)
        ]

        # Apply sector
        if "All" not in selected_sector_name:
            prev_period_opened = prev_period_opened[
                prev_period_opened["Sector_Coresight"].isin([selected_sector_name])
            ]
            prev_period_closed = prev_period_closed[
                prev_period_closed["Sector_Coresight"].isin([selected_sector_name])
            ]

        # Apply parent_chain_name
        if "All" not in parent_chain_name:
            prev_period_opened = prev_period_opened[
                prev_period_opened["ParentName_Coresight"].isin(parent_chain_name)
            ]
            prev_period_closed = prev_period_closed[
                prev_period_closed["ParentName_Coresight"].isin(parent_chain_name)
            ]

        # Apply chain_name
        if "All" not in selected_chain_name:
            prev_period_opened = prev_period_opened[
                prev_period_opened["ChainName_Coresight"].isin(selected_chain_name)
            ]
            prev_period_closed = prev_period_closed[
                prev_period_closed["ChainName_Coresight"].isin(selected_chain_name)
            ]

        # Apply state
        if "All" not in selected_state_name:
            prev_period_opened = prev_period_opened[
                prev_period_opened['State'].isin(selected_state_name)
            ]
            prev_period_closed = prev_period_closed[
                prev_period_closed['State'].isin(selected_state_name)
            ]

        if "All" not in selected_filter:
            prev_period_opened = prev_period_opened[
                prev_period_opened[filter_column].isin(selected_filter)
            ]
            prev_period_closed = prev_period_closed[
                prev_period_closed[filter_column].isin(selected_filter)
            ]

        # Calculate net openings previous period
        prev_opened_count = prev_period_opened['ChainName_Coresight'].count()
        prev_closed_count = prev_period_closed['ChainName_Coresight'].count()
        prev_net_openings = prev_opened_count - prev_closed_count

        # Calculate percent change
        if prev_net_openings == 0:
            pct_change = None
        else:
            pct_change = ((net_openings - prev_net_openings) / abs(prev_net_openings)) * 100

        with col4:
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{prev_net_openings:,}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>Previous Period Net Openings</h6>
            """, unsafe_allow_html=True)

        with col5:
            pct_display = "N/A" if pct_change is None else f"{pct_change:.1f}%"
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{pct_display}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>% Change vs Previous Period</h6>
            """, unsafe_allow_html=True)

        total_population = filtered_data['Population'].sum()
        opened_stores_per_10k = (opened_count / total_population) * 10000 if total_population > 0 else None
        with col6:
            display_value = f"{opened_stores_per_10k:.3f}" if opened_stores_per_10k else "N/A"
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{display_value}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>Opened Stores per 10,000 people</h6>
            """, unsafe_allow_html=True)

        st.markdown("""<style>.custom-hr {border: none;border-top: 1px solid #808080; width: 100%; margin: -10px; padding: 0; margin-left: auto; margin-right: auto;
        }
        </style>
        """,
        unsafe_allow_html=True
        )

        st.markdown('<hr class="custom-hr">', unsafe_allow_html=True)

        # Convert Period to datetime and extract year/month
        filtered_data['Period'] = pd.to_datetime(filtered_data['Period'], errors='coerce')
        filtered_data['year_month'] = filtered_data['Period'].dt.to_period('M').astype(str)

        # Tag
        filtered_data['opened'] = (filtered_data['data_from'] == 'opened').astype(int)
        filtered_data['closed'] = (filtered_data['data_from'] == 'closed').astype(int)

        month_agg = filtered_data.groupby('year_month').agg(
            Openings=('opened', 'sum'),
            Closures=('closed', 'sum')
        ).reset_index()
        month_agg['Net Openings'] = month_agg['Openings'] - month_agg['Closures']
        # Convert to datetime first
        month_agg['year_month_dt'] = pd.to_datetime(month_agg['year_month'], format='%Y-%m')
        # Format as "Jan 2024"
        month_agg['year_month_label'] = month_agg['year_month_dt'].dt.strftime('%b %Y')
        col1, col2 = st.columns([0.7, 0.3])

        with col1:
            df_plot = month_agg.copy()
            df_plot['Closures_plot'] = -df_plot['Closures']  # negative for plotting
            x_vals = df_plot['year_month_label']

            fig = go.Figure()
            # Closures down (red)
            fig.add_trace(go.Bar(
                x=x_vals, y=df_plot['Closures_plot'],
                name="Closures",
                marker_color="#d62e2f",
                text=[f"{-v:,}" for v in df_plot['Closures_plot']],
                textposition="outside",
                textfont=dict(size=12, color="black"),
                hovertemplate="Closures: %{y:,}"
            ))
            # Openings up (green)
            fig.add_trace(go.Bar(
                x=x_vals, y=df_plot['Openings'],
                name="Openings",
                marker_color="#A3C0CE",
                text=[f"{v:,}" for v in df_plot['Openings']],
                textposition="outside",
                textfont=dict(size=12, color="black"),
                hovertemplate="Openings: %{y:,}"
            ))
            # Net line
            fig.add_trace(go.Scatter(
                x=x_vals, y=df_plot['Net Openings'],
                name="Net Store Openings", mode='lines+markers+text',
                line=dict(color='#2D2A29', width=2),
                marker=dict(size=7, color="#000000"),
                text=[f"{v:,}" for v in df_plot['Net Openings']],
                textposition="top center",
                textfont=dict(size=13, color="black"),
                hovertemplate="Net: %{y:,}"
            ))

            fig.update_layout(
                barmode='relative',
                yaxis_title="Number of Stores",
                height=500, plot_bgcolor="white", hovermode="x unified", bargap=0.25,
                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1,
                            font=dict(size=12, color="black")),
                margin=dict(l=40, r=40, t=60, b=40)
            )
            fig.update_xaxes(
                title_text="Month",
                # tickangle=-45,
                # tickfont=dict(size=12, color="black"),
                # title_font=dict(size=14, color="black", family="Arial"),
                type="category",
                categoryorder="array",
                categoryarray=x_vals.tolist(),
                tickmode='array',
                tickvals=x_vals.tolist(),
            )
            fig.update_yaxes(
                tickformat=',', zeroline=True, zerolinewidth=1.5, gridcolor='lightgray',
                # title_font=dict(size=14, color="black", family="Arial"),
                # tickfont=dict(size=12, color="black")
            )
            st.markdown(
                "<h4 style='font-size: 20px;text-align: center; margin-top: 0; margin-bottom: 16px;'>Monthly Store Openings & Closures with Net Store Openings</h4>",
                unsafe_allow_html=True
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Compute net openings per banner
            banner_agg_opened = filtered_data[filtered_data.data_from=='opened']['ChainName_Coresight'].value_counts()
            banner_agg_closed = filtered_data[filtered_data.data_from=='closed']['ChainName_Coresight'].value_counts()

            banner_net = banner_agg_opened.subtract(banner_agg_closed, fill_value=0)
            top_banners = banner_net.sort_values(ascending=False).head(15)

            if not top_banners.empty:
                # Calculate dynamic date period
                start_date = filtered_data['Period'].min()
                end_date = filtered_data['Period'].max()
                start_date_str = start_date.strftime('%b %Y') if not pd.isna(start_date) else "N/A"
                end_date_str = end_date.strftime('%b %Y') if not pd.isna(end_date) else "N/A"
                date_period = f"{start_date_str} to {end_date_str}"
                
                # Calculate metadata for the entire dataset
                num_states = filtered_data['State'].nunique()
                num_msa = filtered_data['MsaName'].nunique()
                
                fig_opened_bar = go.Figure()
                
                fig_opened_bar.add_trace(go.Bar(
                    x=top_banners.index,
                    y=top_banners.values,
                    marker_color='#2D2A29',
                    text=[f"{x:,}" for x in top_banners.values],
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"  # Banner name
                        "<span style='color:#2D2A29'>●</span> "
                        "<b>Net Openings:</b> %{y:,}<br><br>"
                        "<span style='color:#2D2A29'>•</span> "
                        f"<span style='color:black'>Date Period: {date_period}</span><br>"
                        "<span style='color:#2D2A29'>•</span> "
                        f"<span style='color:black'>Number of States: {num_states:,}</span><br>"
                        "<span style='color:#2D2A29'>•</span> "
                        f"<span style='color:black'>Number of MSA: {num_msa:,}</span><br>"
                        "<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor="#2D2A29",
                        font=dict(size=13, color="black", family="Arial")
                    )
                ))
                
                fig_opened_bar.update_layout(
                    xaxis_title="Banner Name",
                    yaxis_title="Net Openings",
                    yaxis=dict(
                        autorange=True,
                        showline=True,
                        zeroline=False
                    ),
                    xaxis=dict(
                        showline=True,
                        zeroline=False,
                        tickangle=45 if len(top_banners) > 5 else 0
                    ),
                    height=500,
                    margin=dict(l=20, r=20, t=40, b=20),
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )
                
                # Additional label rotation for many banners
                if len(top_banners) > 8:
                    fig_opened_bar.update_layout(
                        xaxis=dict(tickangle=60),
                        margin=dict(b=60)
                    )
                
                num_banners = min(15, top_banners.shape[0])
                st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {num_banners} Banners Net Openings</h4>", unsafe_allow_html=True)
                st.plotly_chart(fig_opened_bar, use_container_width=True)
            else:
                st.warning("No net openings data available for banners.")


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

        import plotly.express as px
        import plotly.graph_objects as go

        # 1. Which states to hide
        HIDE_STATE_LABELS = {'MD', 'DC', 'DE', 'NJ', 'CT', 'RI', 'MA', 'VT', 'NH'}
        # 1. Map full state names to abbreviations
        filtered_data['State'] = filtered_data['State'].map(state_abbreviations)

        import plotly.express as px
        import plotly.graph_objects as go
        import math

        # -------- 1. Calculate Net Openings By State --------
        opened_by_state = filtered_data[filtered_data['data_from'] == 'opened']['State'].value_counts()
        closed_by_state = filtered_data[filtered_data['data_from'] == 'closed']['State'].value_counts()
        net_by_state = opened_by_state.subtract(closed_by_state, fill_value=0).astype(int).reset_index()
        net_by_state.columns = ['State', 'Net Openings']
        net_by_state = net_by_state.dropna(subset=['State'])

        # -------- 2. Prepare State Info for Plotting --------
        abbreviation_to_full = {v: k for k, v in state_abbreviations.items()}
        net_by_state['State_Full'] = net_by_state['State'].map(abbreviation_to_full)
        net_by_state['lon'] = net_by_state['State'].map(lambda abbr: state_centers.get(abbr, (None, None))[0])
        net_by_state['lat'] = net_by_state['State'].map(lambda abbr: state_centers.get(abbr, (None, None))[1])

        # -------- 3. Shade Color Scale --------
        def hex_to_rgb(hex_color):
            h = hex_color.lstrip('#')
            return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))

        def rgb_to_hex(rgb_tuple):
            return "#{:02x}{:02x}{:02x}".format(*rgb_tuple)

        def generate_shades(hex_color, n=7, lightest="#F4F2F1"):
            base = hex_to_rgb(hex_color)
            light = hex_to_rgb(lightest)
            shades = []
            for i in range(n):
                ratio = i / (n - 1)
                shade = tuple(
                    int(light[j] + ratio * (base[j] - light[j]))
                    for j in range(3)
                )
                shades.append(rgb_to_hex(shade))
            return shades

        color_scale_2d2a29 = generate_shades("#2D2A29", n=20, lightest="#F4F2F1")

        # -------- 4. Detect/Prune Overlapping State Labels --------
        def calculate_distance(lat1, lon1, lat2, lon2):
            return math.sqrt((lat2 - lat1) ** 2 + (lon2 - lon1) ** 2)

        def detect_overlapping_states_by_count(map_df, state_centers, min_distance=1.5):
            states_to_exclude = set()
            # Known problematic clusters
            northeast_clusters = [
                ['CT', 'RI', 'MA'],
                ['VT', 'NH', 'ME'],
                ['NJ', 'DE', 'MD'],
            ]
            always_exclude = ['ME', 'MA', 'NJ', 'DC', 'WV']
            for state in always_exclude:
                if state in map_df['State'].values:
                    states_to_exclude.add(state)
            for cluster in northeast_clusters:
                cluster_states = [state for state in cluster if state in map_df['State'].values]
                if len(cluster_states) > 1:
                    cluster_data = map_df[map_df['State'].isin(cluster_states)]
                    max_state = cluster_data.loc[cluster_data['Net Openings'].idxmax(), 'State']
                    for state in cluster_states:
                        if state != max_state:
                            states_to_exclude.add(state)
            # Add additional exclusion if necessary
            remaining_states = [state for state in map_df['State'] if state not in states_to_exclude]
            for i, state1 in enumerate(remaining_states):
                if state1 in states_to_exclude or state1 not in state_centers:
                    continue
                for state2 in remaining_states[i+1:]:
                    if state2 in states_to_exclude or state2 not in state_centers:
                        continue
                    distance = calculate_distance(
                        state_centers[state1][1], state_centers[state1][0],
                        state_centers[state2][1], state_centers[state2][0]
                    )
                    if distance < min_distance:
                        state1_value = map_df[map_df['State'] == state1]['Net Openings'].iloc[0]
                        state2_value = map_df[map_df['State'] == state2]['Net Openings'].iloc[0]
                        if state1_value < state2_value:
                            states_to_exclude.add(state1)
                        else:
                            states_to_exclude.add(state2)
            return list(states_to_exclude)

        excluded_states = detect_overlapping_states_by_count(net_by_state, state_centers)
        states_to_annotate = net_by_state[~net_by_state['State'].isin(excluded_states)]

        colors = [
            "black" if val < -150 else "white"
            for state, val in zip(states_to_annotate['State'], states_to_annotate['Net Openings'])
            if state in state_centers
        ]

        # -------- 5. Map Plot + Labels --------
        state_metadata = {}
        for state in net_by_state['State']:
            state_data = filtered_data[filtered_data['State'] == state]
            state_metadata[state] = {
                'num_sectors': state_data['Sector_Coresight'].nunique(),
                'num_banners': state_data['ChainName_Coresight'].nunique(),
                'num_msa': state_data['MsaName'].nunique()
            }

        # Calculate overall date range
        start_date = filtered_data['Period'].min().strftime('%B %Y')
        end_date = filtered_data['Period'].max().strftime('%B %Y')
        date_period = f"{start_date} to {end_date}"

        fig_state = px.choropleth(
            net_by_state,
            locations="State",
            locationmode="USA-states",
            color="Net Openings",
            color_continuous_scale=color_scale_2d2a29,
            scope="usa",
            hover_name="State_Full",
            hover_data={"Net Openings": True, "State": False, "State_Full": False}
        )

        # Customize hover template with state-specific metadata
        fig_state.update_traces(
            hovertemplate=(
                "<b>%{hovertext}</b><br>"  # State name
                "<span style='color:#2D2A29'>●</span> "
                "<b>Net Openings:</b> %{z:,}<br><br>"  # Net openings count
                "<span style='color:#2D2A29'>•</span> "
                f"<span style='color:black'>Date Period: {date_period}</span><br>"
                "<span style='color:#2D2A29'>•</span> "
                "<span style='color:black'>Number of Sectors: %{customdata[0]:,}</span><br>"
                "<span style='color:#2D2A29'>•</span> "
                "<span style='color:black'>Number of Banners: %{customdata[1]:,}</span><br>"
                "<span style='color:#2D2A29'>•</span> "
                "<span style='color:black'>Number of MSA: %{customdata[2]:,}</span><br>"
                "<extra></extra>"
            ),
            customdata=[[
                state_metadata[state]['num_sectors'],
                state_metadata[state]['num_banners'],
                state_metadata[state]['num_msa']
            ] for state in net_by_state['State']],
            hoverlabel=dict(
                bgcolor="white",
                bordercolor="#2D2A29",
                font=dict(size=13, color="black", family="Arial")
            )
        )
        # Annotate with both abbreviation and value, just like your posted map
        fig_state.add_trace(go.Scattergeo(
            locationmode='USA-states',
            lon=[state_centers[state][0] for state in states_to_annotate['State'] if state in state_centers],
            lat=[state_centers[state][1] for state in states_to_annotate['State'] if state in state_centers],
            text=[
                f"{state}<br>{count:,}"
                for state, count in zip(states_to_annotate['State'], states_to_annotate['Net Openings'])
                if state in state_centers
            ],
            mode='text',
            showlegend=False,
            textfont=dict(size=11, color=colors),
            hoverinfo="skip"
        ))

        fig_state.update_layout(
            margin=dict(l=0, r=0, t=30, b=0),
            geo=dict(
                center=dict(lat=37.5, lon=-95),
                showlakes=True,
                lakecolor='rgb(255,255,255)'),
            coloraxis_colorbar=dict(title="Net<br>Openings")
        )

        # Aggregate net openings by city
        opened_by_city = filtered_data[filtered_data['data_from'] == 'opened']['City'].value_counts()
        closed_by_city = filtered_data[filtered_data['data_from'] == 'closed']['City'].value_counts()
        net_by_city = opened_by_city.subtract(closed_by_city, fill_value=0).astype(int)
        top15_cities = net_by_city.sort_values(ascending=False).head(15)

        # Calculate metadata for each city
        city_metadata = {}
        for city in top15_cities.index:
            opened_city_data = filtered_data[(filtered_data['data_from'] == 'opened') & (filtered_data['City'] == city)]
            closed_city_data = filtered_data[(filtered_data['data_from'] == 'closed') & (filtered_data['City'] == city)]
            
            # Combine opened and closed data for this city
            city_data = pd.concat([opened_city_data, closed_city_data])
            
            city_metadata[city] = {
                'num_sectors': city_data['Sector_Coresight'].nunique(),
                'num_banners': city_data['ChainName_Coresight'].nunique(),
                'num_states': city_data['State'].nunique(),
                'num_msa': city_data['MsaName'].nunique()
            }

        # Calculate overall date range
        start_date = filtered_data['Period'].min().strftime('%B %Y')
        end_date = filtered_data['Period'].max().strftime('%B %Y')

        df_top15_cities = top15_cities.reset_index()
        df_top15_cities.columns = ['City', 'Net Openings']

        # Create the figure using graph_objects for better hover control
        fig_cities = go.Figure()

        fig_cities.add_trace(go.Bar(
            x=df_top15_cities['City'],
            y=df_top15_cities['Net Openings'],
            marker_color="#2D2A29",
            text=df_top15_cities['Net Openings'].apply(lambda x: f"{x:,}"),
            textposition='outside',
            hovertemplate=(
                "<b>%{x}</b><br>"  # City name
                "<span style='color:#2D2A29'>●</span> "
                "<b>Net Openings:</b> %{y:,}<br><br>"  # Formatted with commas
                "<span style='color:#2D2A29'>•</span> "
                "<span style='color:black'>Date Period: " + f"{start_date} to {end_date}</span><br>"
                "<span style='color:#2D2A29'>•</span> "
                "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                "<span style='color:#2D2A29'>•</span> "
                "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                "<span style='color:#2D2A29'>•</span> "
                "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                "<span style='color:#2D2A29'>•</span> "
                "<span style='color:black'>Number of MSA: %{customdata[3]}</span><br>"
                "<extra></extra>"
            ),
            customdata=[[
                city_metadata[city]['num_sectors'],
                city_metadata[city]['num_banners'],
                city_metadata[city]['num_states'],
                city_metadata[city]['num_msa']
            ] for city in df_top15_cities['City']],
            hoverlabel=dict(
                bgcolor="white",
                bordercolor='#2D2A29',
                font=dict(size=13, color="black", family="Arial")
            )
        ))

        fig_cities.update_layout(
            yaxis=dict(title='Net Openings', autorange=True),
            xaxis=dict(title='City', tickangle=45 if len(top15_cities) > 5 else 0),
            height=500,
            margin=dict(l=20, r=20, t=40, b=60),  # Increased bottom margin for rotated labels
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )

        # Adjust for many cities
        if len(top15_cities) > 8:
            fig_cities.update_layout(
                xaxis=dict(tickangle=60),
                margin=dict(b=80)  # Extra bottom margin for highly rotated labels
            )

        col1, col2 = st.columns([7, 3])
        with col1:
            st.markdown(
                "<h4 style='font-size: 20px;text-align: center; margin-top: 0; margin-bottom: 16px;'>Net Store Openings by State</h4>",
                unsafe_allow_html=True
            )
            st.plotly_chart(fig_state, use_container_width=True, key="state_map_plot")
        with col2:
            st.markdown(
                f"<h4 style='font-size: 20px;text-align: center; margin-top: 0; margin-bottom: 16px;'>Top {top15_cities.shape[0]} Cities Net Openings</h4>",
                unsafe_allow_html=True
            )
            st.plotly_chart(fig_cities, use_container_width=True, key="city_bar_plot")
        
        
        col_map, col_bar = st.columns([7, 3])

        import math

        def calculate_distance(lat1, lon1, lat2, lon2):
            return math.sqrt((lat2 - lat1) ** 2 + (lon2 - lon1) ** 2)

        def detect_overlapping_states_per_capita(map_df, state_centers, min_distance=1.5):
            # Use same cluster overlap logic as before, but sort by Net_Stores_per_Million
            states_to_exclude = set()
            northeast_clusters = [
                ['CT', 'RI', 'MA'],
                ['VT', 'NH', 'ME'],
                ['NJ', 'DE', 'MD'],
            ]
            always_exclude = ['ME', 'MA', 'DC', 'WV']
            for state in always_exclude:
                if state in map_df['State'].values:
                    states_to_exclude.add(state)
            for cluster in northeast_clusters:
                cluster_states = [state for state in cluster if state in map_df['State'].values]
                if len(cluster_states) > 1:
                    cluster_data = map_df[map_df['State'].isin(cluster_states)]
                    max_state = cluster_data.loc[cluster_data['Net_Stores_per_Million'].idxmax(), 'State']
                    for state in cluster_states:
                        if state != max_state:
                            states_to_exclude.add(state)
            remaining_states = [state for state in map_df['State'] if state not in states_to_exclude]
            for i, state1 in enumerate(remaining_states):
                if state1 in states_to_exclude or state1 not in state_centers:
                    continue
                for state2 in remaining_states[i+1:]:
                    if state2 in states_to_exclude or state2 not in state_centers:
                        continue
                    distance = calculate_distance(
                        state_centers[state1][1], state_centers[state1][0],
                        state_centers[state2][1], state_centers[state2][0]
                    )
                    if distance < min_distance:
                        val1 = map_df[map_df['State'] == state1]['Net_Stores_per_Million'].iloc[0]
                        val2 = map_df[map_df['State'] == state2]['Net_Stores_per_Million'].iloc[0]
                        if val1 < val2:
                            states_to_exclude.add(state1)
                        else:
                            states_to_exclude.add(state2)
            return list(states_to_exclude)

        with col_map:
            # print("Filtered Data:", filtered_data)
            df_cap = filtered_data.dropna(subset=['State', 'Population']).copy()
            # print("Filtered Data after dropna:", df_cap)
            df_cap = df_cap[df_cap['Population'] > 0]
            # print("Filtered Data after population filter:", df_cap)
            if not df_cap.empty:
                opened = df_cap[df_cap['data_from'] == 'opened']
                closed = df_cap[df_cap['data_from'] == 'closed']
                # --- Compute net openings per capita using population_data_by_age_and_sex ---

                opened_by_state = (
                    opened.dropna(subset=['State'])
                          .assign(State=lambda d: d['State'].astype(str).str.strip())
                          .groupby('State', as_index=False)['ChainName_Coresight']
                          .count()
                          .rename(columns={'ChainName_Coresight': 'Opened_Stores'})
                )

                closed_by_state = (
                    closed.dropna(subset=['State'])
                          .assign(State=lambda d: d['State'].astype(str).str.strip())
                          .groupby('State', as_index=False)['ChainName_Coresight']
                          .count()
                          .rename(columns={'ChainName_Coresight': 'Closed_Stores'})
                )

                # Merge openings and closings
                state_merged = (
                    opened_by_state.merge(closed_by_state, on='State', how='outer')
                                   .fillna(0)
                )
                state_merged['Opened_Stores'] = state_merged['Opened_Stores'].astype(int)
                state_merged['Closed_Stores'] = state_merged['Closed_Stores'].astype(int)
                state_merged['Net_Stores'] = state_merged['Opened_Stores'] - state_merged['Closed_Stores']

                pop_df = fetch_data_population()  # should return usps_state_name, zip_code, estimate_total_population
                pop_df['usps_state_name'] = pop_df['usps_state_name'].astype(str).str.strip()
                pop_df['zip_code'] = pop_df['zip_code'].astype(str).str.strip()
                pop_df['estimate_total_population'] = pd.to_numeric(pop_df['estimate_total_population'], errors='coerce')

                # Normalize state names to abbreviations
                pop_df['State'] = pop_df['usps_state_name'].apply(
                    lambda x: x if (isinstance(x, str) and len(x.strip()) == 2)
                    else state_abbreviations.get(str(x).strip())
                )

                state_pop_lookup = (
                    pop_df.dropna(subset=['State', 'zip_code', 'estimate_total_population'])
                          .query('estimate_total_population > 0 and zip_code not in ["0","00000",""]')
                          .drop_duplicates(subset=['State', 'zip_code'])
                          .groupby('State', as_index=False)['estimate_total_population']
                          .sum()
                          .rename(columns={'estimate_total_population': 'State_Population'})
                )

                state_merged = state_merged.merge(state_pop_lookup, on='State', how='left')

                state_merged = state_merged[state_merged['State_Population'].notna() & (state_merged['State_Population'] > 0)].copy()
                state_merged['Net_Stores_per_Million'] = (
                    state_merged['Net_Stores'] / state_merged['State_Population'] * 1_000_000
                ).astype(float)


                # Calculate metadata for each state
                state_metadata = {}
                for state in state_merged['State']:
                    state_data = df_cap[df_cap['State'] == state]
                    state_metadata[state] = {
                        'num_sectors': state_data['Sector_Coresight'].nunique(),
                        'num_banners': state_data['ChainName_Coresight'].nunique(),
                        'num_msa': state_data['MsaName'].nunique()
                    }

                # Calculate overall date range
                start_date = df_cap['Period'].min().strftime('%B %Y')
                end_date = df_cap['Period'].max().strftime('%B %Y')
                date_period = f"{start_date} to {end_date}"

                abbreviation_to_full = {v: k for k, v in state_abbreviations.items()}
                state_merged['State_Full'] = state_merged['State'].map(abbreviation_to_full)
                state_merged['lon'] = state_merged['State'].map(lambda abbr: state_centers.get(abbr, (None, None))[0])
                state_merged['lat'] = state_merged['State'].map(lambda abbr: state_centers.get(abbr, (None, None))[1])

                # --------- Hide overlapping regions, using new logic -----------
                excluded_states = detect_overlapping_states_per_capita(state_merged, state_centers)
                states_to_annotate = state_merged[~state_merged['State'].isin(excluded_states)]

                colors_per_million = [
                    "black" if val < -14 else "white"
                    for state, val in zip(states_to_annotate['State'], states_to_annotate['Net_Stores_per_Million'])
                    if state in state_centers
                ]

                # --------- Main Map -----------
                fig_net_map = px.choropleth(
                    state_merged, locations="State", locationmode="USA-states",
                    color="Net_Stores_per_Million", color_continuous_scale=color_scale_2d2a29,
                    scope="usa", hover_name="State_Full",
                    hover_data={
                        "Net_Stores_per_Million":':.2f',
                        "Net_Stores":True, "Opened_Stores":True, "Closed_Stores":True,
                        "State_Population":True, "State":False, "State_Full":False
                    }
                )

                # Customize hover template with state-specific metadata
                fig_net_map.update_traces(
                    hovertemplate=(
                        "<b>%{hovertext}</b><br>"
                        "<span style='color:#2D2A29'>●</span> <b>Net per Million:</b> %{customdata[0]:.2f}<br>"
                        "<span style='color:#2D2A29'>●</span> <b>Net Openings:</b> %{customdata[1]:,}<br>"
                        "<span style='color:#2D2A29'>•</span> "
                        f"<span style='color:black'>Date Period: {date_period}</span><br>"
                        "<span style='color:#2D2A29'>•</span> "
                        "<span style='color:black'>Number of Sectors: %{customdata[5]:,}</span><br>"
                        "<span style='color:#2D2A29'>•</span> "
                        "<span style='color:black'>Number of Banners: %{customdata[6]:,}</span><br>"
                        "<span style='color:#2D2A29'>•</span> "
                        "<span style='color:black'>Number of MSA: %{customdata[7]:,}</span><br>"
                        "<extra></extra>"
                    ),
                    customdata=[[
                        row['Net_Stores_per_Million'],  # Net per million
                        row['Net_Stores'],  # Net openings
                        row['Opened_Stores'],  # Opened stores
                        row['Closed_Stores'],  # Closed stores
                        row['State_Population'],  # Population
                        state_metadata[row['State']]['num_sectors'],  # Number of sectors
                        state_metadata[row['State']]['num_banners'],  # Number of banners
                        state_metadata[row['State']]['num_msa']  # Number of MSA
                    ] for _, row in state_merged.iterrows()],
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor="#2D2A29",
                        font=dict(size=13, color="black", family="Arial")
                    )
                )

                fig_net_map.update_coloraxes(colorbar_title="Net Openings<br>per Million")

                # --- Annotate with state code AND per-million value, e.g. "CA<br>23.45"
                fig_net_map.add_trace(go.Scattergeo(
                    locationmode='USA-states',
                    lon=[state_centers[state][0] for state in states_to_annotate['State'] if state in state_centers],
                    lat=[state_centers[state][1] for state in states_to_annotate['State'] if state in state_centers],
                    text=[
                        f"{state}<br>{val:.2f}"
                        for state, val in zip(states_to_annotate['State'], states_to_annotate['Net_Stores_per_Million'])
                        if state in state_centers
                    ],
                    mode='text',
                    showlegend=False,
                    textfont=dict(size=11, color=colors_per_million),
                    hoverinfo="skip"
                ))

                fig_net_map.update_layout(
                    margin=dict(l=0, r=0, t=40, b=0),
                    geo=dict(center=dict(lat=37.5, lon=-95), showlakes=True, lakecolor='rgb(255,255,255)')
                )
                st.markdown("<h4 style='font-size: 20px;text-align: center;'>Net Openings per Million Capita by State</h4>", unsafe_allow_html=True)
                st.plotly_chart(fig_net_map, use_container_width=True)
            else:
                st.warning('No eligible records found for net openings per million capita map.')

        # --- Top Sectors Bar ---
        with col_bar:
            # Only consider non-null sectors for net calc
            opened_sector = filtered_data[filtered_data['data_from'] == 'opened']['Sector_Coresight'].value_counts()
            closed_sector = filtered_data[filtered_data['data_from'] == 'closed']['Sector_Coresight'].value_counts()
            net_sector = opened_sector.subtract(closed_sector, fill_value=0)
            net_sector = net_sector[net_sector != 0].sort_values(ascending=False)
            top15 = net_sector.head(15)
            
            if not top15.empty:
                # Calculate dynamic date period
                start_date = filtered_data['Period'].min()
                end_date = filtered_data['Period'].max()
                start_date_str = start_date.strftime('%b %Y') if not pd.isna(start_date) else "N/A"
                end_date_str = end_date.strftime('%b %Y') if not pd.isna(end_date) else "N/A"
                date_period = f"{start_date_str} to {end_date_str}"
                
                # Calculate metadata for the entire dataset
                num_banners = filtered_data['ChainName_Coresight'].nunique()
                num_states = filtered_data['State'].nunique()
                num_msa = filtered_data['MsaName'].nunique()
                
                fig_sector = go.Figure()
                
                fig_sector.add_trace(go.Bar(
                    x=top15.index,
                    y=top15.values,
                    marker_color="#2D2A29",
                    text=[f"{x:,}" for x in top15.values],
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"  # Sector name
                        "<span style='color:#2D2A29'>●</span> "
                        "<b>Net Openings:</b> %{y:,}<br><br>"
                        "<span style='color:#2D2A29'>•</span> "
                        f"<span style='color:black'>Date Period: {date_period}</span><br>"
                        "<span style='color:#2D2A29'>•</span> "
                        f"<span style='color:black'>Number of Banners: {num_banners:,}</span><br>"
                        "<span style='color:#2D2A29'>•</span> "
                        f"<span style='color:black'>Number of States: {num_states:,}</span><br>"
                        "<span style='color:#2D2A29'>•</span> "
                        f"<span style='color:black'>Number of MSA: {num_msa:,}</span><br>"
                        "<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor="#2D2A29",
                        font=dict(size=13, color="black", family="Arial")
                    )
                ))
                
                fig_sector.update_layout(
                    xaxis_title="Sector",
                    height=400,
                    margin=dict(l=20, r=20, t=40, b=20),
                    xaxis=dict(
                        showline=True,
                        zeroline=False,
                        tickangle=45 if len(top15) > 5 else 0
                    ),
                    yaxis=dict(
                        showline=True,
                        zeroline=False
                    ),
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )
                
                # Additional label rotation for many sectors
                if len(top15) > 8:
                    fig_sector.update_layout(
                        xaxis=dict(tickangle=60),
                        margin=dict(b=60)
                    )
                
                st.markdown(
                    f"<h4 style='font-size: 20px;text-align: center;'>Top {top15.shape[0]} Sectors: Net Openings</h4>",
                    unsafe_allow_html=True
                )
                st.plotly_chart(fig_sector, use_container_width=True)
            else:
                st.warning("No Top Sector data available")

        st.markdown("---")

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

    elif selected_tab == "Compare Retailers":

        import calendar
        import pandas as pd
        from datetime import date

        def make_json_safe(val):
            """Recursively convert pandas Timestamp to iso string in dictionaries/lists."""
            if isinstance(val, pd.Timestamp):
                return val.isoformat()
            if isinstance(val, list):
                return [make_json_safe(x) for x in val]
            if isinstance(val, dict):
                return {k: make_json_safe(v) for k, v in val.items()}
            return val

        def determine_valid_start_months(start_year, min_year, max_year, min_date, max_date):
            if start_year == min_year:
                return list(range(min_date.month, 13))
            elif start_year == max_year:
                return list(range(1, max_date.month + 1))
            else:
                return list(range(1, 13))

        def determine_valid_end_years(start_year, min_year, max_year):
            return [y for y in range(start_year, max_year + 1)]

        def determine_valid_end_months(start_year, start_month, end_year, max_year, max_date):
            if end_year == start_year and end_year == max_year:
                return [m for m in range(start_month, max_date.month + 1)]
            elif end_year == start_year:
                return [m for m in range(start_month, 13)]
            elif end_year == max_year:
                return [m for m in range(1, max_date.month + 1)]
            else:
                return list(range(1, 13))

        # Prefix for compare retailer page net openings filters
        RCPREFIX = "retailer_compare_"

        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            with st.expander("Date Range", expanded=user_filters.get(RCPREFIX + "expand_filters", True)):
                combined_data = pd.concat([opened_df, closed_df])

                # Ensure Period is datetime
                combined_data['Period'] = pd.to_datetime(combined_data['Period'])
                opened_df['Period'] = pd.to_datetime(opened_df['Period'])
                closed_df['Period'] = pd.to_datetime(closed_df['Period'])

                # Get min and max available dates
                min_available_date = combined_data['Period'].min()
                max_available_date = combined_data['Period'].max()

                # Calculate default 12-month window
                default_end_date = max_available_date
                default_start_date = (default_end_date - pd.DateOffset(months=11)).replace(day=1)

                # Set defaults or use user filters
                start_year = user_filters.get(RCPREFIX + "start_year", default_start_date.year)
                start_month = user_filters.get(RCPREFIX + "start_month", default_start_date.month)
                end_year = user_filters.get(RCPREFIX + "end_year", default_end_date.year)
                end_month = user_filters.get(RCPREFIX + "end_month", default_end_date.month)

                min_year = min_available_date.year
                max_year = max_available_date.year
                all_years = list(range(min_year, max_year + 1))

                # --- START DATE PICKERS ---
                sm_col, sy_col = st.columns([1.3, 1])
                with sy_col:
                    if start_year not in all_years:
                        start_year = all_years[0]
                    start_year = st.selectbox(
                        "Start Year",
                        options=all_years,
                        index=all_years.index(start_year),
                        key=RCPREFIX + "start_year_select"
                    )
                with sm_col:
                    valid_start_months = determine_valid_start_months(start_year, min_year, max_year, min_available_date, max_available_date)
                    if start_month not in valid_start_months:
                        start_month = valid_start_months[0]
                    start_month = st.selectbox(
                        "Start Month",
                        options=valid_start_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_start_months.index(start_month),
                        key=RCPREFIX + "start_month_select"
                    )

                # --- END DATE PICKERS ---
                em_col, ey_col = st.columns([1.3, 1])
                valid_end_years = determine_valid_end_years(start_year, min_year, max_year)
                if end_year not in valid_end_years:
                    end_year = valid_end_years[0]

                with ey_col:
                    end_year = st.selectbox(
                        "End Year",
                        options=valid_end_years,
                        index=valid_end_years.index(end_year),
                        key=RCPREFIX + "end_year_select"
                    )

                valid_end_months = determine_valid_end_months(start_year, start_month, end_year, max_year, max_available_date)
                if end_month not in valid_end_months:
                    end_month = valid_end_months[0]
                with em_col:
                    end_month = st.selectbox(
                        "End Month",
                        options=valid_end_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_end_months.index(end_month),
                        key=RCPREFIX + "end_month_select"
                    )

                # Check if date range has changed
                date_changed = False
                if (user_filters.get(RCPREFIX + "start_year") != start_year or 
                    user_filters.get(RCPREFIX + "start_month") != start_month or
                    user_filters.get(RCPREFIX + "end_year") != end_year or
                    user_filters.get(RCPREFIX + "end_month") != end_month):
                    date_changed = True
                
                # Save back to user_filters
                user_filters[RCPREFIX + "start_year"] = start_year
                user_filters[RCPREFIX + "start_month"] = start_month
                user_filters[RCPREFIX + "end_year"] = end_year
                user_filters[RCPREFIX + "end_month"] = end_month

                # --- Compute Timestamps and bounds ---
                candidate_start_date = pd.Timestamp(date(start_year, start_month, 1))
                end_day = calendar.monthrange(end_year, end_month)[1]
                candidate_end_date = pd.Timestamp(date(end_year, end_month, end_day))

                # Clamp within min/max available in data
                new_start_date = max(candidate_start_date, min_available_date)
                new_end_date = min(candidate_end_date, max_available_date)

                # Save for possible download, further functions, etc.
                user_filters[RCPREFIX + "start_date"] = new_start_date
                user_filters[RCPREFIX + "end_date"] = new_end_date

                # Filtered dfs for chaining next filters:
                date_filtered_opened = opened_df[
                    (opened_df['Period'] >= new_start_date) & (opened_df['Period'] <= new_end_date)
                ]
                date_filtered_closed = closed_df[
                    (closed_df['Period'] >= new_start_date) & (closed_df['Period'] <= new_end_date)
                ]
                print("date_filtered_opened",date_changed);
                # Reset downstream filters if date changed
                if date_changed:
                    user_filters[RCPREFIX + "selected_sector_name"] = "All"
                    user_filters[RCPREFIX + "parent_chain_name"] = []
                    user_filters[RCPREFIX + "selected_chain_name"] = []
                    # user_filters[RCPREFIX + "selected_state_name"] = ["All"]
                    # user_filters[RCPREFIX + "selected_msa_name"] = ["All"]
                    # user_filters[RCPREFIX + "selected_postal_codes"] = ["All"]
                    st.session_state[RCPREFIX + "selected_state_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_msa_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_postal_codes"] = ["All"]
                    safe_cookie = make_json_safe(auth_cookie)
                    auth_cookie = safe_cookie
                    save_auth_cookie()
                    st.rerun()

        with col2:
            sector_filtered_opened = date_filtered_opened
            sector_filtered_closed = date_filtered_closed

            combined_data = pd.concat([sector_filtered_opened, sector_filtered_closed])
            sector_names = combined_data["Sector_Coresight"].dropna().unique().tolist()

            # Separate "Other"/"Others" from the rest (case-insensitive)
            others_items = [s for s in sector_names if s.lower() in ["other", "others"]]
            regular_items = [s for s in sector_names if s.lower() not in ["other", "others"]]
            regular_items = sorted(regular_items, key=lambda x: x.lower())
            sector_names = ["All"] + regular_items + others_items

            # Get current sector selection or default
            current_sector = user_filters.get(RCPREFIX + "selected_sector_name", "All")
            if current_sector not in sector_names:
                current_sector = "All"
                user_filters[RCPREFIX + "selected_sector_name"] = "All"
                safe_cookie = make_json_safe(auth_cookie)
                # cookie_controller.set(
                #     "auth_data",
                #     json.dumps(auth_cookie),                    # must be a string
                #     expires=datetime.utcnow() + timedelta(days=30),
                #     path="/",
                #     domain="localhost",
                # )
                save_auth_cookie()

            current_index = sector_names.index(current_sector) if current_sector in sector_names else 0
            
            with st.expander("Sector", expanded=user_filters.get(RCPREFIX + "expand_filters", True)):
                selected_sector_name = st.selectbox(
                    "Select Sector",
                    sector_names,
                    index=current_index,
                    key=RCPREFIX + "sector_selectbox"
                )
                # (optional) logging.info(f"User selected sector: {selected_sector_name}")

                if selected_sector_name != current_sector:
                    user_filters[RCPREFIX + "selected_sector_name"] = selected_sector_name
                    user_filters[RCPREFIX + "parent_chain_name"] = []
                    user_filters[RCPREFIX + "selected_chain_name"] = []
                    # user_filters[RCPREFIX + "selected_state_name"] = ["All"]
                    # user_filters[RCPREFIX + "selected_msa_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_state_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_msa_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_postal_codes"] = ["All"]
                    # Handle json serializability for cookie
                    safe_cookie = make_json_safe(auth_cookie)
                    auth_cookie = safe_cookie
                    # cookie_controller.set(
                    #     "auth_data",
                    #     json.dumps(auth_cookie),                    # must be a string
                    #     expires=datetime.utcnow() + timedelta(days=30),
                    #     path="/",
                    #     domain="localhost",
                    # )
                    save_auth_cookie()
                    st.rerun()

            # Filter data by sector
            if "All" not in selected_sector_name:
                sector_filtered_opened = sector_filtered_opened[
                    sector_filtered_opened["Sector_Coresight"].isin([selected_sector_name])
                ]
                sector_filtered_closed = sector_filtered_closed[
                    sector_filtered_closed["Sector_Coresight"].isin([selected_sector_name])
                ]
    
        import pandas as pd



        with col3:
            retailer_filtered_opened = sector_filtered_opened.copy()
            retailer_filtered_closed = sector_filtered_closed.copy()

            # Fill NA for no parent, then get full list of unique parent retailers
            retailer_filtered_opened['ParentName_Coresight'] = retailer_filtered_opened['ParentName_Coresight'].where(
                pd.notna(retailer_filtered_opened['ParentName_Coresight']), "No Parent Retailer"
            )
            retailer_filtered_closed['ParentName_Coresight'] = retailer_filtered_closed['ParentName_Coresight'].where(
                pd.notna(retailer_filtered_closed['ParentName_Coresight']), "No Parent Retailer"
            )

            parent_names = pd.concat([
                retailer_filtered_opened['ParentName_Coresight'],
                retailer_filtered_closed['ParentName_Coresight']
            ]).unique().tolist()
            parent_names = [str(p) for p in parent_names]
            parent_names.sort()
            if "No Parent Retailer" in parent_names:
                parent_names.remove("No Parent Retailer")
                parent_names.append("No Parent Retailer")
            # No "All" option

            current_parent = user_filters.get(RCPREFIX + "parent_chain_name", [])
            # Always ensure it's a list of strings
            valid_selected = [str(p) for p in current_parent if p in parent_names]
            current_parent = valid_selected if valid_selected else []

            with st.expander("Retailers", expanded=user_filters.get(RCPREFIX + "expand_filters", True)):
                parent_chain_name = st.multiselect(
                    "Select Company (min 2)",
                    parent_names,
                    default=current_parent,
                    max_selections=10,
                    key=RCPREFIX + "parent_multiselect_1"
                )

                if RCPREFIX + "parent_multiselect_1" not in st.session_state:
                    st.session_state[RCPREFIX + "parent_multiselect_1"] = parent_chain_name

                if parent_chain_name != current_parent:
                    user_filters[RCPREFIX + "parent_chain_name"] = parent_chain_name
                    # user_filters[RCPREFIX + "selected_state_name"] = ["All"]
                    # user_filters[RCPREFIX + "selected_msa_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_state_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_msa_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_postal_codes"] = ["All"]

                    # Handle json serializability for cookie
                    safe_cookie = make_json_safe(auth_cookie)
                    auth_cookie = safe_cookie
                    # cookie_controller.set(
                    #     "auth_data",
                    #     json.dumps(auth_cookie),                    # must be a string
                    #     expires=datetime.utcnow() + timedelta(days=30),
                    #     path="/",
                    #     domain="localhost",
                    # )
                    save_auth_cookie()
                    st.rerun()

                st.markdown(
                    """<div style='text-align: center; font-size: 0.9rem; font-weight: 500; margin-top: 0.4rem; margin-bottom: 0.6rem; color: #444;'>or</div>""",
                    unsafe_allow_html=True
                )

                # Apply parent filter
                if len(parent_chain_name) >= 1:
                    retailer_filtered_opened = retailer_filtered_opened[
                        retailer_filtered_opened["ParentName_Coresight"].isin(parent_chain_name)
                    ]
                    retailer_filtered_closed = retailer_filtered_closed[
                        retailer_filtered_closed["ParentName_Coresight"].isin(parent_chain_name)
                    ]

                chain_names = pd.concat([
                    retailer_filtered_opened['ChainName_Coresight'],
                    retailer_filtered_closed['ChainName_Coresight']
                ]).unique().tolist()
                # Remove possible None and convert to str
                chain_names = [str(c) for c in chain_names if c is not None]
                chain_names.sort()
                # No "All" by default
                current_chain = user_filters.get(RCPREFIX + "selected_chain_name", [])
                valid_chains = [str(c) for c in current_chain if c in chain_names]
                current_chain = valid_chains if valid_chains else []

                selected_chain_name = st.multiselect(
                    "Select Banner/Brand (min 2, or leave blank for retailer comparison)",
                    chain_names,
                    default=current_chain,
                    max_selections=10,
                    key=RCPREFIX + "multiselect_selected_chain"
                )
                if RCPREFIX + "multiselect_selected_chain" not in st.session_state:
                    st.session_state[RCPREFIX + "multiselect_selected_chain"] = selected_chain_name
                # Update logic -- make SURE you store pure strings only
                if selected_chain_name != current_chain:
                    # Force selected_chain_name to be all strings for safety
                    user_filters[RCPREFIX + "selected_chain_name"] = [str(c) for c in selected_chain_name]
                    # user_filters[RCPREFIX + "selected_state_name"] = ["All"]
                    # user_filters[RCPREFIX + "selected_msa_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_state_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_msa_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_postal_codes"] = ["All"]

                    # Handle json serializability for cookie
                    safe_cookie = make_json_safe(auth_cookie)
                    auth_cookie = safe_cookie
                    # cookie_controller.set(
                    #     "auth_data",
                    #     json.dumps(auth_cookie),                    # must be a string
                    #     expires=datetime.utcnow() + timedelta(days=30),
                    #     path="/",
                    #     domain="localhost",
                    # )
                    save_auth_cookie()
                    st.rerun()

                # Finalize retailer_filtered_data
                if len(selected_chain_name) >= 1:
                    retailer_filtered_opened = retailer_filtered_opened[
                        retailer_filtered_opened["ChainName_Coresight"].isin(selected_chain_name)
                    ]
                    retailer_filtered_closed = retailer_filtered_closed[
                        retailer_filtered_closed["ChainName_Coresight"].isin(selected_chain_name)
                    ]
    
        with col4:
            with st.expander("Location", expanded=user_filters.get(RCPREFIX + "expand_filters", True)):
                location_filtered_opened = retailer_filtered_opened
                location_filtered_closed = retailer_filtered_closed
                
                # --- STATE FILTER ---
                states_opened = location_filtered_opened["State"].dropna().unique().tolist()
                states_closed = location_filtered_closed["State"].dropna().unique().tolist()
                states = sorted(set(states_opened).union(states_closed))
                states = ["All"] + states

                current_state =st.session_state.get(RCPREFIX + "selected_state_name", ["All"])
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
                    key=RCPREFIX + "multiselect_state",
                    disabled=not (
                        len(user_filters.get(RCPREFIX + "parent_chain_name", [])) >= 2 or
                        len(user_filters.get(RCPREFIX + "selected_chain_name", [])) >= 2
                    )
                )
                if len(selected_state_name) > 1 and "All" in selected_state_name:
                    if selected_state_name[-1] == "All":
                        selected_state_name = ["All"]
                    else:
                        selected_state_name = [s for s in selected_state_name if s != "All"]
                elif not selected_state_name:
                    selected_state_name = ["All"]

                if selected_state_name != current_state:
                    st.session_state[RCPREFIX + "selected_state_name"] = selected_state_name
                    st.session_state[RCPREFIX + "selected_msa_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_postal_codes"] = ["All"]
                    safe_cookie = make_json_safe(auth_cookie)
                    auth_cookie = safe_cookie
                    # cookie_controller.set(
                    #     "auth_data",
                    #     json.dumps(auth_cookie),                    # must be a string
                    #     expires=datetime.utcnow() + timedelta(days=30),
                    #     path="/",
                    #     domain=domain,
                    # )
                    save_auth_cookie()
                    st.rerun()

                if selected_state_name != current_state:
                    st.session_state[RCPREFIX + "selected_state_name"] = selected_state_name
                    st.session_state[RCPREFIX + "selected_msa_name"] = ["All"]
                    st.session_state[RCPREFIX + "selected_postal_codes"] = ["All"]
                    # safe_cookie = make_json_safe(auth_cookie)
                    # cookie_controller.set(
                    #     "auth_data",
                    #     json.dumps(auth_cookie),                    # must be a string
                    #     expires=datetime.utcnow() + timedelta(days=30),
                    #     path="/",
                    #     domain="localhost",
                    # )
                    # save_auth_cookie()
                    # st.rerun()

                # Ensure "All" is not mixed with specific states
                # if len(selected_state_name) > 1 and "All" in selected_state_name:
                #     if selected_state_name[-1] == "All":
                #         selected_state_name = ["All"]
                #     else:
                #         selected_state_name = [s for s in selected_state_name if s != "All"]
                # elif not selected_state_name:
                #     selected_state_name = ["All"]

                # if selected_state_name != current_state:
                #     user_filters[RCPREFIX + "selected_state_name"] = selected_state_name
                #     user_filters[RCPREFIX + "selected_msa_name"] = ["All"]
                #     safe_cookie = make_json_safe(auth_cookie)
                #     # cookie_controller.set(
                #     #     "auth_data",
                #     #     json.dumps(auth_cookie),                    # must be a string
                #     #     expires=datetime.utcnow() + timedelta(days=30),
                #     #     path="/",
                #     #     domain="localhost",
                #     # )
                #     save_auth_cookie()
                #     st.rerun()

                # Filter data by selected states
                if "All" not in selected_state_name:
                    location_filtered_opened = location_filtered_opened[location_filtered_opened['State'].isin(selected_state_name)]
                    location_filtered_closed = location_filtered_closed[location_filtered_closed['State'].isin(selected_state_name)]

                # # --- MSA FILTER ---
                # st.markdown("""
                # <style>
                # .st-emotion-cache-wfksaw {
                #     gap: 0.2rem !important; /* Reduce the gap to a smaller value */
                # }
                # </style>
                # """, unsafe_allow_html=True)
                # _, center_col, _ = st.columns([20, 80, 10])
                # with center_col:
                #     # saved_location_type = user_filters.get("location_type_retailers_net", "MSA")
                #     # location_type = st.radio(
                #     #     "Filter by location",
                #     #     ["MSA", "Zip Code"],
                #     #     horizontal=True,
                #     #     index=0 if saved_location_type == "MSA" else 1,
                #     #     key="location_type_retailers_net",
                #     #     label_visibility="collapsed"
                #     # )
                #     # if location_type != saved_location_type:
                #     #     user_filters["location_type_retailers_net"] = location_type
                #     #     safe_cookie = make_json_safe(auth_cookie)
                #     #     # cookie_controller.set(
                #     #     #     "auth_data",
                #     #     #     json.dumps(auth_cookie),                    # must be a string
                #     #     #     expires=datetime.utcnow() + timedelta(days=30),
                #     #     #     path="/",
                #     #     #     domain="localhost",
                #     #     # )
                #     #     save_auth_cookie()
                #     #     st.rerun()
                #     location_type = "MSA"
                # # -----------------
                # # MSA OR ZIP FILTER
                # # -----------------

                # if location_type == "MSA":
                #     msa_opened = location_filtered_opened['MsaName'].dropna().unique().tolist()
                #     msa_closed = location_filtered_closed['MsaName'].dropna().unique().tolist()
                #     filter_values = sorted(set(msa_opened).union(msa_closed))
                #     filter_values = ["All"] + filter_values

                #     current_filter = st.session_state.get("selected_msa_name", ["All"])
                #     multiselect_label = "Select Metropolitan Statistical Area (MSA)"
                #     filter_key = "multiselect_msa"
                #     filter_column = "MsaName"

                # else:  # ZIP CODE selected
                #     zip_opened = location_filtered_opened['PostalCode'].dropna().unique().tolist()
                #     zip_closed = location_filtered_closed['PostalCode'].dropna().unique().tolist()
                #     filter_values = sorted(
                #         {z for z in zip_opened if str(z) != "0"}.union({z for z in zip_closed if str(z) != "0"})
                #     )
                #     filter_values = ["All"] + filter_values

                #     current_filter = user_filters.get("selected_zip_code", ["All"])
                #     multiselect_label = "Select Postal Code"
                #     filter_key = "multiselect_zip_reatailers_net"
                #     filter_column = "PostalCode"

                # # -----------------
                # # MULTISELECT FILTER
                # # -----------------
                # if "All" in current_filter:
                #     current_filter = ["All"]
                # else:
                #     valid_filter = [f for f in current_filter if f in filter_values]
                #     current_filter = valid_filter if valid_filter else ["All"]

                # selected_filter = st.multiselect(
                #     multiselect_label,
                #     filter_values,
                #     default=current_filter,
                #     key=filter_key,
                #     disabled=not user_filters.get(RCPREFIX + "selected_chain_name", [])
                # )
                # logging.info(f"User selected {location_type}: {selected_filter}")

                # # Handle "All"
                # if len(selected_filter) > 1 and "All" in selected_filter:
                #     if selected_filter[-1] == "All":
                #         selected_filter = ["All"]
                #     else:
                #         selected_filter = [f for f in selected_filter if f != "All"]
                # elif not selected_filter:
                #     selected_filter = ["All"]

                # # Update user_filters
                # if location_type == "MSA":
                #     if selected_filter != current_filter:
                #         st.session_state["selected_msa_name"] = selected_filter
                #         # safe_cookie = make_json_safe(auth_cookie)
                #         # cookie_controller.set(
                #         #     "auth_data",
                #         #     json.dumps(auth_cookie),                    # must be a string
                #         #     expires=datetime.utcnow() + timedelta(days=30),
                #         #     path="/",
                #         #     domain="localhost",
                #         # )
                #         # save_auth_cookie()
                #         # st.rerun()
                # else:
                #     if selected_filter != current_filter:
                #         user_filters["selected_zip_code"] = selected_filter
                #         safe_cookie = make_json_safe(auth_cookie)
                #         # cookie_controller.set(
                #         #     "auth_data",
                #         #     json.dumps(auth_cookie),                    # must be a string
                #         #     expires=datetime.utcnow() + timedelta(days=30),
                #         #     path="/",
                #         #     domain="localhost",
                #         # )
                #         save_auth_cookie()
                #         st.rerun()
                # if "All" not in selected_filter:
                #     location_filtered_opened = location_filtered_opened[location_filtered_opened[filter_column].isin(selected_filter)]
                #     location_filtered_closed = location_filtered_closed[location_filtered_closed[filter_column].isin(selected_filter)]
                                # --- MSA FILTER ---
                msa_opened = location_filtered_opened['MsaName'].dropna().unique().tolist()
                msa_closed = location_filtered_closed['MsaName'].dropna().unique().tolist()
                msa_values = sorted(set(msa_opened).union(msa_closed))
                msa_values = ["All"] + msa_values

                current_msa = st.session_state.get(RCPREFIX + "selected_msa_name", ["All"])
                if "All" in current_msa:
                    current_msa = ["All"]
                else:
                    valid_msas = [m for m in current_msa if m in msa_values]
                    current_msa = valid_msas if valid_msas else ["All"]

                selected_msa = st.multiselect(
                    "Select MSA",
                    msa_values,
                    default=current_msa,
                    help="A metro region centered on a large city plus its economically linked suburbs.",
                    key=RCPREFIX + "msa_multiselect",
                    disabled=not (
                        len(user_filters.get(RCPREFIX + "parent_chain_name", [])) >= 2 or
                        len(user_filters.get(RCPREFIX + "selected_chain_name", [])) >= 2
                    )
                )

                # Handle "All" for MSA
                if len(selected_msa) > 1 and "All" in selected_msa:
                    if selected_msa[-1] == "All":
                        selected_msa = ["All"]
                    else:
                        selected_msa = [m for m in selected_msa if m != "All"]
                elif not selected_msa:
                    selected_msa = ["All"]

                if selected_msa != current_msa:
                    st.session_state[RCPREFIX + "selected_msa_name"] = selected_msa
                    st.session_state[RCPREFIX + "selected_postal_codes"] = ["All"]
                    save_auth_cookie()
                    st.rerun()

                if "All" not in selected_msa:
                    location_filtered_opened = location_filtered_opened[location_filtered_opened["MsaName"].isin(selected_msa)]
                    location_filtered_closed = location_filtered_closed[location_filtered_closed["MsaName"].isin(selected_msa)]

                # --- POSTAL CODE FILTER ---
                postal_codes_opened = location_filtered_opened['PostalCode'].dropna().astype(str).unique().tolist()
                postal_codes_closed = location_filtered_closed['PostalCode'].dropna().astype(str).unique().tolist()
                postal_codes = sorted(set(postal_codes_opened + postal_codes_closed))
                postal_codes = [p for p in postal_codes if p != "0" and p != "nan"]  # Remove invalid postal codes
                postal_codes = ["All"] + postal_codes

                current_postal = st.session_state.get(RCPREFIX + "selected_postal_codes", ["All"])
                if "All" in current_postal:
                    current_postal = ["All"]
                else:
                    valid_postals = [p for p in current_postal if p in postal_codes]
                    current_postal = valid_postals if valid_postals else ["All"]

                selected_postal_codes = st.multiselect(
                    "Select Zip Code",
                    postal_codes,
                    default=current_postal,
                    help="U.S. postal area where the store is located.",
                    key=RCPREFIX + "postal_multiselect",
                    disabled=not (
                        len(user_filters.get(RCPREFIX + "parent_chain_name", [])) >= 2 or
                        len(user_filters.get(RCPREFIX + "selected_chain_name", [])) >= 2
                    )
                )

                # Handle "All" for Postal Codes
                if len(selected_postal_codes) > 1 and "All" in selected_postal_codes:
                    if selected_postal_codes[-1] == "All":
                        selected_postal_codes = ["All"]
                    else:
                        selected_postal_codes = [p for p in selected_postal_codes if p != "All"]
                elif not selected_postal_codes:
                    selected_postal_codes = ["All"]

                if selected_postal_codes != current_postal:
                    st.session_state[RCPREFIX + "selected_postal_codes"] = selected_postal_codes
                    save_auth_cookie()
                    st.rerun()

                if "All" not in selected_postal_codes:
                    location_filtered_opened = location_filtered_opened[location_filtered_opened["PostalCode"].astype(str).isin(selected_postal_codes)]
                    location_filtered_closed = location_filtered_closed[location_filtered_closed["PostalCode"].astype(str).isin(selected_postal_codes)]
            
            

        # Combine for convenience
        location_filtered_opened['data_from'] = 'opened'
        location_filtered_closed['data_from'] = 'closed'
        final_filtered_data = pd.concat([location_filtered_opened, location_filtered_closed], ignore_index=True)

        import pandas as pd
        import plotly.graph_objects as go
        import plotly.express as px
        import streamlit as st

        if (
            (len(parent_chain_name) >= 2) or
            (len(selected_chain_name) >= 2)
        ) and not final_filtered_data.empty:

            # --- KPI Section as before ---
            net_openings = location_filtered_opened['ChainName_Coresight'].count() - location_filtered_closed['ChainName_Coresight'].count()
            total_banners = final_filtered_data['ChainName_Coresight'].nunique()
            total_sectors = final_filtered_data['Sector_Coresight'].nunique()

            kpi1, kpi2, kpi3 = st.columns([1, 1, 1])
            with kpi1:
                st.markdown(f"<h1 style='font-size: 40px;'>{net_openings:,}</h1><h6>Net Opened Stores</h6>", unsafe_allow_html=True)
            with kpi2:
                st.markdown(f"<h1 style='font-size: 40px;'>{total_banners:,}</h1><h6>Total Net Affected Banners</h6>", unsafe_allow_html=True)
            with kpi3:
                st.markdown(f"<h1 style='font-size: 40px;'>{total_sectors:,}</h1><h6>Total Net Affected Sectors</h6>", unsafe_allow_html=True)
            st.markdown('<hr style="border-top: 1px solid #808080;">', unsafe_allow_html=True)

            # Ensure date parsing
            final_filtered_data['Period'] = pd.to_datetime(final_filtered_data['Period'], errors='coerce')

            # COLOR MAP for consistency
            all_groups = final_filtered_data['ChainName_Coresight'].unique()
            color_list = px.colors.qualitative.Plotly*3  # repeat in case there are >10
            color_map = {g: color_list[i] for i, g in enumerate(sorted(all_groups))}

            gcol1, gcol2 = st.columns([4,2])

            # --- LINE CHART ---
            with gcol1:
                st.markdown("<h4 style='font-size: 20px;text-align: center;'>Opened Stores Over Time<br></h4>", unsafe_allow_html=True)
                time_grouped = (
                    final_filtered_data[final_filtered_data['data_from'] == 'opened']  # Only opened stores
                    .groupby(['Period', 'ChainName_Coresight'])
                    .size().reset_index(name='OpenedStores')
                )
                fig_line = go.Figure()
                for chain, grp_df in time_grouped.groupby("ChainName_Coresight"):
                    fig_line.add_trace(go.Scatter(
                        x=grp_df['Period'],
                        y=grp_df['OpenedStores'],
                        mode='lines+markers',
                        name=chain,
                        marker=dict(color=color_map.get(chain, "#333")),
                        line=dict(width=3),
                    ))
                fig_line.update_layout(
                    yaxis_title="Opened Stores",
                    xaxis_title="Period",
                    height=380,
                    showlegend=True,
                    legend=dict(
                        x=0.99, y=0.99, xanchor="right", yanchor="top",
                        bgcolor='rgba(0,0,0,0)',
                        bordercolor='rgba(0,0,0,0)',
                        font=dict(size=13), orientation="v"
                    ),
                    margin=dict(l=10, r=10, t=60, b=10)
                )
                config = {
                    'displayModeBar': True,
                    'displaylogo': False,
                    'toImageButtonOptions': {
                        'format': 'png', 'filename': 'opened_stores_by_chain_over_time',
                        'height': 380, 'width': 700, 'scale': 1
                    }
                }
                st.plotly_chart(fig_line, use_container_width=True, config=config)

            # --- BAR CHART: Totals for each banner ---
            with gcol2:
                st.markdown("<h4 style='font-size: 20px; text-align: center;'>Total Opened Stores</h4>", unsafe_allow_html=True)
                bar_grouped = (
                    final_filtered_data[final_filtered_data['data_from'] == 'opened']
                    .groupby(['ChainName_Coresight'])
                    .size().reset_index(name='OpenedStores')
                )
                fig_bar = px.bar(
                    bar_grouped.sort_values('OpenedStores', ascending=False),
                    x="ChainName_Coresight", y="OpenedStores",
                    color="ChainName_Coresight",
                    color_discrete_map=color_map,
                    text="OpenedStores",
                    labels={'ChainName_Coresight': 'Banner', 'OpenedStores': 'Opened Stores'}
                )
                fig_bar.update_traces(textposition='outside')
                fig_bar.update_layout(
                    showlegend=False,
                    yaxis_title="Opened Stores",
                    xaxis_title="Banner",
                    height=350,
                    margin=dict(l=10, r=10, t=10, b=10)
                )
                st.plotly_chart(fig_bar, use_container_width=True)

            import colorsys

            st.markdown("---")
            st.markdown("<h4 style='font-size: 20px; text-align: left;'>Net Opened Stores by Retailer</h4>", unsafe_allow_html=True)

            # Step 1: Calculate net openings by ParentName_Coresight (retailer)
            opened_by_parent = final_filtered_data[final_filtered_data['data_from'] == 'opened'].groupby(
                ['ParentName_Coresight']
            ).size().reset_index(name='OpenedStores')
            closed_by_parent = final_filtered_data[final_filtered_data['data_from'] == 'closed'].groupby(
                ['ParentName_Coresight']
            ).size().reset_index(name='ClosedStores')

            retailer_agg = pd.merge(opened_by_parent, closed_by_parent, on='ParentName_Coresight', how='outer').fillna(0)
            retailer_agg["NetOpenedStores"] = retailer_agg["OpenedStores"] - retailer_agg["ClosedStores"]
            # Remove retailers with net zero (optional, can comment out)
            # retailer_agg = retailer_agg[retailer_agg['NetOpenedStores'] != 0]

            # --- Generate shades of the approved color for each retailer
            def generate_shades(base_hex, num_shades):
                base_rgb = [int(base_hex[i:i+2], 16) / 255. for i in (1, 3, 5)]
                h, l, s = colorsys.rgb_to_hls(*base_rgb)
                # Shades: make some lighter, some darker
                l_values = [min(0.9, max(0.2, l * (0.80 + i*0.20/(max(num_shades-1,1))))) for i in range(num_shades)]
                return [
                    f'#{int(c[0]*255):02x}{int(c[1]*255):02x}{int(c[2]*255):02x}'
                    for c in [colorsys.hls_to_rgb(h, lval, s) for lval in l_values]
                ]

            n = len(retailer_agg)
            my_palette = generate_shades("#A3C0CE", n)
            color_map_retailer = {name: my_palette[i] for i, name in enumerate(retailer_agg['ParentName_Coresight'])}

            # --- Net Openings by Retailer Bar Chart
            import plotly.express as px
            fig_retailer = px.bar(
                retailer_agg.sort_values("NetOpenedStores", ascending=False),
                x="ParentName_Coresight",
                y="NetOpenedStores",
                color="ParentName_Coresight",
                color_discrete_map=color_map_retailer,
                text="NetOpenedStores",
                labels={'ParentName_Coresight': 'Retailer', 'NetOpenedStores': 'Net Opened Stores'}
            )
            fig_retailer.update_traces(
                texttemplate='%{y}',
                textposition='outside',
                cliponaxis=False
            )
            fig_retailer.update_layout(
                showlegend=False,
                height=400,
                margin=dict(t=60, b=40, l=10, r=10)
            )
            st.plotly_chart(fig_retailer, use_container_width=True)

        else:
            st.info("Please select at least two retailers or two banners to start the comparison.")

        st.markdown("---")
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
    
    else:
        import calendar
        import pandas as pd
        from datetime import date

        def make_json_safe(val):
            """Recursively convert pandas Timestamp to iso string in dictionaries/lists."""
            if isinstance(val, pd.Timestamp):
                return val.isoformat()
            if isinstance(val, list):
                return [make_json_safe(x) for x in val]
            if isinstance(val, dict):
                return {k: make_json_safe(v) for k, v in val.items()}
            return val

        def determine_valid_start_months(start_year, min_year, max_year, min_date, max_date):
            if start_year == min_year:
                return list(range(min_date.month, 13))
            elif start_year == max_year:
                return list(range(1, max_date.month + 1))
            else:
                return list(range(1, 13))

        def determine_valid_end_years(start_year, min_year, max_year):
            return [y for y in range(start_year, max_year + 1)]

        def determine_valid_end_months(start_year, start_month, end_year, max_year, max_date):
            if end_year == start_year and end_year == max_year:
                return [m for m in range(start_month, max_date.month + 1)]
            elif end_year == start_year:
                return [m for m in range(start_month, 13)]
            elif end_year == max_year:
                return [m for m in range(1, max_date.month + 1)]
            else:
                return list(range(1, 13))

        # Prefix for sector compare dashboard
        SECPREFIX = "sector_compare_"

        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            with st.expander("Date Range", expanded=user_filters.get(SECPREFIX + "expand_filters", True)):
                # 1. Combine OPENED and CLOSED data for true date bounds
                combined_data = pd.concat([opened_df, closed_df])
                combined_data['Period'] = pd.to_datetime(combined_data['Period'])
                opened_df['Period'] = pd.to_datetime(opened_df['Period'])
                closed_df['Period'] = pd.to_datetime(closed_df['Period'])

                min_available_date = combined_data['Period'].min()
                max_available_date = combined_data['Period'].max()

                # Default 12 month window
                default_end_date = max_available_date
                default_start_date = (default_end_date - pd.DateOffset(months=11)).replace(day=1)

                # Retrieve or assign user filter vals
                start_year = user_filters.get(SECPREFIX + "start_year", default_start_date.year)
                start_month = user_filters.get(SECPREFIX + "start_month", default_start_date.month)
                end_year = user_filters.get(SECPREFIX + "end_year", default_end_date.year)
                end_month = user_filters.get(SECPREFIX + "end_month", default_end_date.month)

                min_year = min_available_date.year
                max_year = max_available_date.year
                all_years = list(range(min_year, max_year + 1))

                # --- START DATE PICKERS ---
                sm_col, sy_col = st.columns([1.3, 1])
                with sy_col:
                    if start_year not in all_years:
                        start_year = all_years[0]
                    start_year = st.selectbox(
                        "Start Year",
                        options=all_years,
                        index=all_years.index(start_year),
                        key=SECPREFIX + "start_year_select"
                    )
                with sm_col:
                    valid_start_months = determine_valid_start_months(start_year, min_year, max_year, min_available_date, max_available_date)
                    if start_month not in valid_start_months:
                        start_month = valid_start_months[0]
                    start_month = st.selectbox(
                        "Start Month",
                        options=valid_start_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_start_months.index(start_month),
                        key=SECPREFIX + "start_month_select"
                    )

                # --- END DATE PICKERS ---
                em_col, ey_col = st.columns([1.3, 1])
                valid_end_years = determine_valid_end_years(start_year, min_year, max_year)
                if end_year not in valid_end_years:
                    end_year = valid_end_years[0]

                with ey_col:
                    end_year = st.selectbox(
                        "End Year",
                        options=valid_end_years,
                        index=valid_end_years.index(end_year),
                        key=SECPREFIX + "end_year_select"
                    )

                valid_end_months = determine_valid_end_months(start_year, start_month, end_year, max_year, max_available_date)
                if end_month not in valid_end_months:
                    end_month = valid_end_months[0]
                with em_col:
                    end_month = st.selectbox(
                        "End Month",
                        options=valid_end_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_end_months.index(end_month),
                        key=SECPREFIX + "end_month_select"
                    )
                date_changed = (
                    user_filters.get(SECPREFIX + "start_year") != start_year or
                    user_filters.get(SECPREFIX + "start_month") != start_month or
                    user_filters.get(SECPREFIX + "end_year") != end_year or
                    user_filters.get(SECPREFIX + "end_month") != end_month
                )
                print("Date changed: ", date_changed)
                # Save selections
                user_filters[SECPREFIX + "start_year"] = start_year
                user_filters[SECPREFIX + "start_month"] = start_month
                user_filters[SECPREFIX + "end_year"] = end_year
                user_filters[SECPREFIX + "end_month"] = end_month

                # --- Compose/Clamp final date range ---
                candidate_start_date = pd.Timestamp(date(start_year, start_month, 1))
                end_day = calendar.monthrange(end_year, end_month)[1]
                candidate_end_date = pd.Timestamp(date(end_year, end_month, end_day))
                new_start_date = max(candidate_start_date, min_available_date)
                new_end_date = min(candidate_end_date, max_available_date)

                user_filters[SECPREFIX + "start_date"] = new_start_date
                user_filters[SECPREFIX + "end_date"] = new_end_date

                # **Filtered for chaining next filter steps:**
                date_filtered_opened = opened_df[
                    (opened_df['Period'] >= new_start_date) & (opened_df['Period'] <= new_end_date)
                ]
                date_filtered_closed = closed_df[
                    (closed_df['Period'] >= new_start_date) & (closed_df['Period'] <= new_end_date)
                ]

                if date_changed:
                    print("Date changed")
                    # Reset all filters in user_filters
                    user_filters[SECPREFIX + "selected_sectors"] = []
                    user_filters[SECPREFIX + "parent_chain_name"] = []
                    user_filters[SECPREFIX + "selected_chain_name"] = []
                    user_filters[SECPREFIX + "selected_state_name"] = ["All"]
                    user_filters[SECPREFIX + "selected_msa_name"] = ["All"]
                    user_filters[SECPREFIX + "selected_zip_code"] = ["All"]
                    # user_filters[SECPREFIX + "selected_postal_codes"] = ["All"] 
                     # Also update session state to ensure UI consistency
                    st.session_state[SECPREFIX + "selected_sectors"] = []
                    st.session_state[SECPREFIX + "parent_chain_name"] = []
                    st.session_state[SECPREFIX + "selected_chain_name"] = []
                    st.session_state[SECPREFIX + "selected_state_name"] = ["All"]
                    st.session_state[SECPREFIX + "selected_msa_name"] = ["All"]
                    st.session_state[SECPREFIX + "selected_zip_code"] = ["All"]
                    # st.session_state[SECPREFIX + "selected_postal_codes"] = ["All"]
                    # Save auth cookie and rerun
                    safe_cookie = make_json_safe(auth_cookie)
                    auth_cookie = safe_cookie
                    save_auth_cookie()
                    st.rerun()
    
        with col2:
            with st.expander("Sector Comparison", expanded=user_filters.get(SECPREFIX + "expand_filters", True)):
                # Use date_filtered_opened/date_filtered_closed, which have already been filtered by date in col1
                date_filtered_combined = pd.concat([date_filtered_opened, date_filtered_closed])

                # All valid sectors in the filtered range
                all_sectors = date_filtered_combined["Sector_Coresight"].dropna().unique().tolist()
                others_items = [s for s in all_sectors if s and s.lower() in ["other", "others"]]
                regular_items = [s for s in all_sectors if s and s.lower() not in ["other", "others"]]
                regular_items = sorted(regular_items, key=lambda x: x.lower())
                sectors = regular_items + others_items

                logging.info(f"Available sectors in date range: {sectors}")

                # Validate previous selected
                current_selected = user_filters.get(SECPREFIX + "selected_sectors", [])
                if current_selected:
                    valid_selected = [s for s in current_selected if not sectors or s in sectors]
                    if not valid_selected:
                        valid_selected = current_selected
                else:
                    valid_selected = []

                selected_sectors = st.multiselect(
                    "Select Sectors to Compare (minimum 2)",
                    options=sectors,
                    default=valid_selected,
                    key=SECPREFIX + "sector_multiselect"
                )

                # Save filter change if changed
                if selected_sectors != current_selected:
                    logging.info(f"Sector selection changed from {current_selected} to {selected_sectors}")
                    user_filters[SECPREFIX + "selected_sectors"] = selected_sectors
                    user_filters[SECPREFIX + "selected_state_name"] = ["All"]
                    user_filters[SECPREFIX + "selected_msa_name"] = ["All"]
                    user_filters[SECPREFIX + "selected_zip_code"] = ["All"]
                    user_filters[SECPREFIX + "selected_chain_name"] = []
                    user_filters[SECPREFIX + "parent_chain_name"] = []
                    safe_cookie = make_json_safe(auth_cookie)
                    auth_cookie = safe_cookie
                    save_auth_cookie()
                    st.rerun()

                # ---- Produce *sector_filtered* variants for use by col3/col4/logic ----
                if selected_sectors:
                    sector_filtered_opened = date_filtered_opened[
                        date_filtered_opened["Sector_Coresight"].isin(selected_sectors)
                    ]
                    sector_filtered_closed = date_filtered_closed[
                        date_filtered_closed["Sector_Coresight"].isin(selected_sectors)
                    ]
                else:
                    # If nothing selected, use nothing (or all? - but this keeps cascading filters correct!)
                    sector_filtered_opened = date_filtered_opened.iloc[0:0]
                    sector_filtered_closed = date_filtered_closed.iloc[0:0]

        SECPREFIX = "sector_compare_"

        with col3:
            with st.expander("Retailers", expanded=user_filters.get(SECPREFIX + "expand_filters", True)):
                sector_filtered_combined = pd.concat([sector_filtered_opened, sector_filtered_closed])
                if user_filters.get(SECPREFIX + "selected_sectors"):
                    
                    # Null-safe parent name
                    sector_filtered_combined['ParentName_Coresight'] = sector_filtered_combined['ParentName_Coresight'].where(
                        pd.notna(sector_filtered_combined['ParentName_Coresight']), "No Parent Retailer"
                    )

                    parent_names = sorted(sector_filtered_combined['ParentName_Coresight'].unique().tolist())
                    if "No Parent Retailer" in parent_names:
                        parent_names.remove("No Parent Retailer")
                        parent_names.append("No Parent Retailer")

                    # Multiselect Parent
                    current_parent_selection = user_filters.get(SECPREFIX + "parent_chain_name", [])
                    valid_parent_selection = [p for p in current_parent_selection if p in parent_names]
                    parent_chain_selection = st.multiselect(
                        "Select Company",
                        options=parent_names,
                        default=valid_parent_selection,
                        help="Select retailers to filter",
                        key=SECPREFIX + "parent_chain_name_select",
                        disabled=False
                    )

                    if parent_chain_selection != valid_parent_selection:
                        user_filters[SECPREFIX + "parent_chain_name"] = parent_chain_selection
                        user_filters[SECPREFIX + "selected_state_name"] = ["All"]
                        user_filters[SECPREFIX + "selected_msa_name"] = ["All"]
                        user_filters[SECPREFIX + "selected_zip_code"] = ["All"]
                        safe_cookie = make_json_safe(auth_cookie)
                        auth_cookie = safe_cookie
                        save_auth_cookie()
                        st.rerun()

                    # Filter for banners
                    if parent_chain_selection:
                        retailer_filtered_combined = sector_filtered_combined[
                            sector_filtered_combined["ParentName_Coresight"].isin(parent_chain_selection)
                        ]
                    else:
                        retailer_filtered_combined = sector_filtered_combined.copy()

                    chain_names = sorted(retailer_filtered_combined['ChainName_Coresight'].dropna().unique().tolist())

                    st.markdown(
                        "<h6 style='text-align: center; margin-top: -10px; margin-bottom: -100px;'>or</h6>",
                        unsafe_allow_html=True
                    )

                    current_chain_selection = user_filters.get(SECPREFIX + "selected_chain_name", [])
                    valid_chain_selection = [c for c in current_chain_selection if c in chain_names]
                    chain_selection = st.multiselect(
                        "Select Banner/Brand",
                        options=chain_names,
                        default=valid_chain_selection,
                        help="Select banners/brands to filter",
                        key=SECPREFIX + "selected_chain_name_select",
                        disabled=False
                    )

                    if chain_selection != valid_chain_selection:
                        user_filters[SECPREFIX + "selected_chain_name"] = chain_selection
                        user_filters[SECPREFIX + "selected_state_name"] = ["All"]
                        user_filters[SECPREFIX + "selected_msa_name"] = ["All"]
                        user_filters[SECPREFIX + "selected_zip_code"] = ["All"]
                        safe_cookie = make_json_safe(auth_cookie)
                        auth_cookie = safe_cookie
                        save_auth_cookie()
                        st.rerun()
                else:
                    st.multiselect(
                        "Select Company", options=[], default=[], disabled=True,
                        help="Please select sectors first"
                    )
                    st.multiselect(
                        "Select Banner/Brand", options=[], default=[], disabled=True,
                        help="Please select sectors first"
                    )
                    # No filtered data produced if disabled
                    retailer_filtered_combined = sector_filtered_combined.iloc[0:0]

                # ---- FOR NXT FILTERS: split opened/closed ---
                if user_filters.get(SECPREFIX + "selected_sectors"):
                    if parent_chain_selection or chain_selection:
                        # Further filter to banners if selected
                        if chain_selection:
                            retailer_filtered = retailer_filtered_combined[
                                retailer_filtered_combined['ChainName_Coresight'].isin(chain_selection)
                            ]
                        else:
                            retailer_filtered = retailer_filtered_combined
                    else:
                        retailer_filtered = retailer_filtered_combined
                    # Now split out for opened/closed for col4
                    retailer_filtered_opened = retailer_filtered[
                        retailer_filtered['data_from'] == 'opened'
                    ] if not retailer_filtered.empty else retailer_filtered_combined.iloc[0:0]
                    retailer_filtered_closed = retailer_filtered[
                        retailer_filtered['data_from'] == 'closed'
                    ] if not retailer_filtered.empty else retailer_filtered_combined.iloc[0:0]
                else:
                    retailer_filtered_opened = sector_filtered_opened.iloc[0:0]
                    retailer_filtered_closed = sector_filtered_closed.iloc[0:0]

        with col4:
            with st.expander("Location", expanded=user_filters.get(SECPREFIX + "expand_filters", True)):
                def mutually_exclusive_all(selected, all_option, valid_options):
                    if all_option in selected and len(selected) > 1:
                        if selected[-1] == all_option:
                            return [all_option]
                        else:
                            return [s for s in selected if s != all_option]
                    elif not selected:
                        return [all_option]
                    else:
                        return selected

                retailer_filtered_combined = pd.concat([retailer_filtered_opened, retailer_filtered_closed])

                # --- STATE MULTISELECT ---
                if not retailer_filtered_combined.empty:
                    valid_states = sorted(retailer_filtered_combined['State'].dropna().unique().tolist())
                else:
                    valid_states = []
                state_options = ["All"] + valid_states
                all_option = "All"
                uf_state_name = user_filters.get(SECPREFIX + "selected_state_name", [])
                uf_state_name = [s for s in uf_state_name if s == all_option or s in valid_states]
                if not uf_state_name or set(uf_state_name) == set(valid_states):
                    default_states = [all_option]
                else:
                    default_states = uf_state_name

                updated_selected_states = st.multiselect(
                    "Select State",
                    options=state_options,
                    default=default_states,
                    help="U.S. state where the store is located.",
                    key=SECPREFIX + "multiselect_state",
                    disabled=not len(user_filters[SECPREFIX + "selected_sectors"]) >= 2
                )
                final_selected_states = mutually_exclusive_all(updated_selected_states, all_option, valid_states)
                if final_selected_states != uf_state_name:
                    user_filters[SECPREFIX + "selected_state_name"] = final_selected_states
                    user_filters[SECPREFIX + "selected_msa_name"] = ["All"]
                    safe_cookie = make_json_safe(auth_cookie)
                    auth_cookie = safe_cookie
                    save_auth_cookie()
                    st.rerun()
                    
                # --- MSA MULTISELECT ---
                if "All" in user_filters.get(SECPREFIX + "selected_state_name", []):
                    location_state_filtered = retailer_filtered_combined
                else:
                    location_state_filtered = retailer_filtered_combined[
                        retailer_filtered_combined["State"].isin(user_filters[SECPREFIX + "selected_state_name"])
                    ]
                st.markdown("""
                <style>
                .st-emotion-cache-wfksaw {
                    gap: 0rem !important; /* Reduce the gap to a smaller value */
                }
                </style>
                """, unsafe_allow_html=True)
                # _, center_col, _ = st.columns([20, 80, 10])
                # with center_col:
                # location_type = "MSA"
                # if location_type == "MSA":
                if not location_state_filtered.empty:
                    valid_msas = sorted(location_state_filtered['MsaName'].dropna().unique().tolist())
                else:
                    valid_msas = []
                filter_options = ["All"] + valid_msas
                current_filter = user_filters.get(SECPREFIX + "selected_msa_name", ["All"])
                multiselect_label = "Select Metropolitan Statistical Area (MSA)"
                filter_key = SECPREFIX + "multiselect_msa"
                filter_column = "MsaName"
                
                if "All" in current_filter:
                    current_filter = ["All"]
                else:
                    valid_filter = [f for f in current_filter if f in filter_options]
                    current_filter = valid_filter if valid_filter else ["All"]

                updated_selected_filter = st.multiselect(
                    multiselect_label,
                    options=filter_options,
                    default=current_filter,
                    help="A metro region centered on a large city plus its economically linked suburbs.",
                    key=filter_key,
                    disabled=not len(user_filters[SECPREFIX + "selected_sectors"]) >= 2
                )
                final_selected_filter = mutually_exclusive_all(updated_selected_filter, all_option, filter_options[1:])

                # Update user_filters for MSA
                if final_selected_filter != user_filters.get(SECPREFIX + "selected_msa_name"):
                    user_filters[SECPREFIX + "selected_msa_name"] = final_selected_filter
                    # Reset Postal Code when MSA changes
                    user_filters[SECPREFIX + "selected_zip_code"] = ["All"]
                    safe_cookie = make_json_safe(auth_cookie)
                    auth_cookie = safe_cookie
                    save_auth_cookie()
                    st.rerun()
                
                # --- POSTAL CODE MULTISELECT ---
                # Filter data based on selected MSAs for Postal Codes
                if "All" in user_filters.get(SECPREFIX + "selected_msa_name", []):
                    location_msa_filtered = location_state_filtered
                else:
                    location_msa_filtered = location_state_filtered[
                        location_state_filtered["MsaName"].isin(user_filters[SECPREFIX + "selected_msa_name"])
                    ]
                
                if not location_msa_filtered.empty:
                    valid_zips = sorted(
                        {str(z).strip() for z in location_msa_filtered['PostalCode'].dropna().unique().tolist()
                         if str(z).strip() != "0" and str(z).strip() != ""}
                    )
                else:
                    valid_zips = []
                zip_options = ["All"] + valid_zips
                current_zip_filter = user_filters.get(SECPREFIX + "selected_zip_code", ["All"])
                if "All" in current_zip_filter:
                    current_zip_filter = ["All"]
                else:
                    valid_zip_filter = [z for z in current_zip_filter if z in zip_options]
                    current_zip_filter = valid_zip_filter if valid_zip_filter else ["All"]
                
                updated_selected_zip = st.multiselect(
                    "Select Zip Code",
                    options=zip_options,
                    default=current_zip_filter,
                    help="U.S. postal area where the store is located.",
                    key=SECPREFIX + "multiselect_zip",
                    disabled=not len(user_filters[SECPREFIX + "selected_sectors"]) >= 2
                )
                final_selected_zip = mutually_exclusive_all(updated_selected_zip, all_option, valid_zips)
                
                # Update user_filters for Postal Code
                if final_selected_zip != user_filters.get(SECPREFIX + "selected_zip_code"):
                    user_filters[SECPREFIX + "selected_zip_code"] = final_selected_zip
                    safe_cookie = make_json_safe(auth_cookie)
                    auth_cookie = safe_cookie
                    save_auth_cookie()
                    st.rerun()

        # Chart logic
        import plotly.express as px

        selected_sectors = user_filters.get(SECPREFIX + "selected_sectors", [])
        if isinstance(selected_sectors, str):
            selected_sectors = [selected_sectors]

        if not selected_sectors or len(selected_sectors) < 2:
            st.info("Please select at least two sectors to start the sector comparison")
        else:
            st.write(f"Comparing sectors: {' vs '.join(selected_sectors)}")
            try:
                # Get primary filters from user_filters/session/whatever storage you use
                selected_sectors = user_filters.get(SECPREFIX + "selected_sectors", [])
                selected_states = user_filters.get(SECPREFIX + "selected_state_name", ["All"])
                selected_msas   = user_filters.get(SECPREFIX + "selected_msa_name", ["All"])
                selected_zip_codes = user_filters.get(SECPREFIX + "selected_zip_code", ["All"])
                selected_parents = user_filters.get(SECPREFIX + "parent_chain_name", [])
                selected_chains = user_filters.get(SECPREFIX + "selected_chain_name", [])

                # Union chain picking (as in your pattern)
                selected_chain_names_union = set(selected_chains)
                if selected_parents:
                    parent_filtered = pd.concat([sector_filtered_opened, sector_filtered_closed])
                    parent_filtered = parent_filtered[parent_filtered['ParentName_Coresight'].isin(selected_parents)]
                    parent_chain_list = parent_filtered['ChainName_Coresight'].dropna().unique().tolist()
                    selected_chain_names_union.update(parent_chain_list)

                # 1️⃣ SECTOR-LEVEL DATA — use only these filters for “top” charts
                sector_compare_data = pd.concat([sector_filtered_opened, sector_filtered_closed]).copy()
                sector_compare_data = sector_compare_data[sector_compare_data['Sector_Coresight'].isin(selected_sectors)]
                if "All" not in selected_states:
                    sector_compare_data = sector_compare_data[sector_compare_data['State'].isin(selected_states)]
                if "All" not in selected_msas:
                    sector_compare_data = sector_compare_data[sector_compare_data['MsaName'].isin(selected_msas)]
                if "All" not in selected_zip_codes:
                    sector_compare_data = sector_compare_data[sector_compare_data['PostalCode'].astype(str).str.strip().isin([str(z).strip() for z in selected_zip_codes])]

                # 2️⃣ RETAILER/BANNER DATA — use all filters (children of sector_compare_data)
                retailer_compare_data = sector_compare_data.copy()
                if selected_chain_names_union:
                    retailer_compare_data = retailer_compare_data[retailer_compare_data["ChainName_Coresight"].isin(selected_chain_names_union)]

                # Defensive: If none left, warn and stop (only for the chart you are plotting)
                if sector_compare_data.empty:
                    st.warning("No data available for the selected filters.")
                    st.stop()

                # ------ Chart logic for FIRST TWO charts: sector_compare_data ONLY -------
                color_map = {
                    sector: color for sector, color in zip(
                        selected_sectors, px.colors.qualitative.Plotly
                    )
                }
                kpi1, kpi2 = st.columns([1, 1])
                with kpi1:
                    st.metric("Net Openings", f"{(sector_filtered_opened.shape[0] - sector_filtered_closed.shape[0]):,}")
                with kpi2:
                    st.metric("Sectors Shown", f"{len(selected_sectors):,}")

                line_col, bar_col = st.columns([4,2])
                with line_col:
                    # Add net column
                    sector_compare_data['is_opened'] = (sector_compare_data['data_from'] == 'opened').astype(int)
                    sector_compare_data['is_closed'] = (sector_compare_data['data_from'] == 'closed').astype(int)
                    sector_compare_data['net'] = sector_compare_data['is_opened'] - sector_compare_data['is_closed']

                    grouped = (
                        sector_compare_data.groupby(['Period', 'Sector_Coresight'])['net'].sum().reset_index()
                    )
                    all_periods = pd.date_range(
                        start=sector_compare_data['Period'].min(),
                        end=sector_compare_data['Period'].max(), freq='MS'
                    )
                    full_data = pd.DataFrame()
                    for sector in selected_sectors:
                        sector_g = grouped[grouped['Sector_Coresight'] == sector].set_index('Period').reindex(all_periods, fill_value=0).reset_index()
                        sector_g['Sector_Coresight'] = sector
                        full_data = pd.concat([full_data, sector_g])
                    fig_line = px.line(
                        full_data,
                        x='index', y='net', color='Sector_Coresight',
                        title='Monthly Net Store Openings by Sector',
                        labels={'net': 'Net Openings', 'index': 'Month', 'Sector_Coresight': 'Sector'},
                        color_discrete_map=color_map
                    )
                    fig_line.update_traces(mode='lines+markers+text', textposition='top center')
                    fig_line.update_xaxes(tickformat="%b %Y", tickangle=45)
                    fig_line.update_layout(
                        legend=dict(
                            x=0.99,
                            y=0.99,
                            xanchor='right',
                            yanchor='top',
                            bgcolor='rgba(0,0,0,0)',
                            bordercolor='rgba(0,0,0,0)',
                            borderwidth=0,
                            font=dict(size=13)
                        ),
                        margin=dict(l=10, r=10, t=60, b=10),  # Increased top margin to 60 and added other margins
                        height=380  # Added height to accommodate the larger top margin
                    )
                    st.plotly_chart(fig_line, use_container_width=True)

                with bar_col:
                    sector_net_totals = (
                        sector_compare_data.groupby('Sector_Coresight')['net'].sum().reset_index()
                        .rename(columns={'net': 'Net Openings'})
                    )
                    fig_bar = px.bar(
                        sector_net_totals, x='Sector_Coresight', y='Net Openings', color='Sector_Coresight',
                        labels={'Sector_Coresight':'Sector', 'Net Openings':'Net Openings'},
                        color_discrete_map=color_map,
                        title='Total Net Openings by Sector'
                    )
                    fig_bar.update_traces(texttemplate='%{y:,}', textposition='outside')
                    fig_bar.update_layout(
                        showlegend=False,
                        height=500
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)

                # ------ Banner/parent performance charts (use retailer_compare_data only!) -------
                st.subheader("Banner Performance by Sector")

                # Ensure 'net' column exists in retailer_compare_data (in case of no retailer filters this equals sector_compare_data)
                if 'net' not in retailer_compare_data.columns:
                    retailer_compare_data['is_opened'] = (retailer_compare_data['data_from'] == 'opened').astype(int)
                    retailer_compare_data['is_closed'] = (retailer_compare_data['data_from'] == 'closed').astype(int)
                    retailer_compare_data['net'] = retailer_compare_data['is_opened'] - retailer_compare_data['is_closed']

                for sector in selected_sectors:
                    sector_data = retailer_compare_data[retailer_compare_data['Sector_Coresight'] == sector]
                    if sector_data.empty:
                        continue
                    banner_totals = (
                        sector_data.groupby('ChainName_Coresight')['net'].sum().reset_index()
                        .rename(columns={'net': 'Net Openings'})
                        .sort_values('Net Openings', ascending=False)
                    )
                    banner_totals = banner_totals[banner_totals['ChainName_Coresight'].notnull()]
                    title = f'Top Banners in {sector}'

                    fig_retailer = px.bar(
                        banner_totals,
                        x='ChainName_Coresight', y='Net Openings',
                        labels={'ChainName_Coresight':'Banners', 'Net Openings':'Net Openings'},
                        title=title,
                        color_discrete_sequence=[color_map[sector]],
                        height=400
                    )
                    fig_retailer.update_traces(
                        texttemplate='%{y}', textposition='outside', cliponaxis=True
                    )
                    fig_retailer.update_layout(
                        xaxis={'categoryorder': 'total descending'},
                        yaxis=dict(showticklabels=False, showgrid=False, title=None),
                        showlegend=False,
                        height=400,
                        margin=dict(t=60, b=40, l=10, r=10)
                    )
                    st.plotly_chart(fig_retailer, use_container_width=True)

            except Exception as e:
                st.error(f"An error occurred: {e}")

        st.markdown("---")

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
    include_html("footer.html")
except ValueError as e:
    st.write(f"{e}:No data available. Please reset filters or refresh page.")