# Version 2.2.3
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
    from sqlalchemy import create_engine, text
    import calendar
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
    from rapidfuzz import process, fuzz
    import re
    from html_utils import include_html
    from streamlit.components.v1 import html
    from datetime import datetime
    import time
    from st_aggrid import AgGrid
    from st_aggrid.shared import JsCode
    from streamlit_cookies_controller import CookieController
    import base64, pathlib
    from pathlib import Path
    import streamlit.components.v1 as components
    from st_aggrid.grid_options_builder import GridOptionsBuilder
    from auth_utils import logout,require_auth,make_json_safe,get_current_domain
    import json
    import time
    import numpy as np
    session_id = require_auth()
  
    cookie_controller = CookieController(key="auth_cookies") 

    load_dotenv()

    st.set_page_config(
        page_title="Store Openings",  # Title of the app
        layout="wide",                 # Use the 'wide' layout
        page_icon = "https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
        initial_sidebar_state="collapsed"  # Sidebar state (optional)
    )

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

    import pandas as pd
    from typing import Optional

    def datefmt(x, fmt: str, default: str = "N/A") -> str:
        """
        Safely format a single value to a date string.
        Accepts datetime/date/str/np.datetime64/pd.Timestamp/None/NaT.
        Returns `default` if not parseable.
        """
        if x is None:
            return default
        if hasattr(x, "strftime"):
            try:
                return x.strftime(fmt)
            except Exception:
                pass

        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return default
        try:
            return dt.strftime(fmt)
        except Exception:
            # handle rare tz-aware cases
            try:
                return dt.tz_localize(None).strftime(fmt)  # type: ignore[attr-defined]
            except Exception:
                return default

    def datefmt_series(s: pd.Series, fmt: str, default: Optional[str] = None) -> pd.Series:
        """
        Vectorized safe formatter for a pandas Series.
        Returns strings; if `default` is provided, fills NaT rows with it.
        """
        out = pd.to_datetime(s, errors="coerce").dt.strftime(fmt)
        return out.fillna(default) if default is not None else out



    STRICT_AUTH = os.getenv("AUTH_STRICT", "1") == "1"

    raw = cookie_controller.get("auth_data")

    try:
        auth_cookie = json.loads(raw) if isinstance(raw, str) else (raw if isinstance(raw, dict) else {})
    except Exception:
        auth_cookie = {}
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
    def _trial_modal_opened():

        ICON = pathlib.Path(__file__).resolve().parent.parent / "assets" / "icons" / "lock.png"
        lock_b64 = base64.b64encode(ICON.read_bytes()).decode("utf-8")

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
    col_space,col4, col1, col2, col3  = st.columns([56, 14, 14, 16, 8]) 
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
            user_filters["returnPage"] = "opening"
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

    # st.cache_data.clear()   

    today = dt.date.today()

    if "last_clear_date" not in st.session_state:
        st.session_state.last_clear_date = today

    if st.session_state.last_clear_date != today:
        st.cache_data.clear()
        st.session_state.last_clear_date = today
        st.toast("Cache cleared at midnight")

    # Cached function to fetch data from the database
    @st.cache_data(show_spinner=False)
    def fetch_data():


        # Connect to the database
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

        opened_query = """SELECT o.storename, o.storetype, o.ChainName_Coresight, o.ParentName_Coresight, 
                                o.Address, o.Address2, o.City, o.MsaName, o.PostalCode, o.State, o.Country, 
                                o.Sector_Coresight, o.Period, o.Population, p.firstappeareddate_chainxy, o.UpdateCycle
                            FROM (
                                SELECT storename, storetype, ChainName_Coresight, ParentName_Coresight, 
                                       Address, Address2, City, MsaName, PostalCode, State, Country, 
                                       Sector_Coresight, Period, Population, status, UpdateCycle, duration_reopening
                                FROM all_opened_py

                                UNION ALL

                                SELECT storename, storetype, ChainName_Coresight, ParentName_Coresight, 
                                       Address, Address2, City, MsaName, PostalCode, State, Country, 
                                       Sector_Coresight, Period, Population, status, UpdateCycle, duration_reopening
                                FROM all_opened_cy

                                UNION ALL

                                SELECT storename, storetype, ChainName_Coresight, ParentName_Coresight, 
                                       Address, Address2, City, MsaName, PostalCode, State, Country, 
                                       Sector_Coresight, Period, Population, status, UpdateCycle, duration_reopening
                                FROM all_opened_acquisition
                            ) o
                            -- LEFT JOIN
                            -- (
                            --     SELECT DISTINCT
                            --            LOWER(TRIM(Address)) AS norm_addr,
                            --            LOWER(TRIM(City))    AS norm_city,
                            --            LOWER(TRIM(State))   AS norm_state,
                            --            ChainName_Coresight  AS chain_from_acq
                            --     FROM   all_active_acquisition
                            --     WHERE  Period = DATE '2024-04-01'
                            -- ) AS ac
                            --    ON  LOWER(TRIM(o.Address)) = ac.norm_addr
                            --    AND LOWER(TRIM(o.City))    = ac.norm_city
                            --    AND LOWER(TRIM(o.State))   = ac.norm_state
                            LEFT JOIN parent_chain_names_data p 
                              ON o.chainname_coresight = p.chainname_coresight
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

        # Run the query and read into DataFrame
        opened_df = pd.read_sql(opened_query, con=conn)
                # Denominator: TOTAL state population from your population table (sum of unique ZIPs per state)

        acquisition_query = """
            SELECT Address, City, State
            FROM all_active_acquisition
            WHERE Period = '2024-04-01'
        """

        acq_df = pd.read_sql(acquisition_query, con=conn)
        acq_df.dropna(subset=['Address', 'City', 'State'], inplace=True)

        opened_df['Period'] = pd.to_datetime(opened_df['Period'], errors='coerce')

        opened_may = opened_df[opened_df['Period'] == pd.Timestamp('2024-05-01')].copy()

        _abbr = {r'\bst\b':'street', r'\brd\b':'road', r'\bave\b':'avenue',
                 r'\bblvd\b':'boulevard', r'\bdr\b':'drive', r'\bln\b':'lane',
                 r'\bhwy\b':'highway', r'\bpkwy\b':'parkway',
                 r'\bn\b':'north', r'\bs\b':'south', r'\be\b':'east', r'\bw\b':'west'}
        street_words = ('street','road','avenue','boulevard','drive','lane',
                        'highway','parkway','route','court','place')
        _rx_suffix = re.compile(rf'\b({"|".join(street_words)})\b$')

        def norm(addr: str) -> str:
            addr = re.sub(r'[^\w\s]', ' ', str(addr).lower())
            addr = re.sub(r'\s+', ' ', addr).strip() + ' '
            for pat, repl in _abbr.items():
                addr = re.sub(pat + r'(?=\s)', repl + ' ', addr)
            addr = re.sub(r'\b(l|la|le|de|di|du|del|dela)\s+(?=[a-z])', r'\1', addr)
            addr = _rx_suffix.sub('', addr)
            return re.sub(r'\s+', ' ', addr).strip()

        opened_may['norm_addr'] = opened_may['Address'].map(norm)
        acq_df['norm_addr']     = acq_df['Address'].map(norm)

        acq_exact = set(acq_df['norm_addr'])
        opened_may['is_exact_acq'] = opened_may['norm_addr'].isin(acq_exact)

        unmatched = opened_may.loc[~opened_may['is_exact_acq']].copy()
        if not unmatched.empty:
            scores = process.cdist(
                unmatched['norm_addr'].values,
                acq_df['norm_addr'].values,
                scorer=fuzz.token_set_ratio,
                score_cutoff=80
            )
            unmatched['is_fuzzy_acq'] = (scores.max(axis=1) >= 80)
        else:
            unmatched['is_fuzzy_acq'] = False

        # merge fuzzy flag back
        opened_may = opened_may.merge(
            unmatched[['Address', 'is_fuzzy_acq']],
            on='Address', how='left'
        )
        opened_may['is_acq'] = opened_may['is_exact_acq'] | opened_may['is_fuzzy_acq'].fillna(False)
        opened_may_clean = opened_may.loc[~opened_may['is_acq']]
        opened_rest      = opened_df.loc[opened_df['Period'] != pd.Timestamp('2024-05-01')]
        opened_df    = pd.concat([opened_rest, opened_may_clean], ignore_index=True)

        # Convert 'Period' to datetime format
        opened_df['Period'] = pd.to_datetime(opened_df['Period'], errors='coerce').dt.date

        # Clean and convert 'UpdateCycle' to integer, handling NaN values
        opened_df['UpdateCycle'] = (
            opened_df['UpdateCycle']
            .fillna(30)
            .astype(int)
        )

        return opened_df

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
                <a href="/net#store-intelligence-platform" target="_self">Net Openings</a>
                <a href="/opening#store-intelligence-platform" target="_self" class="active">Store Openings</a>
                <a href="/closing#store-intelligence-platform" target="_self">Store Closures</a>
                <a href="/active#store-intelligence-platform" target="_self">Active Stores</a>
            </div>
        </div>
        """, unsafe_allow_html=True)


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

    # Create placeholder
    placeholder = st.empty()

    # Show spinner
    with placeholder.container():
        st.markdown("<br><br>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([2, 3, 2])
        with col2:
            html(transforming_spinner_html, height=200, scrolling=False)

    # Load data
    opened_data = fetch_data()

    st.components.v1.html("""
    <script>
    parent.postMessage('complete-spinner', '*');
    </script>
    """, height=0)


    st.markdown("<div style='margin-top: 10px;'></div>", unsafe_allow_html=True)

    latest_ts = opened_data["Period"].max()
    latest_ts = datefmt(latest_ts, "%B %Y")

    top_col1, top_col2, top_col3 = st.columns([78, 10, 12])  # Adjust sizes for alignment as you wish

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
            max_period = opened_data['Period'].max()
            
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
                user_filters["selected_zip_code"] = ["All"]
                
                
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
                st.session_state["selected_state_v2"] = ["All"]
                st.session_state["selected_msa_v2"] = ["All"]
                
                # Clear widget keys for Compare Retailers (except sector_filter)
                widget_keys = [
                    "selected_parent_names_v2", 
                    "selected_chain_names_v2",
                    "selected_state_v2",
                    "selected_msa_v2"
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

        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            logging.info("--- DATE RANGE SECTION START ---")
            with st.expander("Date Range", expanded=st.session_state["expand_filters"]):
                selected_date_range, min_date, max_date = get_synchronized_date_range(
                    opened_data, default_months_back=12, label="opening.py"
                )
                start_date, end_date = selected_date_range
                logging.info(f"Initial date range: {start_date} to {end_date} (min: {min_date}, max: {max_date})")

                qparams = st.query_params
                start_date_param = qparams.get("start_date", [None])[0]
                end_date_param = qparams.get("end_date", [None])[0]
                logging.info(f"Received query params - start_date: {start_date_param}, end_date: {end_date_param}")

                # Initialize session state from query params or defaults
                try:
                    param_start = pd.to_datetime(start_date_param).date()
                    param_end = pd.to_datetime(end_date_param).date()
                    
                    logging.info(f"Successfully parsed dates - start: {param_start}, end: {param_end}")

                    st.session_state.setdefault("start_month", param_start.month)
                    st.session_state.setdefault("start_year", param_start.year)
                    st.session_state.setdefault("end_month", param_end.month)
                    st.session_state.setdefault("end_year", param_end.year)
                except Exception as e:
                    logging.warning(f"Failed to parse query params, using defaults. Error: {e}")
                    st.session_state.setdefault("start_month", start_date.month)
                    st.session_state.setdefault("start_year", start_date.year)
                    st.session_state.setdefault("end_month", end_date.month)
                    st.session_state.setdefault("end_year", end_date.year)

                logging.info(f"Initialized session state - month/year: {st.session_state['start_month']}/{st.session_state['start_year']} to {st.session_state['end_month']}/{st.session_state['end_year']}")

                min_year, max_year = min_date.year, max_date.year
                all_months = list(range(1, 13))
                all_years = list(range(min_year, max_year + 1))
                logging.info(f"Available years: {min_year} to {max_year}")

                # --- START DATE PICKERS ---
                sm_col, sy_col = st.columns([1.3, 1])
                with sm_col:
                    if st.session_state["start_year"] == min_year:
                        valid_start_months = list(range(min_date.month, 13))
                    elif st.session_state["start_year"] == max_year:
                        valid_start_months = list(range(1, max_date.month + 1))
                    else:
                        valid_start_months = all_months

                    logging.info(f"Valid start months for {st.session_state['start_year']}: {valid_start_months}")
                    
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

                valid_end_years = [y for y in all_years if st.session_state["start_year"] <= y <= max_year]
                if st.session_state["end_year"] not in valid_end_years:
                    logging.info(f"Adjusting end_year from {st.session_state['end_year']} to {valid_end_years[0]}")
                    st.session_state["end_year"] = valid_end_years[0]

                if st.session_state["end_year"] == st.session_state["start_year"]:
                    valid_end_months = list(range(st.session_state["start_month"], 13))
                elif st.session_state["end_year"] == max_year:
                    valid_end_months = list(range(1, max_date.month + 1))
                else:
                    valid_end_months = all_months

                if st.session_state["end_month"] not in valid_end_months:
                    logging.info(f"Adjusting end_month from {st.session_state['end_month']} to {valid_end_months[0]}")
                    st.session_state["end_month"] = valid_end_months[0]

                logging.info(f"Valid end months for {st.session_state['end_year']}: {valid_end_months}")

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

                new_start_date = max(date(st.session_state["start_year"], st.session_state["start_month"], 1), min_date)
                end_day = calendar.monthrange(st.session_state["end_year"], st.session_state["end_month"])[1]
                new_end_date = min(date(st.session_state["end_year"], st.session_state["end_month"], end_day), max_date)

                logging.info(f"Calculated new date range: {new_start_date} to {new_end_date}")
                logging.info(f"Previous date range: {start_date} to {end_date}")

                if (new_start_date != start_date or new_end_date != end_date):
                    logging.info("Date range changed - updating state and params")
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
            
            filtered_sector_data = opened_data
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

                # Update cookie if user changed selection, reset children, rerun
                if selected_sector_name != current_sector:
                    user_filters["selected_sector_name"] = selected_sector_name
                    user_filters["parent_chain_name"] = ["All"]
                    user_filters["selected_chain_name"] = ["All"]
                    user_filters["selected_state_name"] = ["All"]
                    user_filters["selected_msa_name"] = ["All"]
                    user_filters["selected_zip_code"] = ["All"]
                    st.session_state["parent_chain_name"] = ["All"]
                    st.session_state["selected_chain_name"] = ["All"]
                    st.session_state["selected_state_name"] = ["All"]
                    st.session_state["selected_msa_name"] = ["All"]
                    st.session_state["selected_zip_code"] = ["All"]

                    save_auth_cookie()
                    st.rerun()

            logging.info("--- SECTOR SECTION END ---")

        with col3:
            logging.info("--- RETAILERS SECTION START ---")

            start_timestamp = pd.Timestamp(start_date)
            end_timestamp = pd.Timestamp(end_date)
            opened_data = opened_data[
                (opened_data['Period'] >= start_timestamp) &
                (opened_data['Period'] <= end_timestamp)
            ]
            if "All" not in selected_sector_name:
                opened_data = opened_data[
                    opened_data["Sector_Coresight"].isin([selected_sector_name])
            ]
            opened_data['ParentName_Coresight'] = opened_data['ParentName_Coresight'].where(
                pd.notna(opened_data['ParentName_Coresight']), "No Parent Retailer")
            parent_names = opened_data['ParentName_Coresight'].unique().tolist()
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
                    user_filters["selected_chain_name"] = ["All"]
                    user_filters["selected_state_name"] = ["All"]
                    user_filters["selected_msa_name"] = ["All"]
                    save_auth_cookie()
                    st.rerun()
                # else:
                #     save_auth_cookie()



                # For downstream use
                # parent_chain_name = user_filters.get("parent_chain_name", ["All"])

                st.markdown(
                    """
                    <div style='text-align: center; font-size: 0.9rem; font-weight: 500; margin-top: -0.4rem; margin-bottom: -0.2rem; color: #444;'>or</div>
                    """,
                    unsafe_allow_html=True
                )

                # Filtering by parent_chain_name ONLY from cookie
                if "All" in parent_chain_name or not parent_chain_name:
                    filtered_data = opened_data
                else:
                    filtered_data = opened_data[opened_data["ParentName_Coresight"].isin(parent_chain_name)]

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
                    save_auth_cookie()
                    st.rerun()
                # else:
                #     save_auth_cookie()

                if "All" in selected_chain_name:
                    selected_chain_name = chain_names

                if "All" in parent_chain_name:
                    parent_chain_name = parent_names

            logging.info("--- RETAILERS SECTION END ---")

        with col4:
            logging.info("--- LOCATION SECTION START ---")
            with st.expander("Location", expanded=st.session_state["expand_filters"]):
                filtered_location_data = opened_data.copy()

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
                    user_filters["selected_msa_name"] = ["All"]  # reset MSA on state change
                    user_filters["selected_zip_code"] = ["All"]  # Reset postal code on state change
                    save_auth_cookie()
                    st.rerun()
                # else:
                #     save_auth_cookie()



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
                st.markdown("""
                <style>
                .st-emotion-cache-wfksaw {
                    gap: 0.2rem !important; /* Reduce the gap to a smaller value */
                }
                </style>
                """, unsafe_allow_html=True)
                # _, center_col, _ = st.columns([20, 80, 10])
                # # Ensure location_type persists across sessions
                if "location_type" not in user_filters:
                    user_filters["location_type"] = st.session_state.get("location_type_base_opening", "MSA")
                if "location_type_base_opening" not in st.session_state:
                    st.session_state["location_type_base_opening"] = user_filters.get("location_type", "MSA")

                # with center_col:
                #     # location_type = st.radio(
                #     #     "Filter by location",          # meaningful label for screen readers
                #     #     "MSA",
                #     #     horizontal=True,
                #     #     key="location_type_base_opening",
                #     #     label_visibility="collapsed"
                #     # )
                location_type = "MSA"

                if location_type != user_filters.get("location_type", "MSA"):
                        user_filters["location_type"] = location_type
                        cookie_controller.set(
                            "auth_data",
                            json.dumps(auth_cookie),                    # must be a string
                            expires=datetime.utcnow() + timedelta(days=30),
                            path="/",
                            domain=domain,
                        )
                        st.rerun()

                # --- LOCATION FILTERS (MSA / ZIP) ---
                if st.session_state["location_type_base_opening"] == "MSA":
                    # ------------- MSA OPTIONS -------------
                    msa_names = sorted(filtered_data_msa['MsaName'].dropna().unique().tolist())
                    msa_names = ["All"] + msa_names

                    current_msa = user_filters.get("selected_msa_name", ["All"])
                    if "All" not in msa_names:
                        msa_names.append("All")
                    if "All" in current_msa:
                        current_msa = ["All"]
                    else:
                        valid_msa = [m for m in current_msa if m in msa_names]
                        current_msa = valid_msa if valid_msa else ["All"]

                    selected_msa_name = st.multiselect(
                        "Select Metropolitan Statistical Area (MSA)",
                        msa_names, 
                        default=current_msa,
                        help="A metro region centered on a large city plus its economically linked suburbs.",
                        key="multiselect_msa"
                    )

                    # Clean selection
                    if len(selected_msa_name) > 1 and "All" in selected_msa_name:
                        if selected_msa_name[-1] == "All":
                            selected_msa_name = ["All"]
                        else:
                            selected_msa_name = [m for m in selected_msa_name if m != "All"]
                    elif not selected_msa_name:
                        selected_msa_name = ["All"]

                    # Persist & rerun if changed
                    if selected_msa_name != current_msa:
                        user_filters["selected_msa_name"] = selected_msa_name
                        user_filters["selected_zip_code"] = ["All"]  # Reset postal code on MSA change
                        save_auth_cookie()
                        st.rerun()
                    # else:
                    #     save_auth_cookie()

                    # Ensure downstream variables exist
                    selected_zip_code = user_filters.get("selected_zip_code", ["All"])
                # else:
                #     # ------------- ZIP CODE OPTIONS -------------
                #     # Restrict zip codes by selected_state_name (already pulled from cookie)
                #     if "All" in selected_state_name or not selected_state_name:
                #         zip_pool = filtered_location_data
                #     else:
                #         zip_pool = filtered_location_data[filtered_location_data['State'].isin(selected_state_name)]
                #     zip_codes = sorted(zip_pool['PostalCode'].dropna().astype(str).str.strip().unique().tolist())
                #     zip_codes = ["All"] + [zc for zc in zip_codes if zc != "0"]

                #     current_zip = user_filters.get("selected_zip_code", ["All"])
                #     if "All" in current_zip:
                #         current_zip = ["All"]
                #     else:
                #         valid_zip = [z for z in current_zip if z in zip_codes]
                #         current_zip = valid_zip if valid_zip else ["All"]

                #     selected_zip_code = st.multiselect(
                #         "Select Zip Code",
                #         zip_codes,
                #         default=current_zip,
                #         help="U.S. postal code where the store is located.",
                #         key="multiselect_zip_code"
                #     )

                #     # Clean selection
                #     if len(selected_zip_code) > 1 and "All" in selected_zip_code:
                #         if selected_zip_code[-1] == "All":
                #             selected_zip_code = ["All"]
                #         else:
                #             selected_zip_code = [z for z in selected_zip_code if z != "All"]
                #     elif not selected_zip_code:
                #         selected_zip_code = ["All"]

                #     if selected_zip_code != current_zip:
                #         user_filters["selected_zip_code"] = selected_zip_code
                #         cookie_controller.set(
                #             "auth_data",
                #             json.dumps(auth_cookie),                    # must be a string
                #             expires=datetime.utcnow() + timedelta(days=30),
                #             path="/",
                #             domain=".coresight.com",
                #         )
                #         st.rerun()

                #     # Ensure MSA variable exists when in Zip mode
                #     selected_msa_name = user_filters.get("selected_msa_name", ["All"])

                # For downstream (outside location section) always pull latest selections from cookie
                selected_msa_name = user_filters.get("selected_msa_name", ["All"])
                selected_zip_code = user_filters.get("selected_zip_code", ["All"])

                # Ensure downstream variables exist
                selected_zip_code = user_filters.get("selected_zip_code", ["All"])
                # ------------- ZIP CODE OPTIONS -------------
                # Restrict zip codes by selected_msa_name if MSA is selected, else by selected_state_name if State is selected, else all
                if "All" not in selected_msa_name and selected_msa_name:
                    zip_pool = filtered_location_data[filtered_location_data['MsaName'].isin(selected_msa_name)]
                elif "All" not in selected_state_name and selected_state_name:
                    zip_pool = filtered_location_data[filtered_location_data['State'].isin(selected_state_name)]
                else:
                    zip_pool = filtered_location_data
                zip_codes = sorted(zip_pool['PostalCode'].dropna().astype(str).str.strip().unique().tolist())
                zip_codes = ["All"] + [zc for zc in zip_codes if zc != "0"]

                current_zip = user_filters.get("selected_zip_code", ["All"])
                if "All" in current_zip:
                    current_zip = ["All"]
                else:
                    valid_zip = [z for z in current_zip if z in zip_codes]
                    current_zip = valid_zip if valid_zip else ["All"]

                selected_zip_code = st.multiselect(
                    "Select Zip Code",
                    zip_codes,
                    default=current_zip,
                    help="U.S. postal area where the store is located.",
                    key="multiselect_zip_code"
                )

                # Clean selection
                if len(selected_zip_code) > 1 and "All" in selected_zip_code:
                    if selected_zip_code[-1] == "All":
                        selected_zip_code = ["All"]
                    else:
                        selected_zip_code = [z for z in selected_zip_code if z != "All"]
                elif not selected_zip_code:
                    selected_zip_code = ["All"]

                if selected_zip_code != current_zip:
                    user_filters["selected_zip_code"] = selected_zip_code
                    save_auth_cookie()
                    st.rerun()
                # else:
                #     save_auth_cookie()

                # For downstream (outside location section) always pull latest selections from cookie
                selected_zip_code = user_filters.get("selected_zip_code", ["All"])

                # Apply ZIP filter in downstream logic
                if "All" in selected_zip_code or not selected_zip_code:
                    pass  # No additional filtering needed
                else:
                    filtered_location_data = filtered_location_data[
                        filtered_location_data['PostalCode'].astype(str).isin([str(z) for z in selected_zip_code])
                    ]

        # Apply filters to opened data

        filtered_opened = opened_data.copy()
        filtered_opened_previous = opened_data.copy()

        if parent_chain_name:
            filtered_opened = filtered_opened[filtered_opened['ParentName_Coresight'].isin(parent_chain_name)]

        if selected_chain_name:
            filtered_opened = filtered_opened[filtered_opened['ChainName_Coresight'].isin(selected_chain_name)]
        if "selected_date_range" in st.session_state and isinstance(st.session_state["selected_date_range"], tuple):
            start_date, end_date = st.session_state["selected_date_range"]
            
            start_timestamp = pd.Timestamp(start_date)
            end_timestamp = pd.Timestamp(end_date)
            filtered_opened = filtered_opened[
                (filtered_opened['Period'] >= start_timestamp) &
                (filtered_opened['Period'] <= end_timestamp)
            ]
        else:
            selected_date_range, _, _ = get_synchronized_date_range(
                opened_data,
                default_months_back=12,
                label="opening.py"
            )
            start_date, end_date = selected_date_range
            start_timestamp = pd.Timestamp(start_date)
            end_timestamp = pd.Timestamp(end_date)
            filtered_opened = filtered_opened[
                (filtered_opened['Period'] >= start_timestamp) &
                (filtered_opened['Period'] <= end_timestamp)
            ]
            st.session_state["selected_date_range"] = (start_date, end_date)

        if selected_sector_name != "All":
            if selected_sector_name == None:
                filtered_opened = filtered_opened[filtered_opened['Sector_Coresight'].isnull()]
            else:
                filtered_opened = filtered_opened[
                filtered_opened['Sector_Coresight'] == selected_sector_name
                ]

        if selected_state_name:
            filtered_opened = filtered_opened[filtered_opened['State'].isin(selected_state_name)]

        # Apply location filter based on radio selection
        if st.session_state["location_type_base_opening"] == "MSA":
            if selected_msa_name and "All" not in selected_msa_name:
                filtered_opened = filtered_opened[filtered_opened['MsaName'].isin(selected_msa_name)]
        if selected_zip_code and "All" not in selected_zip_code:
            filtered_opened = filtered_opened[filtered_opened['PostalCode'].astype(str).isin([str(z) for z in selected_zip_code])]

        #Previous Period calculations for each filters

        if parent_chain_name:
            filtered_opened_previous = filtered_opened_previous[filtered_opened_previous['ParentName_Coresight'].isin(parent_chain_name)]

        if selected_chain_name:
            filtered_opened_previous = filtered_opened_previous[filtered_opened_previous['ChainName_Coresight'].isin(selected_chain_name)]

        if "selected_date_range" in st.session_state and isinstance(st.session_state["selected_date_range"], tuple):
            start_date, end_date = st.session_state["selected_date_range"]
            
            start_timestamp = pd.Timestamp(start_date)
            end_timestamp = pd.Timestamp(end_date)
            
            filtered_opened_previous = filtered_opened_previous[
                (filtered_opened_previous['Period'] >= start_timestamp) &
                (filtered_opened_previous['Period'] <= end_timestamp)
            ]
        else:
            selected_date_range, _, _ = get_synchronized_date_range(
                filtered_opened_previous,
                default_months_back=12,
                label="opening.py"
            )
            start_date, end_date = selected_date_range
            
            start_timestamp = pd.Timestamp(start_date)
            end_timestamp = pd.Timestamp(end_date)
            
            filtered_opened_previous = filtered_opened_previous[
                (filtered_opened_previous['Period'] >= start_timestamp) &
                (filtered_opened_previous['Period'] <= end_timestamp)
            ]
            
            # Update session state
            st.session_state["selected_date_range"] = (start_date, end_date)

        if selected_sector_name != "All":
            if selected_sector_name == None:
                filtered_opened_previous = filtered_opened_previous[filtered_opened_previous['Sector_Coresight'].isnull()]
            else:
                filtered_opened_previous = filtered_opened_previous[
                filtered_opened_previous['Sector_Coresight'] == selected_sector_name
                ]

        if selected_state_name:
            filtered_opened_previous = filtered_opened_previous[filtered_opened_previous['State'].isin(selected_state_name)]

        if st.session_state["location_type_base_opening"] == "MSA":
            if selected_msa_name and "All" not in selected_msa_name:
                filtered_opened_previous = filtered_opened_previous[filtered_opened_previous['MsaName'].isin(selected_msa_name)]
        if selected_zip_code and "All" not in selected_zip_code:
            filtered_opened_previous = filtered_opened_previous[filtered_opened_previous['PostalCode'].astype(str).isin([str(z) for z in selected_zip_code])]


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

        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])


        filtered_opened_sum = opened_data.copy()
        start_date_plot = filtered_opened['Period'].min()
        end_date_plot = filtered_opened['Period'].max()
        filtered_opened_sum = filtered_opened_sum[
                (filtered_opened_sum['Period'] >= start_date_plot) &
                (filtered_opened_sum['Period'] <= end_date_plot) &
                (filtered_opened_sum['ChainName_Coresight'].notnull())
        ]
        # ✅ Apply Parent Chain Name filter
        if "All" not in parent_chain_name and parent_chain_name != []:
            filtered_opened_sum = filtered_opened_sum[
                filtered_opened_sum["ParentName_Coresight"].isin(parent_chain_name)
            ]

        # Apply Chain Name filter
        if "All" not in selected_chain_name and selected_chain_name != []:
            filtered_opened_sum = filtered_opened_sum[
                filtered_opened_sum["ChainName_Coresight"].isin(selected_chain_name)
            ]

        # Apply Sector filter
        if selected_sector_name != "All":
            if selected_sector_name is None:
                filtered_opened_sum = filtered_opened_sum[filtered_opened_sum["Sector_Coresight"].isnull()]
            else:
                filtered_opened_sum = filtered_opened_sum[filtered_opened_sum["Sector_Coresight"] == selected_sector_name]

        # Apply State filter
        if selected_state_name and "All" not in selected_state_name:
            filtered_opened_sum = filtered_opened_sum[filtered_opened_sum["State"].isin(selected_state_name)]

        # Apply Location filter (MSA or Zip)
        if st.session_state["location_type_base_opening"] == "MSA":
            if selected_msa_name and "All" not in selected_msa_name:
                filtered_opened_sum = filtered_opened_sum[filtered_opened_sum["MsaName"].isin(selected_msa_name)]
        if selected_zip_code and "All" not in selected_zip_code:
            filtered_opened_sum = filtered_opened_sum[filtered_opened_sum["PostalCode"].astype(str).isin([str(z) for z in selected_zip_code])]

        sum_opened_stores = filtered_opened_sum['ChainName_Coresight'].count()
        with col1:
            st.markdown(f"""
        <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{sum_opened_stores:,}</h1>
        <h6 style='text-align: left; margin-top: 0px;'>Total Opened Stores</h6>
        """, unsafe_allow_html=True)
        with col2:
            total_retailers = filtered_opened_sum['ChainName_Coresight'].nunique()
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{total_retailers}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>Total Affected Banners</h6>
            """, unsafe_allow_html=True)
        with col3:
            total_categories = filtered_opened_sum['Sector_Coresight'].nunique()
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{total_categories}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>Total Affected Sectors</h6>
            """, unsafe_allow_html=True)

        sum_opened_stores_previous = filtered_opened_previous['ChainName_Coresight'].count()
        if sum_opened_stores_previous > 0:  # Avoid division by zero
            percent_difference = ((sum_opened_stores - sum_opened_stores_previous) / sum_opened_stores_previous) * 100
            with col4:
                st.markdown(f"""
                <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{percent_difference:.1f}%</h1>
                <h6 style='text-align: left; margin-top: 0px;'>% Change Compared to Previous Period</h6>
                """, unsafe_allow_html=True)
        else:
            percent_difference = None  # Handle cases where previous period data is zero or unavailable
            with col4:
                st.markdown(f"""
                <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>N/A</h1>
                <h6 style='text-align: left; margin-top: 0px;'>% Change Compared to Previous Period</h6>
            """, unsafe_allow_html=True)

        total_population = filtered_opened_sum['Population'].sum()
        opened_stores_per_10k = (sum_opened_stores / total_population) * 10000 if total_population > 0 else None
        with col5:
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

        # Helper function to calculate the next highest rounded limit
        def calculate_yaxis_limit(value):
            try:
                if pd.isna(value):  # Check for NaN values
                    print("No data available to display the opened Stores Over Time. Please select broader date range.")
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
            filtered_opened_sum['PeriodMonth'] = filtered_opened_sum['Period'].dt.to_period('M').dt.to_timestamp()

            opened_stores_over_time = filtered_opened_sum.groupby('PeriodMonth').size().reset_index(name='OpenedCount')
            opened_stores_over_time.rename(columns={'PeriodMonth': 'MonthStart'}, inplace=True)

            # Create lists to store metadata for each data point
            num_sectors_list = []
            num_banners_list = []
            num_states_list = []
            num_msa_list = []

            # Calculate metadata for each month
            for month in opened_stores_over_time['MonthStart']:
                month_data = filtered_opened_sum[filtered_opened_sum['PeriodMonth'] == month]
                num_sectors_list.append(month_data['Sector_Coresight'].nunique())
                num_banners_list.append(month_data['ChainName_Coresight'].nunique())
                num_states_list.append(month_data['State'].nunique())
                num_msa_list.append(month_data['MsaName'].nunique())

            # Create the figure with visible counts
            fig_opened_line = go.Figure()

            # Add trace with both permanent labels and hover info
            fig_opened_line.add_trace(go.Scatter(
                x=opened_stores_over_time['MonthStart'],
                y=opened_stores_over_time['OpenedCount'],
                name='Opened Stores',
                mode='lines+markers+text',  # Added 'text' mode
                text=[f"{x:,}" for x in opened_stores_over_time['OpenedCount']],  # Permanent labels
                textposition='top center',  # Position above markers
                line=dict(color='#A3C0CE', width=2),
                marker=dict(size=7),
                hovertemplate=(
                    "%{x|%b %Y}<br>"
                    "<span style='color:#A3C0CE'>●</span> "
                    "<b>Store Opened Count:</b> <b>%{y:,}</b><br><br>"
                    # "<span style='color:#A3C0CE'>•</span> "
                    # "<span style='color:black'>Date Period: %{x|%b %Y}</span><br>"
                    "<span style='color:#A3C0CE'>•</span> "
                    "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                    "<span style='color:#A3C0CE'>•</span> "
                    "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                    "<span style='color:#A3C0CE'>•</span> "
                    "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                    "<span style='color:#A3C0CE'>•</span> "
                    "<span style='color:black'>Number of MSA: %{customdata[3]}</span><br>"
                    "<extra></extra>"
                ),
                customdata=list(zip(num_sectors_list, num_banners_list, num_states_list, num_msa_list)),
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor="#A3C0CE",
                    font=dict(size=13, color="black", family="Arial")
                )
            ))

            # Update layout with text styling
            fig_opened_line.update_layout(
                yaxis_title="Opened Stores",
                margin=dict(l=10, r=10, t=60, b=40),  # Increased bottom margin for text labels
                height=400,  # Slightly increased height
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
                uniformtext_minsize=8,  # Minimum text size
                uniformtext_mode='hide'  # Hide text if it doesn't fit
            )

            # Same config as before
            config = {
                'displayModeBar': True,
                'displaylogo': False,
                'toImageButtonOptions': {
                    'format': 'png',
                    'filename': 'opened_stores_over_time',
                    'height': 400,
                    'width': 700,
                    'scale': 1
                }
            }

            # Display the chart
            with col1:
                st.markdown("<h4 style='font-size: 20px;text-align: center;'>Opened Stores Over Time</h4>", unsafe_allow_html=True)
                st.plotly_chart(fig_opened_line, use_container_width=True, config=config)

        except Exception as e:
            st.write(f"Error creating chart: {e}")
        # --- Opened Chains Bar Chart ---
        try:
            opened_store_counts = filtered_opened_sum['ChainName_Coresight'].value_counts().head(15)
            y_max_opened = calculate_yaxis_limit(opened_store_counts.max())

            # Calculate dynamic date period
            start_date = filtered_opened_sum['Period'].min()
            end_date = filtered_opened_sum['Period'].max()
            start_date_str = datefmt(start_date, "%b %Y")
            end_date_str   = datefmt(end_date,   "%b %Y")
            date_period = f"{start_date_str} to {end_date_str}"

            # Calculate metadata for the entire dataset
            num_states = filtered_opened_sum['State'].nunique()
            num_msa = filtered_opened_sum['MsaName'].nunique()

            # Create figure with consistent styling
            fig_opened_bar = go.Figure()

            fig_opened_bar.add_trace(go.Bar(
                x=opened_store_counts.index,
                y=opened_store_counts.values,
                marker_color='#A3C0CE',
                text=[f"{x:,}" for x in opened_store_counts.values],
                textposition='outside',
                hovertemplate=(
                    "<b>%{x}</b><br>"  # Banner name
                    "<span style='color:#A3C0CE'>●</span> "
                    "<b>Opened Stores:</b> %{y:,}<br><br>"
                    "<span style='color:#A3C0CE'>•</span> "
                    f"<span style='color:black'>Date Period: {date_period}</span><br>"
                    "<span style='color:#A3C0CE'>•</span> "
                    f"<span style='color:black'>Number of States: {num_states:,}</span><br>"
                    "<span style='color:#A3C0CE'>•</span> "
                    f"<span style='color:black'>Number of MSA: {num_msa:,}</span><br>"
                    "<extra></extra>"
                ),
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor='#A3C0CE',
                    font=dict(size=13, color="black", family="Arial")
                )
            ))

            # Calculate dynamic height based on number of banners
            dynamic_height = 400 + (20 * (len(opened_store_counts) - 10)) if len(opened_store_counts) > 10 else 400

            fig_opened_bar.update_layout(
                xaxis_title="Banner Name",
                yaxis_title="Opened Stores",
                yaxis=dict(
                    range=[0, y_max_opened],
                    autorange=False,
                    showline=True,
                    zeroline=False
                ),
                xaxis=dict(
                    showline=True,
                    zeroline=False,
                    tickangle=45 if len(opened_store_counts) > 5 else 0,
                    tickfont=dict(size=11)  # Slightly smaller font for long names
                ),
                height=dynamic_height,
                margin=dict(l=20, r=20, t=60, b=40 + (10 * len(opened_store_counts))),  # Dynamic bottom margin
                uniformtext_minsize=8,
                uniformtext_mode='hide'
            )

            # Adjust for many banners
            if len(opened_store_counts) > 8:
                fig_opened_bar.update_layout(
                    xaxis=dict(tickangle=60),
                    margin=dict(b=60 + (10 * len(opened_store_counts)))  # Extra bottom margin
                )

            # Bar Chart in column
            with col2:
                num_banners = min(15, opened_store_counts.shape[0])
                st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {num_banners} Banners</h4>", unsafe_allow_html=True)
                st.plotly_chart(fig_opened_bar, use_container_width=True)

        except Exception as e:
            st.error(f"Error displaying banner data: {str(e)}")


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

        # Apply the mapping to filtered_closed and filtered_opened
        filtered_opened = map_state_names(filtered_opened_sum)
        #filtered_opened = map_state_names(filtered_opened)


        # --- Opened Stores by State Map ---
        col1, col2 = st.columns([7, 3])

        #st.subheader('Opened Stores by State')


        # Group closed data by state
        opened_by_state = filtered_opened.groupby('State').size().reset_index(name='Opened Stores')
        opened_by_state = opened_by_state.dropna()  # Drop any rows with unmapped states

        if opened_by_state.empty or opened_by_state['State'].isna().all():
                # If no data, display a message and do not plot the graph
                with col1:
                    #st.subheader('Opened Stores by State')
                    print("")
        else:
            import math

            def calculate_distance(lat1, lon1, lat2, lon2):
                """Calculate distance between two points on map"""
                return math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)

            def detect_overlapping_states_by_count(opened_by_state, state_centers, min_distance=1.5):
                """
                Detect states that are too close to each other and would cause overlapping text
                Returns a list of states to exclude from annotations
                (Modified version for absolute store counts instead of per capita)
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
                    if state in opened_by_state['State'].values:
                        states_to_exclude.add(state)
                
                # Check each cluster and keep only the state with highest value
                for cluster in northeast_clusters:
                    cluster_states = [state for state in cluster if state in opened_by_state['State'].values]
                    if len(cluster_states) > 1:
                        # Find the state with highest store count in this cluster
                        cluster_data = opened_by_state[opened_by_state['State'].isin(cluster_states)]
                        max_state = cluster_data.loc[cluster_data['Opened Stores'].idxmax(), 'State']
                        
                        # Exclude all others in the cluster
                        for state in cluster_states:
                            if state != max_state:
                                states_to_exclude.add(state)
                
                # Additional proximity check for any remaining states
                remaining_states = [state for state in opened_by_state['State'] if state not in states_to_exclude]
                
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
                                # Keep the state with higher store count
                                state1_value = opened_by_state[opened_by_state['State'] == state1]['Opened Stores'].iloc[0]
                                state2_value = opened_by_state[opened_by_state['State'] == state2]['Opened Stores'].iloc[0]
                                
                                if state1_value < state2_value:
                                    states_to_exclude.add(state1)
                                else:
                                    states_to_exclude.add(state2)
                
                return list(states_to_exclude)

            # Create reverse dictionary to convert abbreviations to full names
            # Create reverse dictionary to convert abbreviations to full names
            abbreviation_to_full = {v: k for k, v in state_abbreviations.items()}

            # Add full state name column to the dataframe
            # --- 1) Build a full 50-state dataframe (0 for states with no data) ---
            # --- Build a full 50-state frame (zeros for missing states) ---
            all_states = pd.DataFrame({"State": sorted(abbreviation_to_full.keys())})
            opened_by_state_full = (
                all_states
                .merge(opened_by_state[["State", "Opened Stores"]], on="State", how="left")
                .fillna({"Opened Stores": 0})
            )
            opened_by_state_full["State_Full"] = opened_by_state_full["State"].map(abbreviation_to_full)

            # --- Per-state metadata (0s for empty states) ---
            state_metadata = {}
            for s in opened_by_state_full["State"]:
                sd = filtered_opened_sum[filtered_opened_sum["State"] == s]
                state_metadata[s] = {
                    "num_sectors": sd["Sector_Coresight"].nunique() if "Sector_Coresight" in sd.columns else 0,
                    "num_banners": sd["ChainName_Coresight"].nunique() if "ChainName_Coresight" in sd.columns else 0,
                    "num_msa":     sd["MsaName"].nunique() if "MsaName" in sd.columns else 0,
                }

            # --- Date period (just like your reference) ---
            start_date = filtered_opened_sum["Period"].min()
            end_date   = filtered_opened_sum["Period"].max()
            start_date_str = datefmt(start_date, "%b %Y")
            end_date_str   = datefmt(end_date,   "%b %Y")
            date_period = f"{start_date_str} to {end_date_str}"

            # --- Choropleth over ALL states (zeros included) ---
            zmax = int(opened_by_state_full["Opened Stores"].max()) or 1
            fig_opened_map = px.choropleth(
                opened_by_state_full,
                locations="State",
                locationmode="USA-states",
                color="Opened Stores",
                range_color=(0, zmax),
                color_continuous_scale=["#ffffff", "#A3C0CE"],  # light -> blue
                scope="usa",
                hover_name="State_Full",
                hover_data={"State": False}  # keep hover clean; we'll control with hovertemplate
            )

            # Thin white borders so zero-value states are visible like your ref
            # fig_opened_map.update_traces(marker_line_color="#ffffff", marker_line_width=0.8)

            # Hover matches your style (with metadata + date period)
            fig_opened_map.update_traces(
                hovertemplate=(
                    "<b>%{hovertext}</b><br>"
                    "<span style='color:#A3C0CE'>●</span> <b>Opened Stores:</b> %{z:,}<br><br>"
                    "<span style='color:#A3C0CE'>•</span> <span style='color:black'>Date Period: " + date_period + "</span><br>"
                    "<span style='color:#A3C0CE'>•</span> <span style='color:black'>Number of Sectors: %{customdata[0]:,}</span><br>"
                    "<span style='color:#A3C0CE'>•</span> <span style='color:black'>Number of Banners: %{customdata[1]:,}</span><br>"
                    "<span style='color:#A3C0CE'>•</span> <span style='color:black'>Number of MSA: %{customdata[2]:,}</span><br>"
                    "<extra></extra>"
                ),
                customdata=[[
                    state_metadata[s]["num_sectors"],
                    state_metadata[s]["num_banners"],
                    state_metadata[s]["num_msa"],
                ] for s in opened_by_state_full["State"]],
                hoverlabel=dict(bgcolor="white", bordercolor="#A3C0CE", font=dict(size=13, color="black", family="Arial")),
            )

            # Colorbar title like the reference
            fig_opened_map.update_coloraxes(colorbar_title="Opened Stores")

            # Layout to match the reference’s look
            fig_opened_map.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                geo=dict(
                    projection_scale=1.2,
                    center=dict(lat=37.5, lon=-95),
                    showlakes=True, lakecolor="rgb(255,255,255)",
                    showframe=False, showcoastlines=False
                )
            )

            # --- Text labels ONLY for states with data (>0), avoiding overlaps ---
            ann_df = opened_by_state_full[opened_by_state_full["Opened Stores"] > 0]
            excluded_states = detect_overlapping_states_by_count(ann_df, state_centers)  # your helper
            ann_df = ann_df[~ann_df["State"].isin(excluded_states)]

            annotations_opened = go.Scattergeo(
                locationmode="USA-states",
                lon=[state_centers[s][0] for s in ann_df["State"] if s in state_centers],
                lat=[state_centers[s][1] for s in ann_df["State"] if s in state_centers],
                text=[f"{s}<br>{int(cnt):,}" for s, cnt in zip(ann_df["State"], ann_df["Opened Stores"]) if s in state_centers],
                mode="text",
                showlegend=False,
                textfont=dict(size=11, color="black"),
                hoverinfo="skip"
            )
            fig_opened_map.add_trace(annotations_opened)

            with col1:
                st.markdown("<h4 style='font-size: 20px;text-align: center;'>Opened Stores by State</h4>", unsafe_allow_html=True)
                st.plotly_chart(fig_opened_map, use_container_width=True)



        # --- Cities Bar Chart ---
        opened_store_counts = filtered_opened_sum['City'].value_counts().head(15)
        y_max_opened = calculate_yaxis_limit(opened_store_counts.max())

        # Calculate metadata for each city
        city_metadata = {}
        for city in opened_store_counts.index:
            city_data = filtered_opened_sum[filtered_opened_sum['City'] == city]
            city_metadata[city] = {
                'num_sectors': city_data['Sector_Coresight'].nunique(),
                'num_banners': city_data['ChainName_Coresight'].nunique(),
                'num_states': city_data['State'].nunique(),
                'num_msa': city_data['MsaName'].nunique()
            }

        # Calculate overall date range
        start_date = datefmt(filtered_opened_sum['Period'].min(), "%B %Y")
        end_date   = datefmt(filtered_opened_sum['Period'].max(), "%B %Y")


        # Create figure with consistent styling
        fig_opened_bar = go.Figure()

        fig_opened_bar.add_trace(go.Bar(
            x=opened_store_counts.index,
            y=opened_store_counts.values,
            marker_color='#A3C0CE',  # Using your specified color
            text=[f"{x:,}" for x in opened_store_counts.values],
            textposition='outside',
            hovertemplate=(
                "<b>%{x}</b><br>"  # City name
                "<span style='color:#A3C0CE'>●</span> "
                "<b>Opened Stores:</b> %{y:,}<br><br>"
                "<span style='color:#A3C0CE'>•</span> "
                "<span style='color:black'>Date Period: " + f"{start_date} to {end_date}</span><br>"
                "<span style='color:#A3C0CE'>•</span> "
                "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                "<span style='color:#A3C0CE'>•</span> "
                "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                "<span style='color:#A3C0CE'>•</span> "
                "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                "<span style='color:#A3C0CE'>•</span> "
                "<span style='color:black'>Number of MSA: %{customdata[3]}</span><br>"
                "<extra></extra>"
            ),
            customdata=[[
                city_metadata[city]['num_sectors'],
                city_metadata[city]['num_banners'],
                city_metadata[city]['num_states'],
                city_metadata[city]['num_msa']
            ] for city in opened_store_counts.index],
            hoverlabel=dict(
                bgcolor="white",
                bordercolor='#A3C0CE',
                font=dict(size=13, color="black", family="Arial")
            )
        ))

        fig_opened_bar.update_layout(
            xaxis_title="City",
            yaxis_title="Opened Stores",
            yaxis=dict(
                range=[0, y_max_opened],
                autorange=False,
                showline=True,
                zeroline=False
            ),
            xaxis=dict(
                showline=True,
                zeroline=False,
                tickangle=45 if len(opened_store_counts) > 5 else 0  # Auto-rotate if many cities
            ),
            height=450,  # Increased height to accommodate additional hover info
            margin=dict(l=20, r=20, t=60, b=40),  # Adjusted margins
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )

        # Additional label rotation for many cities
        if len(opened_store_counts) > 8:
            fig_opened_bar.update_layout(
                xaxis=dict(tickangle=60),
                margin=dict(b=60)  # Extra bottom margin
            )

        # Bar Chart in column
        with col2:
            st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {opened_store_counts.shape[0]} Cities</h4>", unsafe_allow_html=True)
            st.plotly_chart(fig_opened_bar, use_container_width=True)

        #Stores per capita by state
        col1, col2 = st.columns([7, 3])

        # Numerator: openings per state (from your filtered_opened_sum)
        openings_by_state = (
            filtered_opened_sum.dropna(subset=['State'])
                .assign(State=lambda df: df['State'].astype(str).str.strip())
                .groupby('State', as_index=False)
                .size()
                .rename(columns={'size': 'Openings'})
        )
        openings_by_state['Openings'] = openings_by_state['Openings'].astype(int)

        pop_df = fetch_data_population()
        # Clean + validate
        pop_df['usps_state_name'] = pop_df['usps_state_name'].astype(str).str.strip()
        pop_df['zip_code'] = pop_df['zip_code'].astype(str).str.strip()
        pop_df['estimate_total_population'] = pd.to_numeric(pop_df['estimate_total_population'], errors='coerce')

        state_pop_lookup = (
            pop_df.dropna(subset=['usps_state_name','zip_code','estimate_total_population'])
                  .query('estimate_total_population > 0 and zip_code not in ["0","00000",""]')
                  .drop_duplicates(subset=['usps_state_name','zip_code'])   # each ZIP counted once
                  .groupby('usps_state_name', as_index=False)['estimate_total_population']
                  .sum()
                  .rename(columns={'usps_state_name': 'State',
                                   'estimate_total_population': 'State_Population'})
        )

        # Merge + compute per million
        opened_by_state = openings_by_state.merge(state_pop_lookup, on='State', how='left')
        opened_by_state = opened_by_state[
            opened_by_state['State_Population'].notna() & (opened_by_state['State_Population'] > 0)
        ].copy()

        opened_by_state['Opened Stores per Capita'] = (
            opened_by_state['Openings'] / opened_by_state['State_Population'] * 1_000_000
        ).astype(float)

        # (If Plotly needs float)
        opened_by_state['Opened Stores per Capita'] = opened_by_state['Opened Stores per Capita'].astype(float)

        if opened_by_state.empty or opened_by_state['State'].isna().all():
            with col1:
                print("No data available for Opened stores per capita")
        else:
            import math

            def calculate_distance(lat1, lon1, lat2, lon2):
                """Calculate distance between two points on map"""
                return math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)

            def detect_overlapping_states(opened_by_state, state_centers, min_distance=1.5):
                """
                Detect states that are too close to each other and would cause overlapping text
                Returns a list of states to exclude from annotations
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
                    if state in opened_by_state['State'].values:
                        states_to_exclude.add(state)
                
                # Check each cluster and keep only the state with highest value
                for cluster in northeast_clusters:
                    cluster_states = [state for state in cluster if state in opened_by_state['State'].values]
                    if len(cluster_states) > 1:
                        # Find the state with highest per capita value in this cluster
                        cluster_data = opened_by_state[opened_by_state['State'].isin(cluster_states)]
                        max_state = cluster_data.loc[cluster_data['Opened Stores per Capita'].idxmax(), 'State']
                        
                        # Exclude all others in the cluster
                        for state in cluster_states:
                            if state != max_state:
                                states_to_exclude.add(state)
                
                # Additional proximity check for any remaining states
                remaining_states = [state for state in opened_by_state['State'] if state not in states_to_exclude]
                
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
                                # Keep the state with higher per capita value
                                state1_value = opened_by_state[opened_by_state['State'] == state1]['Opened Stores per Capita'].iloc[0]
                                state2_value = opened_by_state[opened_by_state['State'] == state2]['Opened Stores per Capita'].iloc[0]
                                
                                if state1_value < state2_value:
                                    states_to_exclude.add(state1)
                                else:
                                    states_to_exclude.add(state2)
                
                return list(states_to_exclude)

            # Create the choropleth map
            fig_opened_map = px.choropleth(
                opened_by_state,
                locations="State",
                locationmode="USA-states",
                color="Opened Stores per Capita",
                color_continuous_scale=["#ffffff", "#A3C0CE"],
                scope="usa",
                hover_name="State",
                hover_data={"State": False}  # Only show what we specify in hovertemplate
            )

            # Calculate metadata for each state
            state_metadata = {}
            for state in opened_by_state['State']:
                state_data = filtered_opened_sum[filtered_opened_sum['State'] == state]
                state_metadata[state] = {
                    'num_sectors': state_data['Sector_Coresight'].nunique() if 'Sector_Coresight' in state_data.columns else 0,
                    'num_banners': state_data['ChainName_Coresight'].nunique() if 'ChainName_Coresight' in state_data.columns else 0,
                    'num_msa': state_data['MsaName'].nunique() if 'MsaName' in state_data.columns else 0
                }

            # Calculate overall date range
            start_date = filtered_opened_sum['Period'].min()
            end_date = filtered_opened_sum['Period'].max()
            start_date_str = datefmt(start_date, "%b %Y")
            end_date_str   = datefmt(end_date,   "%b %Y")

            date_period = f"{start_date_str} to {end_date_str}"

            # Customize hover template with state-specific metadata
            fig_opened_map.update_traces(
                hovertemplate=(
                    "<b>%{hovertext}</b><br>"  # State name
                    "<span style='color:#A3C0CE'>●</span> "
                    "<b>Stores per Million:</b> %{z:,.2f}<br><br>"  # Per capita value
                    "<span style='color:#A3C0CE'>•</span> "
                    f"<span style='color:black'>Date Period: {date_period}</span><br>"
                    "<span style='color:#A3C0CE'>•</span> "
                    "<span style='color:black'>Number of Sectors: %{customdata[0]:,}</span><br>"
                    "<span style='color:#A3C0CE'>•</span> "
                    "<span style='color:black'>Number of Banners: %{customdata[1]:,}</span><br>"
                    "<span style='color:#A3C0CE'>•</span> "
                    "<span style='color:black'>Number of MSA: %{customdata[2]:,}</span><br>"
                    "<extra></extra>"
                ),
                customdata=[[
                    state_metadata[state]['num_sectors'],
                    state_metadata[state]['num_banners'],
                    state_metadata[state]['num_msa']
                ] for state in opened_by_state['State']],
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor="#A3C0CE",
                    font=dict(size=13, color="black", family="Arial")
                )
            )

            fig_opened_map.update_coloraxes(
                colorbar_title="Opened Stores per Million Capita"
            )

            # Customize layout
            fig_opened_map.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                geo=dict(
                    projection_scale=1.2,
                    center=dict(lat=37.5, lon=-95),
                    showlakes=True,
                    lakecolor='rgb(255, 255, 255)',
                    showframe=False,
                    showcoastlines=False
                )
            )

            # State annotations (with hover disabled)
            excluded_states = detect_overlapping_states(opened_by_state, state_centers)
            states_to_annotate = opened_by_state[~opened_by_state['State'].isin(excluded_states)]

            # Add Scattergeo layer for non-overlapping state annotations only
            annotations_opened = go.Scattergeo(
                locationmode='USA-states',
                lon=[state_centers[state][0] for state in states_to_annotate['State'] if state in state_centers],
                lat=[state_centers[state][1] for state in states_to_annotate['State'] if state in state_centers],
                text=[f"{state}<br>{opened_per_capita:,.2f}"
                    for state, opened_per_capita in zip(states_to_annotate['State'], 
                                                        states_to_annotate['Opened Stores per Capita'])
                    if state in state_centers],
                mode='text',
                showlegend=False,
                textfont=dict(size=11, color="black"),
                hoverinfo='skip'  # Disables hover for annotations
            )
            fig_opened_map.add_trace(annotations_opened)

            # Display the map in Streamlit with full container width
            with col1:
                st.markdown("<h4 style='font-size: 20px;text-align: center;'>Opened Stores per Capita by State (per Million)</h4>", unsafe_allow_html=True)
                st.plotly_chart(fig_opened_map, use_container_width=True)

            opened_store_counts = filtered_opened_sum['Sector_Coresight'].value_counts().head(15).dropna()
            if opened_store_counts.empty:
                with col2:
                    st.markdown(
                        "<h4 style='font-size: 20px;text-align: center; top-align: center; color: red;'>By Sector data not available</h4>",
                        unsafe_allow_html=True
                    )
            else:
                y_max_opened = calculate_yaxis_limit(opened_store_counts.max())

                # Calculate dynamic date period
                start_date = filtered_opened_sum['Period'].min()
                end_date = filtered_opened_sum['Period'].max()
                start_date_str = datefmt(start_date, "%b %Y")
                end_date_str   = datefmt(end_date,   "%b %Y")

                date_period = f"{start_date_str} to {end_date_str}"

                # Calculate metadata for the entire dataset
                num_banners = filtered_opened_sum['ChainName_Coresight'].nunique()
                num_states = filtered_opened_sum['State'].nunique()
                num_msa = filtered_opened_sum['MsaName'].nunique()

                # Create figure with consistent styling
                fig_opened_bar = go.Figure()

                fig_opened_bar.add_trace(go.Bar(
                    x=opened_store_counts.index,
                    y=opened_store_counts.values,
                    marker_color='#A3C0CE',  # Using your specified color
                    text=[f"{x:,}" for x in opened_store_counts.values],
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"  # Sector name
                        "<span style='color:#A3C0CE'>●</span> "
                        "<b>Opened Stores:</b> %{y:,}<br><br>"
                        "<span style='color:#A3C0CE'>•</span> "
                        f"<span style='color:black'>Date Period: {date_period}</span><br>"
                        "<span style='color:#A3C0CE'>•</span> "
                        f"<span style='color:black'>Number of Banners: {num_banners:,}</span><br>"
                        "<span style='color:#A3C0CE'>•</span> "
                        f"<span style='color:black'>Number of States: {num_states:,}</span><br>"
                        "<span style='color:#A3C0CE'>•</span> "
                        f"<span style='color:black'>Number of MSA: {num_msa:,}</span><br>"
                        "<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor='#A3C0CE',
                        font=dict(size=13, color="black", family="Arial")
                    )
                ))

                fig_opened_bar.update_layout(
                    xaxis_title="Sector",
                    yaxis_title="Opened Stores",
                    yaxis=dict(
                        range=[0, y_max_opened],
                        autorange=False,
                        showline=True,
                        zeroline=False
                    ),
                    xaxis=dict(
                        showline=True,
                        zeroline=False,
                        tickangle=45 if len(opened_store_counts) > 5 else 0  # Auto-rotate if many sectors
                    ),
                    height=400,
                    margin=dict(l=20, r=20, t=60, b=40),  # Increased bottom margin for rotated labels
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )

                # Additional label rotation for many sectors
                if len(opened_store_counts) > 8:
                    fig_opened_bar.update_layout(
                        xaxis=dict(tickangle=60),
                        margin=dict(b=60)  # Extra bottom margin
                    )

                # Bar Chart in 30% width column
                with col2:
                    st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {opened_store_counts.shape[0]} Sectors</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_opened_bar, use_container_width=True)


        # --- Opened, Opened, and Active Stores Tables ---
        filtered_opened['Year'] = filtered_opened['Period'].dt.year
        filtered_opened['Month'] = filtered_opened['Period'].dt.month
        filtered_opened['Opening Month/Year'] =  filtered_opened['Month'].astype(str) + '-' + filtered_opened['Year'].astype(str)
        filtered_opened = filtered_opened[['ChainName_Coresight','ParentName_Coresight','Address','Address2','City','MsaName','PostalCode','State','Country','Sector_Coresight','Opening Month/Year']]

        filtered_opened = filtered_opened.rename(columns={
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
            'Opening Month/Year': 'Opened Date'
        })
        filtered_opened = filtered_opened.reset_index(drop=True)
                # --- OPENED STORES SECTION ---
        col1, col4  = st.columns([7, 1.05])
        with col1:
            st.subheader('Opened Stores Table')

        with col4:
            if not filtered_opened.empty:
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                    filtered_opened.to_excel(writer, index=False, sheet_name='Opened_Stores')
                    worksheet = writer.sheets['Opened_Stores']
                    for i, col in enumerate(filtered_opened.columns):
                        column_len = max(
                            filtered_opened[col].astype(str).map(len).max(),
                            len(col)
                        )
                        worksheet.set_column(i, i, column_len + 2)
                formatted_start_date = datefmt(start_date_plot, "%Y-%m-%d", default="")  # or "N/A"
                formatted_end_date   = datefmt(end_date_plot,   "%Y-%m-%d", default="")
                final_filename = f"Opened_Stores_{formatted_start_date}_to_{formatted_end_date}.xlsx"
                
                st.markdown("""
                    <style>
                        .stDownloadButton>button {
                            background-color: #A3C0CE;
                            color: white;
                            border: none;
                        }
                        .stDownloadButton>button:hover {
                            border: 2px solid #A3C0CE;
                            background-color: white;
                            color: #A3C0CE;
                        }
                    </style>
                """, unsafe_allow_html=True)

                if not filtered_opened.empty:
                    if not is_free_trial:
                        # Premium/employee/etc. => build the file and show real download
                        excel_buffer = io.BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                            filtered_opened.to_excel(writer, index=False, sheet_name='Opened_Stores')
                            worksheet = writer.sheets['Opened_Stores']
                            for i, col in enumerate(filtered_opened.columns):
                                column_len = max(filtered_opened[col].astype(str).map(len).max(), len(col))
                                worksheet.set_column(i, i, column_len + 2)
                        excel_buffer.seek(0)                           
                        formatted_start_date = datefmt(start_date, "%Y-%m-%d", default="")
                        formatted_end_date   = datefmt(end_date,   "%Y-%m-%d", default="")

                        final_filename = f"Opened_Stores_{formatted_start_date}_to_{formatted_end_date}.xlsx"

                        st.download_button(
                            label="Download Data",
                            data=excel_buffer,
                            file_name=final_filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_opened_xlsx",
                            use_container_width=True
                        )
                    else:

                        if st.button("Download Data", key="download_opened_blocked", use_container_width=True):
                            _trial_modal_opened()

                st.markdown('</div>', unsafe_allow_html=True)


        
        # AgGrid(filtered_opened, height=600) # You can adjust the height as needed

        GRID_H = 600  # must match your AgGrid height

        # Always show real data in the grid
        df_for_grid = filtered_opened

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
            key="opened_grid",
        )

        # Overlay (only for trial users)
        if is_free_trial:
            st.markdown(f"""
            <style>
              /* Overlay panel covers the grid completely */
              .grid-overlay-flag[data-target="opened"] {{
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

            <div class="grid-overlay-flag" data-target="opened">
              <img src="data:image/png;base64,{lock_b64}" class="lock" alt="Locked"/>
              <div class="title">For SIP Members Only</div>
              <p style="margin:8px 0 18px 0;">
                Individual Store data is exclusively accessible to SIP members and is not available for trial users.
              </p>
              <a class="cta" href="https://coresight.com/contact/" target="_blank">Contact Us</a>
            </div>
            """, unsafe_allow_html=True)


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

    elif selected_tab == "Compare Retailers":
        def update_options(selected_options, session_key):
            if "All" in selected_options and len(selected_options) > 1:
                if "All" not in st.session_state[session_key]:
                    return ["All"]
                return [opt for opt in selected_options if opt != "All"]
            return selected_options

        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            with st.expander("Date Range", expanded=st.session_state["retailer_comparison_expand_filters"]):
                opened_data['Period'] = pd.to_datetime(opened_data['Period'])
                min_date = opened_data['Period'].min().date()
                max_date = opened_data['Period'].max().date()
                default_start_date = max(min_date, (opened_data['Period'].max() - pd.DateOffset(months=11)).date())

                st.session_state.setdefault("compare_start_month", default_start_date.month)
                st.session_state.setdefault("compare_start_year", default_start_date.year)
                st.session_state.setdefault("compare_end_month", max_date.month)
                st.session_state.setdefault("compare_end_year", max_date.year)

                min_year, max_year = min_date.year, max_date.year
                all_months = list(range(1, 13))
                all_years = list(range(min_year, max_year + 1))

                sm_col, sy_col = st.columns([1.3, 1])
                with sm_col:
                    if st.session_state["compare_start_year"] == min_year:
                        valid_start_months = list(range(min_date.month, 13))
                    elif st.session_state["compare_start_year"] == max_year:
                        valid_start_months = list(range(1, max_date.month + 1))
                    else:
                        valid_start_months = all_months

                    st.selectbox(
                        "Start Month",
                        options=valid_start_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_start_months.index(st.session_state["compare_start_month"]),
                        key="compare_start_month_select"
                    )

                with sy_col:
                    st.selectbox(
                        "Start Year",
                        options=all_years,
                        index=all_years.index(st.session_state["compare_start_year"]),
                        key="compare_start_year_select"
                    )

                st.session_state["compare_start_month"] = st.session_state["compare_start_month_select"]
                st.session_state["compare_start_year"] = st.session_state["compare_start_year_select"]

                em_col, ey_col = st.columns([1.3, 1])
                valid_end_years = [y for y in all_years if st.session_state["compare_start_year"] <= y <= max_year]

                if st.session_state["compare_end_year"] not in valid_end_years:
                    st.session_state["compare_end_year"] = valid_end_years[0]

                if st.session_state["compare_end_year"] == st.session_state["compare_start_year"]:
                    valid_end_months = list(range(st.session_state["compare_start_month"], 13))
                elif st.session_state["compare_end_year"] == max_year:
                    valid_end_months = list(range(1, max_date.month + 1))
                else:
                    valid_end_months = all_months

                if st.session_state["compare_end_month"] not in valid_end_months:
                    st.session_state["compare_end_month"] = valid_end_months[0]

                with em_col:
                    st.selectbox(
                        "End Month",
                        options=valid_end_months,
                        format_func=lambda x: calendar.month_name[x],
                        index=valid_end_months.index(st.session_state["compare_end_month"]),
                        key="compare_end_month_select"
                    )

                with ey_col:
                    st.selectbox(
                        "End Year",
                        options=valid_end_years,
                        index=valid_end_years.index(st.session_state["compare_end_year"]),
                        key="compare_end_year_select"
                    )

                st.session_state["compare_end_month"] = st.session_state["compare_end_month_select"]
                st.session_state["compare_end_year"] = st.session_state["compare_end_year_select"]

                compare_start_date = max(date(st.session_state["compare_start_year"], st.session_state["compare_start_month"], 1), min_date)
                compare_end_day = calendar.monthrange(st.session_state["compare_end_year"], st.session_state["compare_end_month"])[1]
                compare_end_date = min(date(st.session_state["compare_end_year"], st.session_state["compare_end_month"], compare_end_day), max_date)

                selected_date_range = (compare_start_date, compare_end_date)
                start_date, end_date = pd.Timestamp(compare_start_date), pd.Timestamp(compare_end_date)

        data_dr = opened_data[(opened_data['Period'] >= start_date) & (opened_data['Period'] <= end_date)]
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
                
                # Add "All" at the beginning
                sector_names = ["All"] + sector_names
                
                selected_sector_name = st.selectbox("Select Sector", sector_names, key="sector_filter")

            if selected_sector_name and selected_sector_name != "All":
                data_sector = data_dr[data_dr['Sector_Coresight'] == selected_sector_name]
            else:
                data_sector = data_dr.copy()

        # --- Retailer Multi: start EMPTY; require minimum 2 ---
        with col3:
            with st.expander("Retailers", expanded=st.session_state["retailer_comparison_expand_filters"]):
                if not data_sector.empty:
                    data_sector['ParentName_Coresight'] = data_sector['ParentName_Coresight'].where(
                        pd.notna(data_sector['ParentName_Coresight']), "No Parent Retailer"
                    )
                    parent_names = sorted(data_sector['ParentName_Coresight'].unique().tolist())
                else:
                    parent_names = []

                selected_parent_names = st.multiselect(
                    "Select Company",
                    parent_names,
                    default=[],
                    key="selected_parent_names_v2",
                    help="Parent company that owns one or more store banners."
                )

                
                if selected_parent_names != st.session_state.get("previous_selected_parent_names_v2", []):
                    st.session_state["previous_selected_parent_names_v2"] = selected_parent_names
                    # Reset state and MSA if parent selection changes
                    st.session_state["selected_state_v2"] = ["All"]
                    st.session_state["selected_msa_v2"] = ["All"]
                    st.session_state["selected_zip_v2"] = ["All"]

                # Case 1: Parents selected → banners for those parents only
                if len(selected_parent_names) >= 1:
                    data_parent = data_sector[data_sector['ParentName_Coresight'].isin(selected_parent_names)]
                else:
                    # Case 2: NO parents chosen → banners for ALL data in this sector
                    data_parent = data_sector.copy()
                st.markdown(f"""<h6 style='text-align: center; margin-top: -10px; margin-bottom: -100px;'>or</h6>""", unsafe_allow_html=True)
                # Always populate banners from the (possibly whole) sector...
                if not data_parent.empty:
                    chain_names = sorted(data_parent['ChainName_Coresight'].dropna().unique().tolist())
                else:
                    chain_names = []
                banner_label = "Select Banner/Brand" if chain_names else "No Banner/Brand Available"
                selected_chain_names = st.multiselect(
                    banner_label,
                    chain_names,
                    default=[],
                    key="selected_chain_names_v2",
                    help="The specific retail banner or storefront name customers see."
                )

                if selected_chain_names != st.session_state.get("previous_selected_chain_names_v2", []):
                    st.session_state["previous_selected_chain_names_v2"] = selected_chain_names
                    # Reset state and MSA if parent selection changes
                    st.session_state["selected_state_v2"] = ["All"]
                    st.session_state["selected_msa_v2"] = ["All"]
                    st.session_state["selected_zip_v2"] = ["All"]
                   

            

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
            with st.expander("Location", expanded=st.session_state["retailer_comparison_expand_filters"]):

                # ------- STATE FILTER -------
                valid_states = sorted(data_chain['State'].dropna().unique().tolist()) if not data_chain.empty else []
                all_option = "All"
                state_options = [all_option] + valid_states

                has_parent_or_chain = bool(len(st.session_state.get("selected_parent_names_v2", []))>=2) or bool(len(st.session_state.get("selected_chain_names_v2", []))>=2)

                # Determine default value (don't write to session state)
                # if not has_parent_or_chain:
                #     default_states = ["All"]
                # else:
                #     current_value = st.session_state.get("selected_state_v2", ["All"])
                #     default_states = mutually_exclusive_all(current_value, all_option, valid_states)

                # # # Also reset MSA when parent/chain is cleared
                # # if not has_parent_or_chain and "selected_msa_v2" and "selected_zip_v2" in st.session_state:
                # #     st.session_state["selected_msa_v2"] = ["All"]
                # #     st.session_state["selected_zip_v2"] = ["All"]


                # user_selected_states = st.multiselect(
                #     "Select State",
                #     options=state_options,
                #     default=default_states,
                #     help="U.S. state where the store is located.",
                #     key="selected_state_v2",
                #     disabled=not has_parent_or_chain
                # )
                # if user_selected_states != st.session_state.get("previous_selected_state_v2", []):
                #     st.session_state["selected_msa_v2"] = ["All"]   # reset to All
                #     st.session_state["selected_zip_v2"] = ["All"]   # reset ZIP on state change
                #     # st.rerun()
                # st.session_state["previous_selected_state_v2"] = user_selected_states

                # # previous_state = st.session_state.get("previous_selected_state", None)
                # # if previous_state != user_selected_states:
                # #     st.session_state["selected_msa_v2"] = ["All"]   # reset to All
                # #     st.session_state["selected_zip_v2"] = ["All"]   # reset ZIP on state change
                # #     # st.rerun()
                # # st.session_state["previous_selected_state"] = user_selected_states

                # state_selection = mutually_exclusive_all(user_selected_states, all_option, valid_states)
                if not has_parent_or_chain:
                    default_states = ["All"]
                else:
                    current_value = st.session_state.get("selected_state_v2", ["All"])
                    default_states = mutually_exclusive_all(current_value, all_option, valid_states)

                # Initialize session state with default value if it doesn't exist
                if "selected_state_v2" not in st.session_state:
                    st.session_state["selected_state_v2"] = default_states

                # Create the widget with a different key to avoid conflicts
                user_selected_states = st.multiselect(
                    "Select State",
                    options=state_options,
                    # Use the session state value as default
                    default=st.session_state["selected_state_v2"],
                    help="U.S. state where the store is located.",
                    key="selected_state_v2_widget",  # Different key name to avoid conflict
                    disabled=not has_parent_or_chain
                )

                # Apply mutually exclusive logic manually after widget interaction
                if len(user_selected_states) > 1 and "All" in user_selected_states:
                    if user_selected_states[-1] == "All":
                        # User selected "All" after other options, so keep only "All"
                        st.session_state["selected_state_v2"] = ["All"]
                    else:
                        # User selected other options when "All" was selected, so remove "All"
                        st.session_state["selected_state_v2"] = [s for s in user_selected_states if s != "All"]
                    st.session_state["selected_msa_v2"] = ["All"]   # reset to All
                    st.session_state["selected_zip_v2"] = ["All"]   # reset ZIP on state change
                    st.rerun()
                elif not user_selected_states:
                    st.session_state["selected_state_v2"] = ["All"]
                    st.session_state["selected_msa_v2"] = ["All"]   # reset to All
                    st.session_state["selected_zip_v2"] = ["All"]   # reset ZIP on state change
                    st.rerun()

                if user_selected_states != st.session_state.get("previous_selected_state_v2", []):
                    st.session_state["previous_selected_state_v2"] = user_selected_states
                    st.session_state["selected_msa_v2"] = ["All"]   # reset to All
                    st.session_state["selected_zip_v2"] = ["All"]   # reset ZIP on state change
                    st.rerun()

                state_selection = mutually_exclusive_all(user_selected_states, all_option, valid_states)
                st.markdown("""
                <style>
                .st-emotion-cache-wfksaw {
                    gap: 0.2rem !important; /* Reduce the gap to a smaller value */
                }
                </style>
                """, unsafe_allow_html=True)
                _, center_col, _ = st.columns([20, 80, 10])

                # -------- Persist location type for Compare Retailers --------
                # if "location_type_retailers" not in user_filters:
                #     user_filters["location_type_retailers"] = st.session_state.get("location_type_retailers_opening", "MSA")
                # if "location_type_retailers_opening" not in st.session_state:
                #     st.session_state["location_type_retailers_opening"] = st.session_state.get("location_type_retailers", "MSA")

                # with center_col:
                    # location_type_retailers = st.radio(
                    #     "Filter by location",
                    #     "MSA",
                    #     horizontal=True,
                    #     key="location_type_retailers_opening",
                    #     label_visibility="collapsed"
                        
                    # )
                    # location_type_retailers = "MSA"
                    # if location_type_retailers != user_filters.get("location_type_retailers", "MSA"):
                    #     user_filters["location_type_retailers"] = location_type_retailers
                    #     cookie_controller.set(
                    #         "auth_data",
                    #         json.dumps(auth_cookie),                    # must be a string
                    #         expires=datetime.utcnow() + timedelta(days=30),
                    #         path="/",
                    #         domain=domain,
                    #     )
                    #     st.rerun()
                # ------- LOCATION FILTER (MSA or ZIP) -------
                # Filter dataset by state first
                if "All" in state_selection:
                    location_pool = data_chain
                else:
                    location_pool = data_chain[data_chain["State"].isin(state_selection)]

                valid_locations = sorted(location_pool["MsaName"].dropna().unique().tolist()) if not location_pool.empty else []
                options_list = ["All"] + valid_locations

                has_parent_or_chain = bool(len(st.session_state.get("selected_parent_names_v2", []))>=2) or bool(len(st.session_state.get("selected_chain_names_v2", []))>=2)
                # if not has_parent_or_chain:
                #     st.session_state["selected_msa_v2"] = ["All"]
                #     st.session_state["selected_zip_v2"] = ["All"]

                # Get current MSA selection and clean it
                current_msa = st.session_state.get("selected_msa_v2", ["All"])
                if "All" in current_msa:
                    current_msa = ["All"]
                else:
                    current_msa = [m for m in current_msa if m in valid_locations] or ["All"]

                # Update the session state with cleaned selection
                st.session_state["selected_msa_v2"] = current_msa

                # Render the MSA multiselect
                user_selected_loc = st.multiselect(
                    "Select Metropolitan Statistical Area (MSA)",
                    options=options_list,
                    default=current_msa,
                    help="A metro region centered on a large city plus its economically linked suburbs.",
                    key="msa_selector",  # Using a different key to avoid conflicts
                    disabled=not has_parent_or_chain
                )

                # Clean the selection
                if len(user_selected_loc) > 1 and "All" in user_selected_loc:
                    user_selected_loc = ["All"] if user_selected_loc[-1] == "All" else [s for s in user_selected_loc if s != "All"]
                elif not user_selected_loc:
                    user_selected_loc = ["All"]

                # Check if MSA selection changed
                if user_selected_loc != current_msa:
                    st.session_state["selected_msa_v2"] = user_selected_loc
                    st.session_state["selected_zip_v2"] = ["All"]  # Reset ZIP when MSA changes
                    st.rerun()

                # if "All" in state_selection:
                #     location_pool = data_chain
                # else:
                #     location_pool = data_chain[data_chain["State"].isin(state_selection)]

                # valid_locations = sorted(location_pool["MsaName"].dropna().unique().tolist()) if not location_pool.empty else []
                # options_list = ["All"] + valid_locations

                # has_parent_or_chain = bool(st.session_state.get("selected_parent_names_v2", [])) or bool(st.session_state.get("selected_chain_names_v2", []))
                # if not has_parent_or_chain:
                #     st.session_state["selected_msa_v2"] = ["All"]
                #     st.session_state["selected_zip_v2"] = ["All"]
                # # else:
                # #     current_value = st.session_state.get("selected_msa_v2", ["All"])
                #         # if len(valid_locations) == 1 and current_value == ["All"]:
                #         #     st.session_state["selected_msa_v2"] = [valid_locations[0]]
                #         # else:
                #         #     st.session_state["selected_msa_v2"] = mutually_exclusive_all(current_value, all_option, valid_locations)
                
                # location_label = "Select Metropolitan Statistical Area (MSA)"
                # current_msa = st.session_state.get("selected_msa_opening_v2", ["All"])
                # user_selected_loc = st.multiselect(
                #     location_label,
                #     options=options_list,
                #     # default=default_selection,
                #     help="A metro region centered on a large city plus its economically linked suburbs.",
                #     key="selected_msa_v2",
                #     disabled=not has_parent_or_chain
                # )
                # if user_selected_loc != current_msa:
                #     st.session_state["selected_msa_opening_v2"] = user_selected_loc
                #     st.session_state["selected_zip_v2"] = ["All"]  # Reset ZIP when MSA changes
                #     st.rerun()
                # # Clean up the selection
                # if len(user_selected_loc) > 1 and "All" in user_selected_loc:
                #     if user_selected_loc[-1] == "All":
                #         user_selected_loc = ["All"]
                #     else:
                #         user_selected_loc = [s for s in user_selected_loc if s != "All"]
                # elif not user_selected_loc:
                #     user_selected_loc = ["All"]
               

                    # if user_selected_loc != st.session_state.get("previous_selected_msa_v2", []):
                    #     st.session_state["previous_selected_msa_v2"] = user_selected_loc
                    #     # Reset state and MSA if parent selection changes
                    #     # st.session_state["selected_state_v2"] = ["All"]
                    #     st.session_state["selected_zip_v2"] = ["All"]
                    #     st.session_state["selected_msa_v2"] = ["All"]
                # location_selection = mutually_exclusive_all(user_selected_loc, "All", valid_locations)   
                # if len(user_selected_loc) > 1 and "All" in user_selected_loc:
                #         if user_selected_loc[-1] == "All":
                #             print("jj",user_selected_loc)
                #             user_selected_loc = ["All"]
                #         else:
                #             print("jj2",user_selected_loc)
                #             user_selected_loc = [s for s in user_selected_loc if s != "All"]
                # elif not user_selected_loc:
                #         print("jj3",user_selected_loc)
                #         user_selected_loc = ["All"] 
                # if user_selected_loc != st.session_state.get("previous_selected_msa_v2", []):
                #         print("hiii")
                #         st.session_state["previous_selected_msa_v2"] = user_selected_loc
                #         # Reset state and MSA if parent selection changes
                #         # st.session_state["selected_state_v2"] = ["All"]
                #         st.session_state["selected_zip_v2"] = ["All"]
                #         st.rerun()
                    
                
                filter_column = "MsaName"


                # ZIP Code multiselect (independent)
                # Filter location_pool by selected_msa_v2 if MSA is selected, else by selected_state_v2 if State is selected, else location_pool
                # selected_state_v2 = st.session_state.get("selected_state_v2", ["All"])
                # selected_msa_v2 = st.session_state.get("selected_msa_v2", ["All"])
                # if "All" not in selected_msa_v2:
                #     zip_pool = location_pool[location_pool['MsaName'].isin(selected_msa_v2)]
                # elif "All" not in selected_state_v2:
                #     zip_pool = location_pool[location_pool['State'].isin(selected_state_v2)]
                # else:
                #     zip_pool = location_pool
                # valid_locations = sorted(zip_pool["PostalCode"].dropna().astype(str).unique().tolist()) if not zip_pool.empty else []
                # options_list = ["All"] + valid_locations

                # ss_selected_loc = st.session_state.get("selected_zip_v2", ["All"])
                # if len(valid_locations) == 1 and ss_selected_loc == ["All"]:
                #     default_selection = ["All"]
                # else:
                #     default_selection = mutually_exclusive_all(ss_selected_loc, "All", valid_locations)
                # # Set the session state to this default value if it doesn't exist
                # if "selected_zip_v2" not in st.session_state:
                #     st.session_state["selected_zip_v2"] = default_selection
                # location_label = "Select Postal Code"
                # user_selected_loc = st.multiselect(
                #     location_label,
                #     options=options_list,
                #     default=default_selection,
                #     help="U.S. postal code where the store is located.",
                #     key="selected_zip_v2",
                #     disabled=not has_parent_or_chain
                # )

                # if len(user_selected_loc) > 1 and "All" in user_selected_loc:
                #     user_selected_loc = ["All"] if user_selected_loc[-1] == "All" else [z for z in user_selected_loc if z != "All"]
                # elif not user_selected_loc:
                #     user_selected_loc = ["All"]

                # # Update session state and rerun if selection changed
                # if user_selected_loc != st.session_state.get("previous_selected_zip_v2", []):
                #     st.session_state["previous_selected_zip_v2"] = user_selected_loc
                #     save_auth_cookie()
                #     st.rerun()
                
                # location_selection = mutually_exclusive_all(user_selected_loc, "All", valid_locations)
                # ZIP Code multiselect (independent)
# Filter location_pool by selected_msa_v2 if MSA is selected, else by selected_state_v2 if State is selected, else location_pool
                selected_state_v2 = st.session_state.get("selected_state_v2", ["All"])
                selected_msa_v2 = st.session_state.get("selected_msa_v2", ["All"])
                if "All" not in selected_msa_v2:
                    zip_pool = location_pool[location_pool['MsaName'].isin(selected_msa_v2)]
                elif "All" not in selected_state_v2:
                    zip_pool = location_pool[location_pool['State'].isin(selected_state_v2)]
                else:
                    zip_pool = location_pool
                valid_locations = sorted(zip_pool["PostalCode"].dropna().astype(str).unique().tolist()) if not zip_pool.empty else []
                options_list = ["All"] + valid_locations

                ss_selected_loc = st.session_state.get("selected_zip_v2", ["All"])
                if len(valid_locations) == 1 and ss_selected_loc == ["All"]:
                    default_selection = ["All"]
                else:
                    default_selection = mutually_exclusive_all(ss_selected_loc, "All", valid_locations)

                # Initialize session state with default value if it doesn't exist
                if "selected_zip_v2" not in st.session_state:
                    st.session_state["selected_zip_v2"] = default_selection

                location_label = "Select Zip Code"
                user_selected_loc = st.multiselect(
                    location_label,
                    options=options_list,
                    # Use the session state value as default
                    default=st.session_state["selected_zip_v2"],
                    help="U.S. postal area where the store is located.",
                    key="selected_zip_v2_widget",  # Different key name to avoid conflict
                    disabled=not has_parent_or_chain
                )

                # Apply mutually exclusive logic manually after widget interaction
                if len(user_selected_loc) > 1 and "All" in user_selected_loc:
                    if user_selected_loc[-1] == "All":
                        # User selected "All" after other options, so keep only "All"
                        st.session_state["selected_zip_v2"] = ["All"]
                    else:
                        # User selected other options when "All" was selected, so remove "All"
                        st.session_state["selected_zip_v2"] = [z for z in user_selected_loc if z != "All"]
                    st.rerun()
                elif not user_selected_loc:
                    st.session_state["selected_zip_v2"] = ["All"]
                    st.rerun()

                # Update session state and rerun if selection changed after applying logic
                if user_selected_loc != st.session_state.get("previous_selected_zip_v2", []):
                    st.session_state["previous_selected_zip_v2"] = user_selected_loc
                    save_auth_cookie()
                    st.rerun()

                location_selection = mutually_exclusive_all(user_selected_loc, "All", valid_locations)
                filter_column = "PostalCode"
                # filter_column = "PostalCode"

                # ------- FILTER FINAL DATA FOR ANALYSIS -------
                final_filtered_data = location_pool

                # Apply MSA filter if selected
                selected_msa_v2 = st.session_state.get("selected_msa_v2", ["All"])
                if "All" not in selected_msa_v2:
                    final_filtered_data = final_filtered_data[final_filtered_data['MsaName'].isin(selected_msa_v2)]

                # Apply ZIP filter if selected
                selected_zip_v2 = st.session_state.get("selected_zip_v2", ["All"])
                if "All" not in selected_zip_v2:
                    final_filtered_data = final_filtered_data[final_filtered_data['PostalCode'].astype(str).isin([str(z) for z in selected_zip_v2])]

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
                    <h6 style='text-align: left; margin-top: 0px;'>Total Opened Stores</h6>
                """, unsafe_allow_html=True)
            with mcol2:
                st.markdown(f"""
                    <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{final_filtered_data['ChainName_Coresight'].nunique()}</h1>
                    <h6 style='text-align: left; margin-top: 0px;'>Total Affected Banners</h6>
                """, unsafe_allow_html=True)
            with mcol3:
                st.markdown(f"""
                    <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{final_filtered_data['Sector_Coresight'].nunique()}</h1>
                    <h6 style='text-align: left; margin-top: 0px;'>Total Affected Sectors</h6>
                """, unsafe_allow_html=True)

            # --- LINE CHART ---
            gcol1, gcol2 = st.columns([4,2])
            with gcol1:
                st.markdown("<h4 style='font-size: 20px;text-align: center;'>Opened Stores Over Time<br></h4>", unsafe_allow_html=True)
                time_grouped = (
                    final_filtered_data.groupby(['Period', 'ParentName_Coresight', 'ChainName_Coresight'])
                    .size().reset_index(name='OpenedStores')
                )
                
                metadata_dict = {}
                for period in time_grouped['Period'].unique():
                    period_data = final_filtered_data[final_filtered_data['Period'] == period]
                    for (parent, chain), grp_df in period_data.groupby(["ParentName_Coresight", "ChainName_Coresight"]):
                        key = f"{period}|{parent}|{chain}"
                        metadata_dict[key] = {
                            'num_sectors': grp_df['Sector_Coresight'].nunique(),
                            'num_banners': grp_df['ChainName_Coresight'].nunique(),
                            'num_states': grp_df['State'].nunique(),
                            'num_msa': grp_df['MsaName'].nunique()
                        }
                
                fig_line = go.Figure()
                for (parent, chain), grp_df in time_grouped.groupby(["ParentName_Coresight", "ChainName_Coresight"]):
                    legend_grp = chain  # Just the ChainName for the legend
                    color = color_map.get(f"{parent} | {chain}", "#333")
                    
                    custom_data = []
                    for _, row in grp_df.iterrows():
                        key = f"{row['Period']}|{parent}|{chain}"
                        metadata = metadata_dict.get(key, {'num_sectors': 0, 'num_banners': 0, 'num_states': 0, 'num_msa': 0})
                        custom_data.append([
                            metadata['num_sectors'],
                            metadata['num_banners'], 
                            metadata['num_states'],
                            metadata['num_msa']
                        ])
                    
                    fig_line.add_trace(go.Scatter(
                        x=grp_df['Period'],
                        y=grp_df['OpenedStores'],
                        mode='lines+markers',
                        name=legend_grp,
                        line=dict(color=color, width=2),
                        marker=dict(size=7, color=color),
                        hovertemplate=(
                            "<b>%{x|%b %Y}</b><br>"  # Formatted date
                            "<span style='color:" + color + "'>●</span> "
                            "<b>" + legend_grp + ":</b> %{y:,}<br><br>"
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
                        customdata=custom_data,
                        hoverlabel=dict(
                            bgcolor="white",
                            bordercolor=color,
                            font=dict(size=13, color="black", family="Arial")
                        )
                    ))
                
                fig_line.update_layout(
                    yaxis_title="Opened Stores",
                    xaxis_title="Period",
                    height=400,  # Increased height to accommodate additional hover info
                    showlegend=True,
                    legend=dict(
                        x=0.99, y=0.99,
                        xanchor="right",
                        yanchor="top",
                        bgcolor='rgba(0,0,0,0)',  # fully transparent
                        bordercolor='rgba(0,0,0,0)',  # no border
                        font=dict(size=13),
                        orientation="v"
                    ),
                    margin=dict(l=10, r=10, t=60, b=10),
                    xaxis=dict(
                        tickformat="%b %Y",  # Clean date formatting
                        showline=True,
                        zeroline=False
                    ),
                    yaxis=dict(
                        showline=True,
                        zeroline=False
                    )
                )
                
                config = {
                    'displayModeBar': True,
                    'displaylogo': False,  # Only remove the Plotly logo
                    'toImageButtonOptions': {
                        'format': 'png',
                        'filename': 'opened_stores_by_chain_over_time',
                        'height': 400,
                        'width': 700,
                        'scale': 1
                    }
                }

                st.plotly_chart(fig_line, use_container_width=True, config=config)
            with gcol2:
                st.markdown("<h4 style='font-size: 20px; text-align: center;'>Opened Stores Counts</h4>", unsafe_allow_html=True)
                bar_grouped = (
                    final_filtered_data.groupby(['ParentName_Coresight', 'ChainName_Coresight'])
                    .size().reset_index(name='OpenedStores')
                )
                bar_grouped['GroupLabel'] = bar_grouped['ParentName_Coresight'] + " | " + bar_grouped["ChainName_Coresight"]

                # Create figure with consistent styling
                fig_bar = go.Figure()

                for _, row in bar_grouped.iterrows():
                    banner = row['ChainName_Coresight']
                    parent = row['ParentName_Coresight']
                    color = color_map.get(row['GroupLabel'], "#333")  # Fallback color
                    
                    fig_bar.add_trace(go.Bar(
                        x=[banner],
                        y=[row['OpenedStores']],
                        name=parent,
                        marker_color=color,
                        text=[f"{row['OpenedStores']:,}"],
                        textposition='outside',
                        hovertemplate=(
                            f"<b>{banner}</b><br>"
                            f"<span style='color:{color}'>●</span> "
                            f"<b>Opened Stores:</b> %{{y:,}}<extra></extra>"
                        ),
                        hoverlabel=dict(
                            bgcolor="white",
                            bordercolor=color,
                            font=dict(size=13, color="black", family="Arial")
                        )
                    ))

                fig_bar.update_layout(
                    xaxis_title="Banner",
                    yaxis_title="Opened Stores",
                    showlegend=False,
                    height=350 + (20 * len(bar_grouped)),  # Dynamic height based on number of bars
                    margin=dict(l=10, r=10, t=10, b=10 + (10 * len(bar_grouped))),  # Dynamic bottom margin
                    xaxis=dict(
                        showline=True,
                        zeroline=False,
                        tickangle=45,
                        tickfont=dict(size=11),  # Smaller font size
                        automargin=True,  # Let Plotly handle margin
                        type='category'  # Ensures categorical spacing
                    ),
                    yaxis=dict(
                        showline=True,
                        zeroline=False,
                        showticklabels=False
                    ),
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )

                # Additional solution for extreme cases
                if len(bar_grouped) > 10:
                    fig_bar.update_layout(
                        xaxis=dict(
                            tickangle=90,
                            tickfont=dict(size=10)
                        ),
                        height=400 + (15 * len(bar_grouped))
                    )

                st.plotly_chart(fig_bar, use_container_width=True)

            st.markdown("---")
            st.markdown("<h4 style='font-size: 20px; text-align: left;'>Opened Stores by Retailer</h4>", unsafe_allow_html=True)
            retailer_agg = final_filtered_data.groupby(['ParentName_Coresight']).size().reset_index(name='OpenedStores')

            # Generate shades of the approved color (#d62e2f)
            def generate_shades(base_hex, num_shades):
                base_rgb = [int(base_hex[i:i+2], 16)/255. for i in (1, 3, 5)]
                h, l, s = colorsys.rgb_to_hls(*base_rgb)
                # Shades: make some lighter, some darker
                l_values = [min(0.9, max(0.2, l * (0.80 + i*0.20/(max(num_shades-1,1))))) for i in range(num_shades)]
                return [f'#{int(c[0]*255):02x}{int(c[1]*255):02x}{int(c[2]*255):02x}' for c in [colorsys.hls_to_rgb(h, lval, s) for lval in l_values]]

            n = len(retailer_agg)
            my_palette = generate_shades("#A3C0CE", n)
            color_map_retailer = {name: my_palette[i] for i, name in enumerate(retailer_agg['ParentName_Coresight'])}

            # Create figure with consistent styling
            fig_retailer = go.Figure()

            for _, row in retailer_agg.iterrows():
                retailer = row['ParentName_Coresight']
                color = color_map_retailer[retailer]
                
                fig_retailer.add_trace(go.Bar(
                    x=[retailer],
                    y=[row['OpenedStores']],
                    name=retailer,
                    marker_color=color,
                    text=[f"{row['OpenedStores']:,}"],  # Formatted with thousands separator
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"  # Retailer name
                        "<span style='color:" + color + "'>●</span> "
                        "<b>Opened Stores:</b> %{y:,}<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor=color,
                        font=dict(size=13, color="black", family="Arial")
                    )
                ))

            fig_retailer.update_layout(
                xaxis_title="Retailer",
                yaxis_title="Opened Stores",
                showlegend=False,
                height=400,
                margin=dict(t=60, b=40, l=10, r=10),
                xaxis=dict(
                    showline=True,
                    zeroline=False
                ),
                yaxis=dict(
                    showline=True,
                    zeroline=False,
                    showticklabels=False  # Cleaner look without y-axis numbers
                ),
                uniformtext_minsize=8,  # Ensures text labels remain readable
                uniformtext_mode='hide'
            )

            st.plotly_chart(fig_retailer, use_container_width=True)

            # --- Table ---
            final_filtered_data['Opening Month/Year'] = datefmt_series(
                final_filtered_data['Period'], "%b-%Y"
            )
            filtered_opened_compare = final_filtered_data[['ChainName_Coresight', 'ParentName_Coresight', 'Address',
                                                        'Address2', 'City', 'MsaName', 'PostalCode', 'State', 'Country',
                                                        'Sector_Coresight', 'Opening Month/Year']]
            filtered_opened_compare = filtered_opened_compare.rename(columns={
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
                'Opening Month/Year': 'Opened Date'
            })
            col1, col4  = st.columns([7, 1.05])
            with col1:
                st.subheader('Opened Stores Table')

            with col4:
                if not filtered_opened_compare.empty:
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        filtered_opened_compare.to_excel(writer, index=False, sheet_name='Opened_Stores')
                        worksheet = writer.sheets['Opened_Stores']
                        for i, col in enumerate(filtered_opened_compare.columns):
                            column_len = max(
                                filtered_opened_compare[col].astype(str).map(len).max(),
                                len(col)
                            )
                            worksheet.set_column(i, i, column_len + 2)

                    formatted_start_date = datefmt(start_date, "%Y-%m-%d", default="")
                    formatted_end_date   = datefmt(end_date,   "%Y-%m-%d", default="")

                    final_filename = f"Opened_Stores_{formatted_start_date}_to_{formatted_end_date}.xlsx"

                    # Download button styles
                    st.markdown("""
                        <style>
                            .stDownloadButton>button {
                                background-color: #A3C0CE;
                                color: white;
                                border: none;
                            }
                            .stDownloadButton>button:hover {
                                border: 2px solid #A3C0CE;
                                background-color: white;
                                color: #A3C0CE;
                            }
                        </style>
                    """, unsafe_allow_html=True)

                    if not is_free_trial:
                        # Premium users → real download
                        excel_buffer.seek(0)
                        st.download_button(
                            label="Download Data",
                            data=excel_buffer,
                            file_name=final_filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_opened_compare_xlsx",
                            use_container_width=True
                        )
                    else:
                        # Trial users → fake download button
                        if st.button("Download Data", key="download_opened_compare_blocked", use_container_width=True):
                            _trial_modal_opened()

                st.markdown('</div>', unsafe_allow_html=True)

            GRID_H = 600
            df_for_grid = filtered_opened_compare

            # Lock icon
            ICON = pathlib.Path(__file__).resolve().parent.parent / "assets" / "icons" / "lock.png"
            lock_b64 = base64.b64encode(ICON.read_bytes()).decode("utf-8")

            gb = GridOptionsBuilder.from_dataframe(df_for_grid)
            grid_options = gb.build()

            AgGrid(
                df_for_grid,
                gridOptions=grid_options,
                height=GRID_H,
                key="opened_compare_grid",
            )

            if is_free_trial:
                st.markdown(f"""
                <style>
                  /* Overlay panel covers the grid completely */
                  .grid-overlay-flag[data-target="opened"] {{
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

                <div class="grid-overlay-flag" data-target="opened">
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
        # Initialize session state for sector comparison
        session_defaults = {
            "sector_comparison_selected_sectors": [],
            "sector_comparison_expand_filters": True,
            "sector_compare_parent_chain_name": [],
            "sector_compare_selected_chain_name": [],
            "sector_compare_selected_state_name": [],
            "sector_compare_selected_msa_name": [],
            "sector_compare_selected_zip_name": [],
        }

        for key, value in session_defaults.items():
            st.session_state.setdefault(key, value)

        # ---- ROBUST SYNC QUERY PARAMS ONCE, ON FIRST LOAD ----
        def ensure_list(val):
            # Handles str, list, or None for query params
            if val is None:
                return []
            if isinstance(val, list):
                return val
            else:
                return [val]

        params = st.query_params
        if "sector_compare_synced" not in st.session_state:
            st.session_state["sector_comparison_selected_sectors"] = ensure_list(params.get("compare_sectors"))
            st.session_state["sector_compare_parent_chain_name"] = ensure_list(params.get("parent_chain"))
            st.session_state["sector_compare_selected_chain_name"] = ensure_list(params.get("chain"))
            st.session_state["sector_compare_selected_state_name"] = ensure_list(params.get("state"))
            st.session_state["sector_compare_selected_msa_name"] = ensure_list(params.get("msa"))
            st.session_state["sector_compare_synced"] = True

        # Save selected filters back to query parameters
        def update_query_params():
            st.query_params.update(
                compare_sectors=st.session_state["sector_comparison_selected_sectors"],
                parent_chain=st.session_state["sector_compare_parent_chain_name"],
                chain=st.session_state["sector_compare_selected_chain_name"],
                state=st.session_state["sector_compare_selected_state_name"],
                msa=st.session_state["sector_compare_selected_msa_name"]
            )

        # Layout for the filters
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            with st.expander("Date Range", expanded=st.session_state["sector_comparison_expand_filters"]):
                opened_data['Period'] = pd.to_datetime(opened_data['Period'])
                min_date = opened_data['Period'].min().date()
                max_date = opened_data['Period'].max().date()
                default_start_date = max(min_date, (opened_data['Period'].max() - pd.DateOffset(months=11)).date())

                st.session_state.setdefault("sector_compare_start_month", default_start_date.month)
                st.session_state.setdefault("sector_compare_start_year", default_start_date.year)
                st.session_state.setdefault("sector_compare_end_month", max_date.month)
                st.session_state.setdefault("sector_compare_end_year", max_date.year)

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
                update_query_params()

        with col2:
            with st.expander("Sector Comparison", expanded=st.session_state["sector_comparison_expand_filters"]):
                logging.info("Initializing sector comparison filter")
                start_date, end_date = st.session_state["sector_compare_date_range"]
                logging.info(f"Filtering data for date range: {start_date} to {end_date}")
                
                filtered_sector_data = opened_data[
                    (opened_data['Period'] >= pd.Timestamp(start_date)) & 
                    (opened_data['Period'] <= pd.Timestamp(end_date))
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
                    print(f"Sector selection changed from {st.session_state['sector_comparison_selected_sectors']} to {selected_sectors}")
                    st.session_state["sector_comparison_selected_sectors"] = selected_sectors
                    st.session_state["sector_compare_selected_msa_name"] = ["All"]
                    st.session_state["sector_compare_selected_zip_name"] = ["All"]
                    st.session_state["sector_compare_selected_state_name"] = ["All"]

                    # update_query_params()
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
                    
                    temp_data = opened_data[
                        (opened_data['Period'] >= pd.Timestamp(start_date)) &
                        (opened_data['Period'] <= pd.Timestamp(end_date)) &
                        (opened_data["Sector_Coresight"].isin(selected_sectors))
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
                        st.session_state["sector_compare_selected_msa_name"] = ["All"]
                        st.session_state["sector_compare_selected_zip_name"] = ["All"]
                        st.session_state["sector_compare_selected_state_name"] = ["All"]
                        update_query_params()
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
                        st.session_state["sector_compare_selected_msa_name"] = ["All"]
                        st.session_state["sector_compare_selected_zip_name"] = ["All"]
                        st.session_state["sector_compare_selected_state_name"] = ["All"]
                        update_query_params()
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
                    current_filtered_data = opened_data.copy()
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
                        disabled=not (len(st.session_state["sector_comparison_selected_sectors"])>=2)
                    )
                    final_selected_states = mutually_exclusive_all(updated_selected_states, all_option, valid_states)
                    # If change, update and rerun for instant chip/UI correction
                    if final_selected_states != ss_state_name:
                        st.session_state["sector_compare_selected_state_name"] = final_selected_states
                        st.session_state["sector_compare_selected_msa_name"] = ["All"]  # reset MSA selection if state changed
                        st.session_state["sector_compare_selected_zip_name"] = ["All"]  # 
                        st.rerun()

                    st.markdown("""
                    <style>
                    .st-emotion-cache-wfksaw {
                        gap: 0.2rem !important; /* Reduce the gap to a smaller value */
                    }
                    </style>
                """, unsafe_allow_html=True)
                    _, center_col, _ = st.columns([20, 80, 10])
                    # ---- Persist location type for Compare Sectors ----
                    if "location_type_sectors" not in user_filters:
                        user_filters["location_type_sectors"] = st.session_state.get("location_type_sectors_opening", "MSA")
                    if "location_type_sectors_opening" not in st.session_state:
                        st.session_state["location_type_sectors_opening"] = user_filters.get("location_type_sectors", "MSA")

                    with center_col:
                        # location_type_sectors = st.radio(
                        #     "Filter by location",
                        #     "MSA",
                        #     horizontal=True,
                        #     key="location_type_sectors_opening",
                        #     label_visibility="collapsed"
                        # )
                        location_type_sectors = "MSA"
                        # if location_type_sectors != user_filters.get("location_type_sectors", "MSA"):
                        #     user_filters["location_type_sectors"] = location_type_sectors
                        #     cookie_controller.set(
                        #         "auth_data",
                        #         json.dumps(auth_cookie),                    # must be a string
                        #         expires=datetime.utcnow() + timedelta(days=30),
                        #         path="/",
                        #         domain=domain,
                        #     )
                        #     st.rerun()

                    # -------- MSA MULTISELECT ---------
                    if "All" in st.session_state["sector_compare_selected_state_name"]:
                        location_state_filtered = current_filtered_data
                    else:
                        location_state_filtered = current_filtered_data[
                            current_filtered_data["State"].isin(st.session_state["sector_compare_selected_state_name"])
                        ]
                    # -------- LOCATION MULTISELECT (MSA or ZIP) ---------
                    # MSA MULTISELECT
                    valid_locations = sorted(location_state_filtered['MsaName'].dropna().unique().tolist()) if not location_state_filtered.empty else []
                    options_list = [all_option] + valid_locations

                    old_sel = st.session_state.get("sector_compare_selected_msa_name", [])
                    ss_selected_loc = [m for m in old_sel if m == all_option or m in valid_locations]
                    if not ss_selected_loc or set(ss_selected_loc) == set(valid_locations):
                        default_locs = [all_option]
                    else:
                        default_locs = ss_selected_loc

                    multiselect_label = "Select Metropolitan Statistical Area (MSA)"
                    updated_selected = st.multiselect(
                        multiselect_label,
                        options=options_list,
                        default=default_locs,
                        help="A metro region centered on a large city plus its economically linked suburbs.",
                        key="sector_compare_multiselect_msa",
                        disabled=not (len(st.session_state["sector_comparison_selected_sectors"])>=2)
                    )

                    final_selected = mutually_exclusive_all(updated_selected, all_option, valid_locations)
                    if final_selected != ss_selected_loc:
                        st.session_state["sector_compare_selected_msa_name"] = final_selected
                        st.session_state["sector_compare_selected_zip_name"] = ["All"]
                        st.rerun()

                    # ZIP Code multiselect (independent)
                    selected_state_name = st.session_state.get("sector_compare_selected_state_name", ["All"])
                    selected_msa_name = st.session_state.get("sector_compare_selected_msa_name", ["All"])
                    if "All" not in selected_msa_name and selected_msa_name:
                        zip_pool = location_state_filtered[location_state_filtered['MsaName'].isin(selected_msa_name)]
                    elif "All" not in selected_state_name and selected_state_name:
                        zip_pool = location_state_filtered[location_state_filtered['State'].isin(selected_state_name)]
                    else:
                        zip_pool = location_state_filtered
                    valid_locations_zip = sorted(zip_pool["PostalCode"].dropna().astype(str).unique().tolist()) if not zip_pool.empty else []
                    options_list_zip = [all_option] + valid_locations_zip

                    old_sel_zip = st.session_state.get("sector_compare_selected_zip_name", [])
                    ss_selected_loc_zip = [z for z in old_sel_zip if z == all_option or z in valid_locations_zip]
                    # if len(valid_locations_zip) == 1 and ss_selected_loc_zip == ["All"]:
                    #     default_locs_zip = ["All"]
                    if not ss_selected_loc_zip :
                        default_locs_zip = [all_option]
                    else:
                        default_locs_zip = ss_selected_loc_zip

                    multiselect_label_zip = "Select Zip Code"
                    updated_selected_zip = st.multiselect(
                        multiselect_label_zip,
                        options=options_list_zip,
                        default=default_locs_zip,
                        help="U.S. postal area where the store is located.",
                        key="sector_compare_multiselect_zip",
                        disabled=not (len(st.session_state["sector_comparison_selected_sectors"])>=2)
                    )

                    final_selected_zip = mutually_exclusive_all(updated_selected_zip, all_option, valid_locations_zip)
                    if final_selected_zip != ss_selected_loc_zip:
                        st.session_state["sector_compare_selected_zip_name"] = final_selected_zip
                        st.rerun()

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
                        parent_filtered_data = opened_data[
                            opened_data['ParentName_Coresight'].isin(st.session_state["sector_compare_parent_chain_name"])
                        ]
                        parent_chain_list = parent_filtered_data['ChainName_Coresight'].dropna().unique().tolist()
                        selected_chain_names_union.update(parent_chain_list)

                    if st.session_state.get("sector_compare_selected_chain_name"):
                        selected_chain_names_union.update(st.session_state["sector_compare_selected_chain_name"])

                    # Filter opened_data for selected sectors and date range
                    start_date, end_date = st.session_state["sector_compare_date_range"]
                    filtered_opened_compare_for_sectors = opened_data[
                        (opened_data['Period'] >= pd.Timestamp(start_date)) &
                        (opened_data['Period'] <= pd.Timestamp(end_date)) &
                        (opened_data["Sector_Coresight"].isin(selected_sectors))
                    ]

                    # Apply state filter only if "All" is NOT selected
                    selected_states = st.session_state.get("sector_compare_selected_state_name", ["All"])
                    if "All" not in selected_states:
                        filtered_opened_compare_for_sectors = filtered_opened_compare_for_sectors[
                            filtered_opened_compare_for_sectors["State"].isin(selected_states)
                        ]

                    # Apply location filter (MSA and Zip independently)
                    # Apply MSA filter if selected
                    sel_msa = st.session_state.get("sector_compare_selected_msa_name", ["All"])
                    if "All" not in sel_msa:
                        filtered_opened_compare_for_sectors = filtered_opened_compare_for_sectors[
                            filtered_opened_compare_for_sectors["MsaName"].isin(sel_msa)
                        ]

                    # Apply ZIP filter if selected
                    sel_zip = st.session_state.get("sector_compare_selected_zip_name", ["All"])
                    if "All" not in sel_zip:
                        filtered_opened_compare_for_sectors = filtered_opened_compare_for_sectors[
                            filtered_opened_compare_for_sectors["PostalCode"].astype(str).isin([str(z) for z in sel_zip])
                        ]

                    # Create a copy for retailer-specific filtering
                    filtered_opened_compare_for_retailers = filtered_opened_compare_for_sectors.copy()
                    if selected_chain_names_union:
                        filtered_opened_compare_for_retailers = filtered_opened_compare_for_retailers[
                            filtered_opened_compare_for_retailers["ChainName_Coresight"].isin(selected_chain_names_union)
                        ]

                    if filtered_opened_compare_for_sectors.empty:
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
                        monthly_data = (
                            filtered_opened_compare_for_sectors
                            .groupby(['Period', 'Sector_Coresight'])
                            .size()
                            .reset_index(name='OpenedStores')
                        )
                        all_periods = pd.date_range(start=start_date, end=end_date, freq='MS')
                        full_data = pd.DataFrame()

                        for sector in selected_sectors:
                            sector_only = monthly_data[monthly_data['Sector_Coresight'] == sector]
                            sector_only = sector_only.set_index('Period').reindex(all_periods, fill_value=0).reset_index()
                            sector_only['Sector_Coresight'] = sector
                            full_data = pd.concat([full_data, sector_only])

                        # Pre-calculate metadata for each period and sector
                        metadata_dict = {}
                        for period in all_periods:
                            period_data = filtered_opened_compare_for_sectors[filtered_opened_compare_for_sectors['Period'] == period]
                            for sector in selected_sectors:
                                sector_data = period_data[period_data['Sector_Coresight'] == sector]
                                key = f"{period}|{sector}"
                                metadata_dict[key] = {
                                    'num_banners': sector_data['ChainName_Coresight'].nunique(),
                                    'num_states': sector_data['State'].nunique(),
                                    'num_msa': sector_data['MsaName'].nunique()
                                }

                        # Create figure with consistent styling
                        fig_line = go.Figure()
                        
                        # Add traces for each sector with custom styling
                        for sector, grp_df in full_data.groupby('Sector_Coresight'):
                            color = color_map.get(sector, "#333")  # Get color from your mapping
                            
                            # Prepare custom data for each point
                            custom_data = []
                            for _, row in grp_df.iterrows():
                                key = f"{row['index']}|{sector}"
                                metadata = metadata_dict.get(key, {'num_banners': 0, 'num_states': 0, 'num_msa': 0})
                                custom_data.append([
                                    metadata['num_banners'], 
                                    metadata['num_states'],
                                    metadata['num_msa']
                                ])
                            
                            fig_line.add_trace(go.Scatter(
                                x=grp_df['index'],
                                y=grp_df['OpenedStores'],
                                mode='lines+markers',
                                name=sector,
                                line=dict(color=color, width=2),
                                marker=dict(size=7, color=color),
                                hovertemplate=(
                                    "<b>%{x|%b %Y}</b><br>"  # Formatted month-year
                                    "<span style='color:" + color + "'>●</span> "
                                    "<b>" + sector + ":</b> %{y:,}<br><br>"
                                    # "<span style='color:" + color + "'>•</span> "
                                    # "<span style='color:black'>Date Period: %{x|%b %Y}</span><br>"
                                    "<span style='color:" + color + "'>•</span> "
                                    "<span style='color:black'>Number of Banners: %{customdata[0]}</span><br>"
                                    "<span style='color:" + color + "'>•</span> "
                                    "<span style='color:black'>Number of States: %{customdata[1]}</span><br>"
                                    "<span style='color:" + color + "'>•</span> "
                                    "<span style='color:black'>Number of MSA: %{customdata[2]}</span><br>"
                                    "<extra></extra>"
                                ),
                                customdata=custom_data,
                                hoverlabel=dict(
                                    bgcolor="white",
                                    bordercolor=color,
                                    font=dict(size=13, color="black", family="Arial")
                                )
                            ))

                        # Update layout with consistent styling
                        fig_line.update_layout(
                            title='Monthly Opened Stores by Sector',
                            xaxis_title="Month",
                            yaxis_title="Opened Stores",
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
                            margin=dict(l=10, r=10, t=60, b=10),
                            height=400  # Increased height to accommodate additional hover info
                        )

                        config = {
                            'displayModeBar': True,
                            'displaylogo': False,
                            'toImageButtonOptions': {
                                'format': 'png',
                                'filename': 'monthly_opened_stores_by_sector',
                                'height': 400,
                                'width': 700,
                                'scale': 1
                            }
                        }

                        st.plotly_chart(fig_line, use_container_width=True, config=config)

                    with bar_col:
                        total_counts = (
                            filtered_opened_compare_for_sectors
                            .groupby('Sector_Coresight')
                            .size()
                            .reset_index(name='TotalOpenedStores')
                        )
                        
                        # Create figure with consistent styling
                        fig_bar = go.Figure()
                        
                        for _, row in total_counts.iterrows():
                            sector = row['Sector_Coresight']
                            color = color_map.get(sector, "#333")  # Get color from your mapping
                            
                            fig_bar.add_trace(go.Bar(
                                x=[sector],
                                y=[row['TotalOpenedStores']],
                                name=sector,
                                marker_color=color,
                                text=[f"{row['TotalOpenedStores']:,}"],
                                textposition='outside',
                                hovertemplate=(
                                    "<b>%{x}</b><br>"  # Sector name
                                    "<span style='color:" + color + "'>●</span> "
                                    "<b>Total Opened Stores:</b> %{y:,}<extra></extra>"
                                ),
                                hoverlabel=dict(
                                    bgcolor="white",
                                    bordercolor=color,
                                    font=dict(size=13, color="black", family="Arial")
                                )
                            ))
                        
                        fig_bar.update_layout(
                            title='Total Opened Stores by Sector',
                            xaxis_title="Sector",
                            yaxis_title="Opened Stores",
                            showlegend=False,
                            height=500,
                            margin=dict(l=10, r=10, t=60, b=10),
                            xaxis=dict(
                                showline=True,
                                zeroline=False
                            ),
                            yaxis=dict(
                                showline=True,
                                zeroline=False
                            )
                        )
                        st.plotly_chart(fig_bar, use_container_width=True)

                    # Retailer Performance by Sector section
                    st.subheader("Banner Performance by Sector")
                    
                    # Determine which data to use based on retailer selections
                    display_data = filtered_opened_compare_for_retailers if selected_chain_names_union else filtered_opened_compare_for_sectors
                    
                    for sector in selected_sectors:
                        sector_data = display_data[display_data['Sector_Coresight'] == sector]
                        if sector_data.empty:
                            continue

                        retailer_counts = (
                            sector_data['ChainName_Coresight']
                            .value_counts()
                            .reset_index()
                        )
                        retailer_counts.columns = ['Banners', 'OpenedStores']

                        title = f'Top Banners in {sector}'
                        color = color_map[sector]  # Get the sector color
                        
                        # Create figure with consistent styling
                        fig_retailer = go.Figure()
                        
                        fig_retailer.add_trace(go.Bar(
                            x=retailer_counts['Banners'],
                            y=retailer_counts['OpenedStores'],
                            name=sector,
                            marker_color=color,
                            text=retailer_counts['OpenedStores'],
                            textposition='outside',
                            hovertemplate=(
                                "<b>%{x}</b><br>"  # Banner name
                                "<span style='color:" + color + "'>●</span> "
                                "<b>Opened Stores:</b> %{y:,}<extra></extra>"
                            ),
                            hoverlabel=dict(
                                bgcolor="white",
                                bordercolor=color,
                                font=dict(size=13, color="black", family="Arial")
                            )
                        ))

                        fig_retailer.update_layout(
                            title=title,
                            xaxis={
                                'categoryorder': 'total descending',
                                'title': None
                            },
                            yaxis={
                                'showticklabels': False,
                                'showgrid': False,
                                'title': None
                            },
                            showlegend=False,
                            height=400,
                            margin=dict(t=60, b=40, l=10, r=10),
                            uniformtext_minsize=8,
                            uniformtext_mode='hide'
                        )
                        
                        st.plotly_chart(fig_retailer, use_container_width=True)

                    # Prepare table data
                    table_data = display_data.copy()
                    table_data['Year'] = table_data['Period'].dt.year
                    table_data['Month'] = table_data['Period'].dt.month
                    table_data['Opening Month/Year'] = (table_data['Month'].astype(str) + '-' + table_data['Year'].astype(str))
                    
                    # Select and rename columns
                    table_data = table_data[[
                        'ChainName_Coresight', 'ParentName_Coresight', 'Address', 'Address2',
                        'City', 'MsaName', 'PostalCode', 'State', 'Country', 'Sector_Coresight',
                        'Opening Month/Year'
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
                        'Opening Month/Year': 'Opened Date'
                    })

                    col1, col4  = st.columns([7, 1.05])
                    with col1:
                        st.subheader('Opened Stores Table')

                    # ---- Download + AG Grid with trial overlay for `table_data` ----

                    with col4:
                        if not table_data.empty:
                            excel_buffer = io.BytesIO()
                            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                                table_data.to_excel(writer, index=False, sheet_name='Opened_Stores')
                                worksheet = writer.sheets['Opened_Stores']
                                for i, col in enumerate(table_data.columns):
                                    column_len = max(
                                        table_data[col].astype(str).map(len).max(),
                                        len(col)
                                    )
                                    worksheet.set_column(i, i, column_len + 2)

                            formatted_start_date = datefmt(start_date, "%Y-%m-%d", default="")
                            formatted_end_date   = datefmt(end_date,   "%Y-%m-%d", default="")

                            final_filename = f"Opened_Stores_{formatted_start_date}_to_{formatted_end_date}.xlsx"

                            # Download button styles
                            st.markdown("""
                                <style>
                                    .stDownloadButton>button {
                                        background-color: #A3C0CE;
                                        color: white;
                                        border: none;
                                    }
                                    .stDownloadButton>button:hover {
                                        border: 2px solid #A3C0CE;
                                        background-color: white;
                                        color: #A3C0CE;
                                    }
                                </style>
                            """, unsafe_allow_html=True)

                            if not is_free_trial:
                                # Premium users → real download
                                excel_buffer.seek(0)
                                st.download_button(
                                    label="Download Data",
                                    data=excel_buffer,
                                    file_name=final_filename,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_table_xlsx",
                                    use_container_width=True
                                )
                            else:
                                # Trial users → fake download button that opens your modal
                                if st.button("Download Data", key="download_table_blocked", use_container_width=True):
                                    _trial_modal_opened()

                        st.markdown('</div>', unsafe_allow_html=True)

                    GRID_H = 600  # keep consistent with the grid height you want

                    df_for_grid = table_data

                    # Lock icon
                    ICON = pathlib.Path(__file__).resolve().parent.parent / "assets" / "icons" / "lock.png"
                    lock_b64 = base64.b64encode(ICON.read_bytes()).decode("utf-8")

                    gb = GridOptionsBuilder.from_dataframe(df_for_grid)
                    grid_options = gb.build()

                    AgGrid(
                        df_for_grid,
                        gridOptions=grid_options,
                        height=GRID_H,
                        key="table_grid",
                    )

                    if is_free_trial:
                        st.markdown(f"""
                        <style>
                          .grid-overlay-flag[data-target="table"] {{
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

                        <div class="grid-overlay-flag" data-target="table">
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
            document and <a href="/change-logs#store-intelligence-platform" target="_self">Data Release Notes</a>
            for more details.
            </p>
            """,
            unsafe_allow_html=True,
        )
        placeholder.empty()

    include_html("footer.html")
except ValueError as e:
    st.write("No data available. Please reset filters or refresh page.")
    placeholder.empty()