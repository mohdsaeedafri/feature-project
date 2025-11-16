"""
BasePage - Foundation class for all SIP pages
Handles authentication, page configuration, headers/footers, and database connections
"""
import streamlit as st
import pandas as pd
import mysql.connector
import os
import json
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from streamlit_cookies_controller import CookieController
from html_utils import include_html
from auth_utils import logout, require_auth, get_current_domain

# Load environment variables
load_dotenv()

class BasePage:
    def __init__(self, page_title="Store Intelligence Platform", layout="wide"):
        """Initialize the base page with common configuration"""
        self.page_title = page_title
        self.layout = layout
        self._setup_page_config()
        self._setup_logging()
        self._setup_session_state()
        self._setup_auth()
        self._setup_styling()
        
    def _setup_page_config(self):
        """Configure the Streamlit page"""
        st.set_page_config(
            page_title=self.page_title,
            layout=self.layout,
            page_icon="https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
            initial_sidebar_state="collapsed"
        )
        
    def _setup_logging(self):
        """Setup logging configuration"""
        LOG_LEVEL_STR = os.getenv("LOG_LEVEL") or 'INFO'
        log_level = getattr(logging, LOG_LEVEL_STR, logging.INFO)
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )
        
    def _setup_session_state(self):
        """Initialize session state variables"""
        if "expand_filters" not in st.session_state:
            st.session_state["expand_filters"] = True
            
    def _setup_auth(self):
        """Setup authentication"""
        self.session_id = require_auth()
        self.cookie_controller = CookieController(key="auth_cookies")
        self._load_auth_cookie()
        self.domain = get_current_domain()
        
    def _load_auth_cookie(self):
        """Load authentication cookie"""
        raw = self.cookie_controller.get("auth_data")
        try:
            self.auth_cookie = json.loads(raw) if isinstance(raw, str) else (raw if isinstance(raw, dict) else {})
        except Exception:
            self.auth_cookie = {}
            
    def save_auth_cookie(self):
        """Save authentication cookie"""
        try:
            self.cookie_controller.set(
                "auth_data",
                json.dumps(self.auth_cookie),
                expires=datetime.utcnow() + timedelta(days=30),
                path="/",
                domain=self.domain,
            )
        except Exception as e:
            logging.warning(f"cookie write failed: {e}")
            
    def _setup_styling(self):
        """Apply common CSS styling"""
        st.markdown("""
            <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
            <meta http-equiv="Pragma" content="no-cache">
            <meta http-equiv="Expires" content="0">
        """, unsafe_allow_html=True)
        
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
        
        # Hide Streamlit default styling
        hide_streamlit_style = """
        <style>
        #MainMenu {visibility: hidden;}
        .css-1y0tads, .block-container {
            padding-top: 2.1rem !important;
        }
        </style>
        """
        st.markdown(hide_streamlit_style, unsafe_allow_html=True)
        
        # Bootstrap CSS
        st.markdown('<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">', unsafe_allow_html=True)
        
    def render_header(self):
        """Render the common header"""
        include_html("header.html")
        
    def render_navigation_buttons(self, changelog_return_page="active"):
        """Render the common navigation buttons"""
        col_space, col4, col1, col2, col3 = st.columns([56, 14, 14, 8, 8])
        
        user_filters = self.auth_cookie.get("filters", {}) if isinstance(self.auth_cookie, dict) else {}
        
        with col4:
            if self.auth_cookie.get("membership_type") == "Employee":
                is_staging = os.getenv("ENVIRONMENT", "").lower() == "staging"
                if is_staging:
                    overview_url = "https://sip-portal-stg.coresight.com/app_release_portal"
                else:
                    overview_url = "https://sip-portal.coresight.com/app_release_portal"
                st.link_button("App Release Portal", overview_url, type="tertiary", icon=":material/release_alert:")
                
        with col1:
            if st.button("Data Release Notes", key="go_to_changelog", type="tertiary", icon=":material/history_2:"):
                user_filters["returnPage"] = changelog_return_page
                self.auth_cookie["filters"] = user_filters
                self.save_auth_cookie()
                st.switch_page("pages/changelogs.py")
                
        with col2:
            is_staging = os.getenv("ENVIRONMENT", "").lower() == "staging"
            if is_staging:
                overview_url = "https://stage3.coresight.com/store-intelligence-platform-overview/"
            else:
                overview_url = "https://www.coresight.com/store-intelligence-platform-overview/"
            st.link_button("Overview", overview_url, type="tertiary", icon=":material/help:")
            
        with col3:
            if st.button("Log Out", key="Logout", type="tertiary", icon=":material/logout:"):
                logout()
                
    def get_database_connection(self):
        """Get database connection with environment variables"""
        connection_params = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME')
        }
        return mysql.connector.connect(**connection_params)
        
    def get_synchronized_date_range(self, data, default_months_back=12, label=""):
        """Get synchronized date range for data filtering"""
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