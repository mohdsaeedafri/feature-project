"""
Refactored Store Openings Page - Using the new modular SIP components
This demonstrates the modular approach by using the new components with actual data
"""
import streamlit as st
import pandas as pd
import logging
from datetime import date
import json
import calendar
from datetime import datetime, timedelta
import plotly.graph_objects as go
import io
import mysql.connector
import re
import os
from rapidfuzz import process, fuzz
from sqlalchemy import create_engine, text

# Import our new modular components
from sip_components.base_page import BasePage
from sip_components.ui_components import UIComponents
from sip_components.database_manager import DatabaseManager
from sip_components.filter_manager import FilterManager
from sip_components.styling_manager import StylingManager
from sip_components.tab_components import TabComponents
from html_utils import include_html
from streamlit_cookies_controller import CookieController

# Initialize the base page (handles auth, config, headers, etc.)
page = BasePage(page_title="Store Openings")

# Render common page elements
page.render_header()
page.render_navigation_buttons(changelog_return_page="opening")

# Apply consistent styling
styling = StylingManager()
styling.apply_global_styles()
styling.hide_streamlit_elements()
styling.apply_bootstrap()

# Initialize other components
ui = UIComponents()
db = DatabaseManager()
filter_manager = FilterManager(page.auth_cookie,page_prefix="opening_",base_page=page)
tabs = TabComponents()

# Get cookie controller for auth
cookie_controller = CookieController(key="auth_cookies")
raw = cookie_controller.get("auth_data")
try:
    auth_cookie = json.loads(raw) if isinstance(raw, str) else (raw if isinstance(raw, dict) else {})
except Exception:
    auth_cookie = {}

user_filters = auth_cookie.get("filters", {}) if isinstance(auth_cookie, dict) else {}
domain = page.domain

def save_auth_cookie():
    """Save authentication cookie"""
    try:
        cookie_controller.set(
            "auth_data",
            json.dumps(auth_cookie),
            expires=datetime.utcnow() + timedelta(days=30),
            path="/",
            domain=domain,
        )
    except Exception as e:
        logging.warning(f"cookie write failed: {e}")

# Render tab navigation (now includes the Store Intelligence Platform heading)
tabs.render_tab_navigation(current_page="opening")

# Function to fetch actual data from database
@st.cache_data(show_spinner=False)
def fetch_opening_data():
    """Fetch actual opening data from database"""
    # Connect to the database using environment variables
    connection_params = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME')
    }

    if os.getenv('ENABLE_SSL', 'false').lower() == 'true' and os.getenv('SSL_CA'):
        connection_params['ssl_ca'] = os.getenv('SSL_CA')
        logging.info("SSL connection enabled")
    else:
        logging.info("SSL connection not enabled")
    
    conn = mysql.connector.connect(**connection_params)
    
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
    conn.close()
    return opened_df

# Add a new function to fetch square footage data separately
@st.cache_data(show_spinner=False)
def fetch_square_footage_data():
    """Fetch square footage data from parent_chain_names_data table"""
    connection_params = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME')
    }
    if os.getenv('ENABLE_SSL', 'false').lower() == 'true' and os.getenv('SSL_CA'):
        connection_params['ssl_ca'] = os.getenv('SSL_CA')
        logging.info("SSL connection enabled")
    else:
        logging.info("SSL connection not enabled")
    conn = mysql.connector.connect(**connection_params)
    
    # Fetch parent_chain_names_data for square footage calculation
    parent_chain_query = "SELECT ParentName_Coresight, ChainName_Coresight, Average_Square_Footage FROM parent_chain_names_data"
    parent_chain_df = pd.read_sql(parent_chain_query, con=conn)
    
    conn.close()
    return parent_chain_df

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

placeholder = st.empty()
st.markdown("""
    <style>
    /* Hide Streamlit's built-in spinner */
    .stSpinner {
        display: none !important;
    }
    </style>
""", unsafe_allow_html=True)
# Show loading spinner while fetching data
with st.spinner(""):
    placeholder.markdown(
        """
         <style>
        .loader-container {
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            height: 30vh;
        }
        .loader {
            border: 8px solid #f3f3f3; /* Light grey */
            border-top: 8px solid #e74c3c; /* Blue */
            border-radius: 50%;
            width: 80px;
            height: 80px;
            animation: spin 1s linear infinite;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .loading-text {
            margin-top: 20px;
            font-size: 1rem;
            font-weight: 500;
            color: #444;
            font-family: "Segoe UI", Arial, sans-serif;
        }
        </style>

        <div class="loader-container">
            <div class="loader"></div>
            <div class="loading-text">Loading Dashboard – this may take 2–3 minutes, please wait…</div>
        </div>
        """,
        unsafe_allow_html=True
    )
    # Fetch actual data from database
    opened_data = fetch_opening_data()
    opened_data['Period'] = pd.to_datetime(opened_data['Period'], errors='coerce')
    latest_ts = opened_data["Period"].max()
    latest_ts = tabs.datefmt(latest_ts, "%B %Y")
    
    # Fetch square footage data separately
    square_footage_data = fetch_square_footage_data()
    placeholder.empty()  # Remove loading spinner after data is fetched
# Filter section
st.markdown("<div style='margin-top: 0px;'></div>", unsafe_allow_html=True)

# Render tab selector
selected_tab, top_col2, top_col3 = tabs.render_tab_selector(default_tab="Base Dashboard")

with top_col3:
    tabs.render_filter_controls()

with top_col2:
    tabs.render_reset_button(cookie_controller, auth_cookie, opened_data)

# Handle different tabs
if selected_tab == "Base Dashboard":
    # Render filters using the filter manager and get filter values
    filter_values = filter_manager.render_all_filters(opened_data)

    # Apply all filters to data
    filtered_data = filter_manager.apply_all_filters(opened_data)
    filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
    
    # Calculate previous period data for comparison
    if len(filtered_data) > 0 and 'Period' in filtered_data.columns:
        current_min_period = filtered_data['Period'].min()
        # Get data from previous period (same range, shifted back by the period range)
        period_range = filtered_data['Period'].max() - filtered_data['Period'].min()
        previous_min = current_min_period - period_range
        previous_max = current_min_period
        previous_data = opened_data[(opened_data['Period'] >= previous_min) & (opened_data['Period'] < previous_max)]
    else:
        previous_data = None

    # Render metrics using TabComponents with square footage data
    total_stores, total_retailers, total_sectors = tabs.render_metrics_section(
        filtered_data, 
        metrics_title="Opened", 
        previous_data=previous_data, 
        show_extended_metrics=True,
        square_footage_data=square_footage_data,
        chain_name_selected=filter_values["selected_chain_name"],
        parent_chain_name_selected=filter_values["parent_chain_name"]
    )

    # Render charts using TabComponents
    tabs.render_standard_charts(filtered_data, chart_title_prefix="Opened")

    # Render data table using TabComponents
    tabs.render_data_table(filtered_data, table_title="Opened Stores")

elif selected_tab == "Compare Retailers":
    # Create a custom filter manager for Compare Retailers tab with different session state keys
    compare_retailers_filter_manager = FilterManager(page.auth_cookie)
    
    # Override the session state keys to avoid conflicts with base dashboard
    # compare_retailers_filter_manager.user_filters = user_filters.copy()
    # Define a function to determine when location filters should be enabled
    def retailers_location_enable_conditions(parent_chain_name, selected_chain_name, sector_selection):
        # Enable location filters only if either parent or chain filters have selections
        return len(parent_chain_name) > 0 or len(selected_chain_name) > 0
    
    # Render all filters with special conditions for Compare Retailers tab
    filter_values = compare_retailers_filter_manager.render_all_filters(
        opened_data,
        session_state_key_prefix="compare_retailers_",
        location_enable_conditions=retailers_location_enable_conditions
    )
    
    parent_chain_name = filter_values["parent_chain_name"]
    selected_chain_name = filter_values["selected_chain_name"]
    
    # Update session state with the selected values for proper tracking
    st.session_state["selected_parent_names_v2"] = parent_chain_name
    st.session_state["selected_chain_names_v2"] = selected_chain_name
            
    # Check if at least 2 retailers are selected
    total_selected_retailers = len(parent_chain_name) + len(selected_chain_name)
        
    if total_selected_retailers < 2:
        st.info("Please select at least two retailers (either companies or banners) to start the retailer comparison")
    else:
        # Apply all filters to data for final display using the correct session state key prefix
        filtered_data = compare_retailers_filter_manager.apply_all_filters(opened_data, session_state_key_prefix="compare_retailers_")
        filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
        # Render charts and data table
        # Check if filtered_data is empty by checking its length
        is_empty = len(filtered_data) == 0
        if not is_empty and total_selected_retailers >= 2:
            # Render metrics (restoring as requested)
            tabs.render_metrics_section(filtered_data, metrics_title="Opened", show_extended_metrics=False, square_footage_data=square_footage_data, chain_name_selected=filter_values["selected_chain_name"], parent_chain_name_selected=filter_values["parent_chain_name"])
            
            # Render comparison charts
            tabs.render_compare_retailers_charts(filtered_data,"", parent_chain_name, selected_chain_name, chart_title_prefix="Opened")
            
            # Render data table
            tabs.render_data_table(filtered_data, table_title="Opened Stores - Compare Retailers")
        else:
            st.info("No data available for the selected filters")

elif selected_tab == "Compare Sectors":
    # Create a custom filter manager for Compare Sectors tab with different session state keys
    compare_sectors_filter_manager = FilterManager(page.auth_cookie)
    
    # Override the session state keys to avoid conflicts with base dashboard
    compare_sectors_filter_manager.user_filters = user_filters.copy()
    
    # Define functions to determine when filters should be enabled
    def sectors_retailer_enable_conditions(parent_chain_name, selected_chain_name, sector_selection):
        # Enable retailer filters only if at least 1 sector is selected
        return sector_selection and len(sector_selection) >= 2
    
    def sectors_location_enable_conditions(parent_chain_name, selected_chain_name, sector_selection):
        # Enable location filters only if at least 2 sectors are selected
        return sector_selection and len(sector_selection) >= 2
    
    # Render all filters with special conditions for Compare Sectors tab
    filter_values = compare_sectors_filter_manager.render_all_filters(
        opened_data,
        session_state_key_prefix="sector_compare_",
        use_sector_comparison=True,
        retailer_filter_disabled=False,  # Initially disabled
        location_enable_conditions=sectors_location_enable_conditions
    )
    
    sector_comparison_selected_sectors = filter_values["sector_selection"]
    parent_chain_name = filter_values["parent_chain_name"]
    selected_chain_name = filter_values["selected_chain_name"]
    
    # Update session state with the selected values for proper tracking
    st.session_state["sector_comparison_selected_sectors"] = sector_comparison_selected_sectors
    st.session_state["sector_compare_parent_chain_name"] = parent_chain_name
    st.session_state["sector_compare_selected_chain_name"] = selected_chain_name
            
    # Check if at least 2 sectors are selected
    if len(sector_comparison_selected_sectors) < 2:
        st.info("Please select at least two sectors to start the sector comparison")
    else:
        # Apply all filters to data for final display using the correct session state key prefix
        filtered_data = compare_sectors_filter_manager.apply_all_filters(opened_data, session_state_key_prefix="sector_compare_")
        filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
        # Render charts and data table
        # Check if filtered_data is empty by checking its length
        is_empty = len(filtered_data) == 0
        if not is_empty:
            # Don't render metrics for Compare Sectors tab
            
            # Render comparison charts
            tabs.render_compare_sectors_charts(filtered_data, sector_comparison_selected_sectors, chart_title_prefix="Opened")
            
            # Render data table
            tabs.render_data_table(filtered_data, table_title="Opened Stores - Compare Sectors")
        else:
            st.info("No data available for the selected filters")

ui.render_data_disclaimer(latest_ts)

include_html("footer.html")