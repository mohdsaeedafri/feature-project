"""
Refactored Store Closings Page - Using the new modular SIP components
This demonstrates the modular approach by using the new components with actual data
"""
from traceback import print_tb
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
page = BasePage(page_title="Store Closings")

# Render common page elements
page.render_header()
page.render_navigation_buttons(changelog_return_page="closing")

# Apply consistent styling
styling = StylingManager()
styling.remove_metric_link()
 

# Initialize other components
ui = UIComponents()
db = DatabaseManager()
filter_manager = FilterManager(page.auth_cookie, page_prefix="closing_")
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
tabs.render_tab_navigation(current_page="closing")

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
    parent_chain_df['Average_Square_Footage'] = pd.to_numeric(parent_chain_df['Average_Square_Footage'], errors='coerce')
    parent_chain_df['Average_Square_Footage'] = parent_chain_df['Average_Square_Footage'] * 1000.0
    
    conn.close()
    return parent_chain_df

# Function to fetch actual data from database
@st.cache_data(show_spinner=False)
def fetch_closing_data():
    """Fetch actual closing data from database"""
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
    
    # SQL query
    closed_query = """select c.storename, c.storetype, c.ChainName_Coresight, c.ParentName_Coresight, 
                                c.Address, c.Address2, c.City, c.MsaName, c.PostalCode, c.State, c.Country, 
                                c.Sector_Coresight, c.Period, c.Population, c.UpdateCycle from (
                                SELECT storename, storetype, ChainName_Coresight, ParentName_Coresight, Address, 
                                      Address2, City, MsaName, PostalCode, State, Country, Sector_Coresight, Period, 
                                      Population, status, UpdateCycle, duration_closing, hashid
                                FROM all_closed_py
                                union all
                                SELECT storename, storetype, ChainName_Coresight, ParentName_Coresight, Address, 
                                Address2, City, MsaName, PostalCode, State, Country, Sector_Coresight, Period, 
                                Population, status, UpdateCycle, duration_closing, hashid
                                FROM all_closed_cy
                                UNION ALL
                                SELECT storename, storetype, ChainName_Coresight, ParentName_Coresight, Address, 
                                Address2, City, MsaName, PostalCode, State, Country, Sector_Coresight, Period, 
                                Population, status, UpdateCycle, duration_closing, hashid
                                FROM all_closed_acquisition
                                UNION ALL
                                SELECT storename, storetype, ChainName_Coresight, ParentName_Coresight, Address, 
                                Address2, City, MsaName, PostalCode, State, Country, Sector_Coresight, Period, 
                                Population, status, UpdateCycle, duration_closing, hashid
                                FROM all_closed_bankruptcy
                                ) c LEFT JOIN parent_chain_names_data p 
                                  ON c.chainname_coresight = p.chainname_coresight
                                where
                                (
                                  (c.ChainName_Coresight IS NULL OR c.ChainName_Coresight NOT IN ('Hoka', 'Sephora', 'Lululemon Athletica','Finish Line','Sunglass Hut',"Carter's",'Marc Jacobs',
                                  'Bulgari', "Dick's Sporting Goods", 'CVS Pharmacy', 'Circle K', 'Aerie', 'American Eagle Outfitters', 'Aerie', 'American Eagle Outfitters',
                                  '7-Eleven','Anthropologie','Babies"R"Us','Balenciaga','Bottega Veneta','Burberry','Cartier','Century 21','Chanel','Christian Dior',"Claire’s",'Coach',
                                  "Conn's",'Converse','Deciem','Dolce & Gabbana','Fendi','Giant Eagle','Givenchy','Gucci','Hallmark','Hy-Vee','James Avery Artisan Jewelry',
                                  "L'Occitane",'LensCrafters',"Levi's",'Lord & Taylor','Louis Vuitton','LUSH','Marc Jacobs','Massimo Dutti','Moncler','Pandora','Prada','Ralph Lauren',
                                  'Ted Baker','The North Face','Under Armour','Williams Sonoma','Yves Saint laurent','Free People'))
                                  OR (c.ChainName_Coresight = 'Hoka' AND LOWER(c.storename) LIKE '%hoka%')
                                  OR (c.ChainName_Coresight = 'Sephora' AND LOWER(c.storename) NOT LIKE '%penney%' AND LOWER(c.storename) NOT LIKE '%kohl%')
                                  OR (c.ChainName_Coresight = 'Lululemon Athletica' AND COALESCE(c.storetype, '') NOT IN ('popup','seasonal'))
                                  OR (c.ChainName_Coresight = 'Finish Line' AND LOWER(c.storename) NOT LIKE '%macy%' AND LOWER(c.storename) NOT LIKE '%jd sports%')
                                  AND (c.ChainName_Coresight = 'Finish Line' AND COALESCE(c.storetype, '') NOT IN ('JD Sports'))
                                  OR (c.ChainName_Coresight = 'Sunglass Hut' AND LOWER(c.storename) NOT LIKE '%macy%' AND LOWER(c.storename) NOT LIKE '%bloomingdale%' AND LOWER(c.storename) NOT LIKE '%bass pro%' AND LOWER(c.storename) NOT LIKE '%cabela%' AND LOWER(c.storename) NOT LIKE '%saks%' AND LOWER(c.storename) NOT LIKE '%belk%')
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

    # Run the query and read into DataFrame
    closed_df = pd.read_sql(closed_query, con=conn)
    conn.close()
    
    # Convert 'Period' to datetime format
    closed_df['Period'] = pd.to_datetime(closed_df['Period'], errors='coerce').dt.date

    # Clean and convert 'UpdateCycle' to integer, handling NaN values
    closed_df['UpdateCycle'] = (
        closed_df['UpdateCycle']
        .fillna(30)
        .astype(int)
    )
    
    return closed_df
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
    closed_data = fetch_closing_data()
    print("Closed Data refactored:", closed_data.shape)
    square_footage_data = fetch_square_footage_data()
    latest_ts = closed_data["Period"].max()
    latest_ts = tabs.datefmt(latest_ts, "%B %Y")
    placeholder.empty()
# Filter section
st.markdown("<div style='margin-top: 0px;'></div>", unsafe_allow_html=True)

# Render tab selector
selected_tab, top_col2, top_col3 = tabs.render_tab_selector(default_tab="Base Dashboard")

with top_col3:
    tabs.render_filter_controls()

with top_col2:
    tabs.render_reset_button(cookie_controller, auth_cookie, closed_data)

# Handle different tabs
if selected_tab == "Base Dashboard":
    # Render filters using the filter manager and get filter values
    filter_values = filter_manager.render_all_filters(closed_data)

    # Apply all filters to data
    filtered_data = filter_manager.apply_all_filters(closed_data)
    filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
    # Calculate previous period data for comparison
    if len(filtered_data) > 0 and 'Period' in filtered_data.columns:
        current_min_period = filtered_data['Period'].min()
        # Get data from previous period (same range, shifted back by the period range)
        period_range = filtered_data['Period'].max() - filtered_data['Period'].min()
        previous_min = current_min_period - period_range
        previous_max = current_min_period
        print("Previous Period:", previous_min, previous_max)
        previous_data = closed_data[(closed_data['Period'] >= previous_min) & (closed_data['Period'] < previous_max)]
        
    else:
        previous_data = None
    print("Previous Data in closing:", previous_data.shape if previous_data is not None else None)

    # Render metrics using TabComponents
    styling.render_horizontal_line()
    tabs.render_metrics_section(filtered_data, metrics_title="Closed", previous_data=previous_data, show_extended_metrics=True, square_footage_data=square_footage_data, chain_name_selected=filter_values["selected_chain_name"], parent_chain_name_selected=filter_values["parent_chain_name"])

    # Render charts using TabComponents
    tabs.render_standard_charts(filtered_data, chart_title_prefix="Closed")

    # Render data table using TabComponents
    tabs.render_data_table(filtered_data, table_title="Closed Stores Table")

elif selected_tab == "Compare Retailers":
    # Create a custom filter manager for Compare Retailers tab with different session state keys
    compare_retailers_filter_manager = FilterManager(page.auth_cookie)
    
    # Override the session state keys to avoid conflicts with base dashboard
    compare_retailers_filter_manager.user_filters = user_filters.copy()
    
    # Define a function to determine when location filters should be enabled
    def retailers_location_enable_conditions(parent_chain_name, selected_chain_name, sector_selection):
        # Enable location filters only if either parent or chain filters have selections
        return len(parent_chain_name) > 0 or len(selected_chain_name) > 0
    
    # Render all filters with special conditions for Compare Retailers tab
    filter_values = compare_retailers_filter_manager.render_all_filters(
        closed_data,
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
    print("Total Selected Retailers in closing:", parent_chain_name)
    if total_selected_retailers < 2:
        st.info("Please select at least two retailers (either companies or banners) to start the retailer comparison")
    else:
        # Apply all filters to data for final display using the correct session state key prefix
        filtered_data = compare_retailers_filter_manager.apply_all_filters(closed_data, session_state_key_prefix="compare_retailers_")
        filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
        # Render charts and data table
        # Check if filtered_data is empty by checking its length
        is_empty = len(filtered_data) == 0
        if not is_empty and total_selected_retailers >= 2:
            # Render metrics (restoring as requested)
            styling.render_horizontal_line()
            tabs.render_metrics_section(filtered_data, metrics_title="Closed", show_extended_metrics=False, square_footage_data=square_footage_data, chain_name_selected=filter_values["selected_chain_name"], parent_chain_name_selected=filter_values["parent_chain_name"])
            
            # Render comparison charts
            tabs.render_compare_retailers_charts(filtered_data,"", parent_chain_name, selected_chain_name, chart_title_prefix="Closed")
            
            # Render data table
            tabs.render_data_table(filtered_data, table_title="Closed Stores Table")
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
        return sector_selection and len(sector_selection) >= 1
    
    def sectors_location_enable_conditions(parent_chain_name, selected_chain_name, sector_selection):
        # Enable location filters only if at least 2 sectors are selected
        return sector_selection and len(sector_selection) >= 2
    print("Sector Selection in closing:", sectors_location_enable_conditions)
    # Render all filters with special conditions for Compare Sectors tab
    filter_values = compare_sectors_filter_manager.render_all_filters(
        closed_data,
        session_state_key_prefix="sector_compare_",
        use_sector_comparison=True,
        retailer_filter_disabled=True,  # Initially disabled
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
        filtered_data = compare_sectors_filter_manager.apply_all_filters(closed_data, session_state_key_prefix="sector_compare_")
        filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
        # Render charts and data table
        # Check if filtered_data is empty by checking its length
        is_empty = len(filtered_data) == 0
        if not is_empty:
            # Don't render metrics for Compare Sectors tab
            
            # Render comparison charts
            tabs.render_compare_sectors_charts(filtered_data, sector_comparison_selected_sectors, chart_title_prefix="Closed")
            
            # Render data table
            tabs.render_data_table(filtered_data, table_title="Closed Stores Table")
        else:
            st.info("No data available for the selected filters")


ui.render_data_disclaimer(latest_ts)
include_html("footer.html")