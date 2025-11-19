"""
Refactored Store Active Page - Using the new modular SIP components
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
page = BasePage(page_title="Active Stores")

# Render common page elements
page.render_header()
page.render_navigation_buttons(changelog_return_page="active")

# Apply consistent styling
styling = StylingManager()
styling.remove_metric_link()

# Initialize other components
ui = UIComponents()
db = DatabaseManager()
filter_manager = FilterManager(page.auth_cookie, page_prefix="active_")
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
tabs.render_tab_navigation(current_page="active")

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
def fetch_active_data():
    """Fetch actual active data from database"""
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
    cursor = conn.cursor()
    
    cursor.execute("SELECT MAX(Period) FROM all_active_cy")
    max_cy = cursor.fetchone()[0]  # type: ignore
    
    cursor.execute("SELECT MAX(Period) FROM all_active_py")
    max_py = cursor.fetchone()[0]  # type: ignore
    
    cursor.close()
    conn.close()
    
    max_period = max(pd.to_datetime(max_cy), pd.to_datetime(max_py)).date()  # type: ignore
    twelve_months_back = (max_period - pd.DateOffset(months=11)).replace(day=1).date()  # type: ignore
    # Initialize with database-derived dates (last 12 months)
    query_start_date = twelve_months_back
    query_end_date = max_period
    
    # Check query parameters for custom date range
    qparams = st.query_params
    start_date_param = qparams.get("start_date", None)
    end_date_param = qparams.get("end_date", None)
    
    # Only expand query if BOTH parameters are provided
    if start_date_param is not None and end_date_param is not None:
        try:
            requested_start = pd.to_datetime(start_date_param).date()  # type: ignore
            requested_end = pd.to_datetime(end_date_param).date()  # type: ignore
            
            # Adjust query dates based on requested range
            if requested_start < twelve_months_back:
                query_start_date = requested_start
            
            if requested_end > max_period:  # type: ignore
                query_end_date = max_period
            else:
                query_end_date = requested_end
        except Exception:
            pass
    
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
    
    # Fetch from all three tables
    def fetch_query(query):
        local_conn = mysql.connector.connect(**connection_params)
        df = pd.read_sql(query, local_conn)
        local_conn.close()
        return df
    
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        future_cy = executor.submit(fetch_query, query_cy)
        future_py = executor.submit(fetch_query, query_py)
        future_acq = executor.submit(fetch_query, query_acq)
        
        df_cy = future_cy.result()
        df_py = future_py.result()
        df_acq = future_acq.result()
    
    # Combine all data
    active_df = pd.concat([df_cy, df_py, df_acq], ignore_index=True)
    conn.close()
    return active_df

@st.cache_data(show_spinner=False)
def fetch_data_population():
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
    active_data = fetch_active_data()
    active_data['Period'] = pd.to_datetime(active_data['Period'], errors='coerce')
    square_footage_data = fetch_square_footage_data()
    latest_ts = active_data["Period"].max()
    latest_ts = tabs.datefmt(latest_ts, "%B %Y") 
    placeholder.empty()
    # Store the database date range in session state for synchronization with filters
    if 'Period' in active_data.columns and len(active_data) > 0:
        st.session_state['database_date_range'] = (active_data['Period'].min(), active_data['Period'].max())

# Filter section
st.markdown("<div style='margin-top: 0px;'></div>", unsafe_allow_html=True)

# Render tab selector
selected_tab, top_col2, top_col3 = tabs.render_tab_selector(default_tab="Base Dashboard")

with top_col3:
    tabs.render_filter_controls()

with top_col2:
    tabs.render_reset_button(cookie_controller, auth_cookie, active_data)

# Handle different tabs
if selected_tab == "Base Dashboard":
    # Render filters using the filter manager and get filter values
    filter_values = filter_manager.render_all_filters(active_data)

    # Apply all filters to data
    filtered_data = filter_manager.apply_all_filters(active_data)
    # For Active page metrics, also calculate end-month-only data (filtered_Recent equivalent)
    if len(filtered_data) > 0 and 'Period' in filtered_data.columns:
        # Get the maximum period (end month) from filtered data
        max_period = filtered_data['Period'].max()
        if pd.notna(max_period):
            end_month = max_period.month
            end_year = max_period.year
            
            # Filter for only the end month data (for Total Active Stores metric)
            filtered_data_recent = filtered_data[
                (filtered_data['Period'].dt.month == end_month) &
                (filtered_data['Period'].dt.year == end_year)
            ]
            
            # Check if end month has data
            if len(filtered_data_recent) == 0:
                end_month_str = pd.Timestamp(year=end_year, month=end_month, day=1).strftime('%B %Y')  # type: ignore
                st.warning(f"⚠️ No data available for the last month ({end_month_str}). Please adjust your filters or select a broader date range.")
                filtered_data_recent = filtered_data  # Fallback to all filtered data
        else:
            filtered_data_recent = filtered_data
    else:
        filtered_data_recent = filtered_data
    
    # Calculate previous period data for comparison
    if len(filtered_data) > 0 and 'Period' in filtered_data.columns:
        current_min_period = filtered_data['Period'].min()
        # Get data from previous period (same range, shifted back by the period range)
        period_range = filtered_data['Period'].max() - filtered_data['Period'].min()
        previous_min = current_min_period - period_range
        previous_max = current_min_period
        previous_data = active_data[(active_data['Period'] >= previous_min) & (active_data['Period'] < previous_max)]
    else:
        previous_data = None

    # Render metrics using TabComponents
    styling.render_horizontal_line()
    tabs.render_metrics_section(filtered_data, metrics_title="Active", previous_data=previous_data, show_extended_metrics=True, is_active_page=True, filtered_data_recent=filtered_data_recent, square_footage_data=square_footage_data, chain_name_selected=filter_values["selected_chain_name"], parent_chain_name_selected=filter_values["parent_chain_name"],active_sq_footage=True)
    filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
    # Render charts using TabComponents
    tabs.render_standard_charts(filtered_data,filtered_data_recent, chart_title_prefix="Active")

    # Render data table using TabComponents
    tabs.render_data_table(filtered_data, table_title="Active Stores Table")

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
        active_data,
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
        filtered_data = compare_retailers_filter_manager.apply_all_filters(active_data, session_state_key_prefix="compare_retailers_")
        filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
        # Render charts and data table
        # Check if filtered_data is empty by checking its length
        is_empty = len(filtered_data) == 0
        if not is_empty and total_selected_retailers >= 2:
            # Render metrics (restoring as requested)
            styling.render_horizontal_line()
            tabs.render_metrics_section(filtered_data, metrics_title="Active", show_extended_metrics=False,active_sq_footage=False, square_footage_data=square_footage_data, chain_name_selected=filter_values["selected_chain_name"], parent_chain_name_selected=filter_values["parent_chain_name"])
            
            # Render comparison charts
            tabs.render_compare_retailers_charts(filtered_data,"", parent_chain_name, selected_chain_name, chart_title_prefix="Active")
            
            # Render data table
            tabs.render_data_table(filtered_data, table_title="Active Stores Table")
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
    
    # Render all filters with special conditions for Compare Sectors tab
    filter_values = compare_sectors_filter_manager.render_all_filters(
        active_data,
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
        filtered_data = compare_sectors_filter_manager.apply_all_filters(active_data, session_state_key_prefix="sector_compare_")
        filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
        # Render charts and data table
        # Check if filtered_data is empty by checking its length
        is_empty = len(filtered_data) == 0
        if not is_empty:
            # Don't render metrics for Compare Sectors tab
            
            # Render comparison charts
            tabs.render_compare_sectors_charts(filtered_data, sector_comparison_selected_sectors, chart_title_prefix="Active")
            
            # Render data table
            tabs.render_data_table(filtered_data, table_title="Active Stores Table")
        else:
            st.info("No data available for the selected filters")

ui.render_data_disclaimer(latest_ts)
include_html("footer.html")
