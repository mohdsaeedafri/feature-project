"""
Refactored Net Openings Page - Using the new modular SIP components
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
page = BasePage(page_title="Net Openings")

# Render common page elements
page.render_header()
page.render_navigation_buttons(changelog_return_page="net")

# Apply consistent styling
styling = StylingManager()
styling.apply_global_styles()
styling.hide_streamlit_elements()
styling.apply_bootstrap()

# Initialize other components
ui = UIComponents()
db = DatabaseManager()
filter_manager = FilterManager(page.auth_cookie, page_prefix="net_")
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
tabs.render_tab_navigation(current_page="net")

# Function to fetch actual data from database
@st.cache_data(show_spinner=False)
def fetch_separate_data():
    """Fetch both opened and closed data from database"""
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

    conn = None
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

        # Convert 'Period' to datetime format (keep as datetime, not date)
        opened_df['Period'] = pd.to_datetime(opened_df['Period'], errors='coerce')
        closed_df['Period'] = pd.to_datetime(closed_df['Period'], errors='coerce')

        return opened_df, closed_df

    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()

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
    parent_chain_df['Average_Square_Footage'] = pd.to_numeric(parent_chain_df['Average_Square_Footage'], errors='coerce')
    parent_chain_df['Average_Square_Footage'] = parent_chain_df['Average_Square_Footage'] * 1000.0
    
    conn.close()
    return parent_chain_df
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
    opened_df, closed_df = fetch_separate_data()
    latest_ts = opened_df["Period"].max()
    latest_ts = latest_ts.strftime("%B %Y")
    square_footage_data = fetch_square_footage_data()
    placeholder.empty()
# Filter section
st.markdown("<div style='margin-top: 0px;'></div>", unsafe_allow_html=True)

# Render tab selector
selected_tab, top_col2, top_col3 = tabs.render_tab_selector(default_tab="Base Dashboard")

with top_col3:
    tabs.render_filter_controls()

combined_data = pd.concat([opened_df, closed_df])
with top_col2:
    tabs.render_reset_button(cookie_controller, auth_cookie, combined_data)

# Handle different tabs
if selected_tab == "Base Dashboard":
    # Render filters using the filter manager and get filter values
    filter_values = filter_manager.render_all_filters(combined_data)

    # Apply all filters to both opened and closed data separately
    # IMPORTANT: Apply sector filtering first, then transform NULL ParentName, then apply remaining filters
    opened_filtered = filter_manager.apply_all_filters(opened_df)
    closed_filtered = filter_manager.apply_all_filters(closed_df)
    
    # Apply "No Parent Retailer" transformation to filtered data for consistent retailer filtering
    opened_filtered['ParentName_Coresight'] = opened_filtered['ParentName_Coresight'].where(
        pd.notna(opened_filtered['ParentName_Coresight']), "No Parent Retailer"
    )
    closed_filtered['ParentName_Coresight'] = closed_filtered['ParentName_Coresight'].where(
        pd.notna(closed_filtered['ParentName_Coresight']), "No Parent Retailer"
    )
    
    # Combine filtered data with data_from column
    opened_filtered['data_from'] = 'opened'
    closed_filtered['data_from'] = 'closed'
    filtered_data = pd.concat([opened_filtered, closed_filtered])
    filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
    
    # For previous period - we need to manually filter since filter_manager uses session state dates
    # Get current period bounds from session state, not from filtered data
    if len(filtered_data) > 0 and (len(opened_filtered) > 0 or len(closed_filtered) > 0):
        # Get the original date range from session state
        selected_date_range = st.session_state.get("selected_date_range", (opened_df['Period'].min(), opened_df['Period'].max()))
        current_min, current_max = selected_date_range
        
        # Calculate period length correctly
        from dateutil.relativedelta import relativedelta
        num_months = (current_max.year - current_min.year) * 12 + (current_max.month - current_min.month) + 1
        
        # Previous period - Fix the date calculation
        # The previous period should end just before the current period starts
        prev_end = current_min - pd.DateOffset(days=1)
        # The previous period should start num_months before prev_end
        prev_start = prev_end - relativedelta(months=num_months-1)
        
        
        # Check if dates are in correct order
        if prev_start > prev_end:       
            # but shifted back in time
            prev_end = current_min - pd.DateOffset(days=1)
            prev_start = current_min - pd.DateOffset(days=1) - relativedelta(months=num_months-1)
        
        # Get ALL filters - IMPORTANT: Get sector from user_filters directly, not from filter_values
        sector_selection = filter_manager.user_filters.get("selected_sector_name", "All")
        parent_chain_name = filter_manager.user_filters.get("parent_chain_name", ["All"])
        selected_chain_name = filter_manager.user_filters.get("selected_chain_name", ["All"])
        selected_state_name = filter_manager.user_filters.get("selected_state_name", ["All"])
        selected_msa_name = filter_manager.user_filters.get("selected_msa_name", ["All"])
        selected_postal_codes = filter_manager.user_filters.get("selected_zip_code", ["All"])

        
        # Start with date-filtered previous period data
        # IMPORTANT: Apply same "No Parent Retailer" transformation to match original net.py
        prev_opened = opened_df[(opened_df['Period'] >= prev_start) & (opened_df['Period'] <= prev_end)]
        
        prev_closed = closed_df[(closed_df['Period'] >= prev_start) & (closed_df['Period'] <= prev_end)]
        
        # Ensure we're working with pandas DataFrames
        if not isinstance(prev_opened, pd.DataFrame):
            prev_opened = pd.DataFrame(prev_opened)
        if not isinstance(prev_closed, pd.DataFrame):
            prev_closed = pd.DataFrame(prev_closed)
        
        # Apply sector (handle both single value and "All")
        if sector_selection and sector_selection != "All":
            prev_opened = prev_opened[prev_opened["Sector_Coresight"] == sector_selection]
            prev_closed = prev_closed[prev_closed["Sector_Coresight"] == sector_selection]
        
        # Apply parent (check if it's a list and handle "All") - MATCH ORIGINAL net.py logic
        if  "All" not in parent_chain_name :
            # Use numpy isin for compatibility
            import numpy as np
            mask_opened = np.isin(prev_opened["ParentName_Coresight"], parent_chain_name)
            mask_closed = np.isin(prev_closed["ParentName_Coresight"], parent_chain_name)
            prev_opened = prev_opened[mask_opened]
            prev_closed = prev_closed[mask_closed]
        
        # Apply chain (check if it's a list and handle "All")
        if isinstance(selected_chain_name, list) and "All" not in selected_chain_name and selected_chain_name:
            # Use numpy isin for compatibility
            import numpy as np
            mask_opened = np.isin(prev_opened["ChainName_Coresight"], selected_chain_name)
            mask_closed = np.isin(prev_closed["ChainName_Coresight"], selected_chain_name)
            prev_opened = prev_opened[mask_opened]
            prev_closed = prev_closed[mask_closed]
        
        # Apply state (check if it's a list and handle "All")
        if isinstance(selected_state_name, list) and "All" not in selected_state_name:
            # Use numpy isin for compatibility
            import numpy as np
            mask_opened = np.isin(prev_opened["State"], selected_state_name)
            mask_closed = np.isin(prev_closed["State"], selected_state_name)
            prev_opened = prev_opened[mask_opened]
            prev_closed = prev_closed[mask_closed]
        
        # Apply MSA (check if it's a list and handle "All")
        if isinstance(selected_msa_name, list) and "All" not in selected_msa_name:
            # Use numpy isin for compatibility
            import numpy as np
            mask_opened = np.isin(prev_opened["MsaName"], selected_msa_name)
            mask_closed = np.isin(prev_closed["MsaName"], selected_msa_name)
            prev_opened = prev_opened[mask_opened]
            prev_closed = prev_closed[mask_closed]
        
        # Apply postal (check if it's a list and handle "All")
        if isinstance(selected_postal_codes, list) and "All" not in selected_postal_codes:
            # Use numpy isin for compatibility
            import numpy as np
            mask_opened = np.isin(prev_opened["PostalCode"].astype(str), [str(p) for p in selected_postal_codes])
            mask_closed = np.isin(prev_closed["PostalCode"].astype(str), [str(p) for p in selected_postal_codes])
            prev_opened = prev_opened[mask_opened]
            prev_closed = prev_closed[mask_closed]
        
        prev_net_openings = len(prev_opened) - len(prev_closed)
    else:
        prev_net_openings = 0

    # Calculate net-specific metrics manually
    opened_count = opened_filtered['ChainName_Coresight'].count()
    closed_count = closed_filtered['ChainName_Coresight'].count()
    net_openings = opened_count - closed_count
    total_retailers = filtered_data['ParentName_Coresight'].nunique()
    total_sectors = filtered_data['Sector_Coresight'].nunique()
    
    # Calculate previous period net using the manually filtered prev data
    if prev_net_openings != 0:
        percent_change = ((net_openings - prev_net_openings) / abs(prev_net_openings)) * 100
        percent_display = f"{percent_change:.1f}%"
    else:
        percent_display = "N/A"
    
    # Calculate stores per 10k people - IMPORTANT: Use filtered_data (combined) for population
    total_population = filtered_data['Population'].sum() if 'Population' in filtered_data.columns else 0
    if total_population > 0:
        stores_per_10k = (opened_count / total_population) * 10000
        stores_per_10k_display = f"{stores_per_10k:.3f}"
    else:
        stores_per_10k_display = "N/A"
    
    # Render metrics
    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
    ui.render_metric_card("Net Opened Stores", net_openings, column=col1)
    ui.render_metric_card("Total Affected Banners", total_retailers, column=col2)
    ui.render_metric_card("Total Affected Sectors", total_sectors, column=col3)
    ui.render_metric_card("Previous Period Net Openings", prev_net_openings, column=col4)
    ui.render_metric_card("% Change vs Previous Period", percent_display, column=col5)
    ui.render_metric_card("Opened Stores per 10,000 people", stores_per_10k_display, column=col6)
    
    formatted_value= tabs.render_metrics_section(filtered_data, metrics_title="Net", show_extended_metrics=False, square_footage_data=square_footage_data, chain_name_selected=filter_values["selected_chain_name"], parent_chain_name_selected=filter_values["parent_chain_name"])
    ui.render_metric_card("Net Square Footage", formatted_value, column=col7)

    
    # Render horizontal line
    styling.render_horizontal_line()
    
    # Render charts
    col1, col2 = st.columns([0.7, 0.3])
    
    with col1:
        # Monthly chart
        chart_data = filtered_data.copy()
        chart_data['year_month'] = chart_data['Period'].dt.to_period('M').astype(str)
        chart_data['opened'] = (chart_data['data_from'] == 'opened').astype(int)
        chart_data['closed'] = (chart_data['data_from'] == 'closed').astype(int)
        
        month_agg = chart_data.groupby('year_month').agg(
            Openings=('opened', 'sum'),
            Closures=('closed', 'sum')
        ).reset_index()
        month_agg['Net Openings'] = month_agg['Openings'] - month_agg['Closures']
        month_agg['year_month_dt'] = pd.to_datetime(month_agg['year_month'], format='%Y-%m')
        month_agg['year_month_label'] = month_agg['year_month_dt'].dt.strftime('%b %Y')
        
        df_plot = month_agg.copy()
        df_plot['Closures_plot'] = -df_plot['Closures']
        x_vals = df_plot['year_month_label']
        
        fig = go.Figure()
        fig.add_trace(go.Bar(x=x_vals, y=df_plot['Closures_plot'], name="Closures", marker_color="#d62e2f",
                             text=[f"{-v:,}" for v in df_plot['Closures_plot']], textposition="outside"))
        fig.add_trace(go.Bar(x=x_vals, y=df_plot['Openings'], name="Openings", marker_color="#A3C0CE",
                             text=[f"{v:,}" for v in df_plot['Openings']], textposition="outside"))
        fig.add_trace(go.Scatter(x=x_vals, y=df_plot['Net Openings'], name="Net Store Openings", 
                                mode='lines+markers+text', line=dict(color='#2D2A29', width=2),
                                marker=dict(size=7), text=[f"{v:,}" for v in df_plot['Net Openings']], 
                                textposition="top center"))
        
        fig.update_layout(barmode='relative', yaxis_title="Number of Stores", height=500, 
                         plot_bgcolor="white", hovermode="x unified")
        st.markdown("<h4 style='font-size: 20px;text-align: center;'>Monthly Store Openings & Closures with Net Store Openings</h4>", unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top banners chart
        # Convert to pandas Series if needed for value_counts
        opened_data = filtered_data[filtered_data['data_from']=='opened']['ChainName_Coresight']
        closed_data = filtered_data[filtered_data['data_from']=='closed']['ChainName_Coresight']
        
        # Ensure we're working with pandas Series for value_counts
        if not isinstance(opened_data, pd.Series):
            opened_data = pd.Series(opened_data)
        if not isinstance(closed_data, pd.Series):
            closed_data = pd.Series(closed_data)
            
        banner_agg_opened = opened_data.value_counts()
        banner_agg_closed = closed_data.value_counts()
        banner_net = banner_agg_opened.subtract(banner_agg_closed, fill_value=0)
        top_banners = banner_net.sort_values(ascending=False).head(15)
        
        if not top_banners.empty:
            fig_banner = go.Figure()
            fig_banner.add_trace(go.Bar(x=top_banners.index, y=top_banners.values, marker_color='#2D2A29',
                                       text=[f"{x:,}" for x in top_banners.values], textposition='outside'))
            fig_banner.update_layout(xaxis_title="Banner Name", yaxis_title="Net Openings", height=500)
            st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {min(15, len(top_banners))} Banners Net Openings</h4>", unsafe_allow_html=True)
            st.plotly_chart(fig_banner, use_container_width=True)
    # Render Standard Charts with net data (opened - closed)
    # The UIComponents will handle the net calculation based on the data_from column
    tabs.render_standard_charts(filtered_data, chart_title_prefix="Net")

    # Render data table
    # tabs.render_data_table(filtered_data, table_title="Net Stores (Opened & Closed)")

elif selected_tab == "Compare Retailers":
    compare_filter_manager = FilterManager(page.auth_cookie)
    compare_filter_manager.user_filters = user_filters.copy()
    
    def retailers_location_enable_conditions(parent_chain_name, selected_chain_name, sector_selection):
        return len(parent_chain_name) > 0 or len(selected_chain_name) > 0
    
    filter_values = compare_filter_manager.render_all_filters(
        combined_data, session_state_key_prefix="compare_retailers_",
        location_enable_conditions=retailers_location_enable_conditions
    )
    
    if len(filter_values["parent_chain_name"]) + len(filter_values["selected_chain_name"]) < 2:
        st.info("Please select at least two retailers to start comparison")
    else:
        # Apply same "No Parent Retailer" transformation for consistency
        opened_df_copy = opened_df.copy()
        closed_df_copy = closed_df.copy()
        opened_df_copy['ParentName_Coresight'] = opened_df_copy['ParentName_Coresight'].where(
            pd.notna(opened_df_copy['ParentName_Coresight']), "No Parent Retailer"
        )
        closed_df_copy['ParentName_Coresight'] = closed_df_copy['ParentName_Coresight'].where(
            pd.notna(closed_df_copy['ParentName_Coresight']), "No Parent Retailer"
        )
        opened_filtered = compare_filter_manager.apply_all_filters(opened_df_copy, session_state_key_prefix="compare_retailers_")
        closed_filtered = compare_filter_manager.apply_all_filters(closed_df_copy, session_state_key_prefix="compare_retailers_")
        
        opened_filtered['data_from'] = 'opened'
        closed_filtered['data_from'] = 'closed'
        filtered_data = pd.concat([opened_filtered, closed_filtered])
        filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
        if len(filtered_data) > 0:
            col1, col2, col3 = st.columns(3)
            ui.render_metric_card("Net Opened Stores", len(opened_filtered) - len(closed_filtered), column=col1)
            ui.render_metric_card("Total Affected Banners", filtered_data['ChainName_Coresight'].nunique(), column=col2)
            ui.render_metric_card("Total Affected Sectors", filtered_data['Sector_Coresight'].nunique(), column=col3)
            
            tabs.render_compare_retailers_charts(opened_filtered,filtered_data, filter_values["parent_chain_name"],filter_values["selected_chain_name"], chart_title_prefix="Net")
                                                
            tabs.render_data_table(filtered_data, table_title="Net Stores Table")

elif selected_tab == "Compare Sectors":
    compare_filter_manager = FilterManager(page.auth_cookie)
    compare_filter_manager.user_filters = user_filters.copy()
    
    def sectors_location_enable_conditions(parent_chain_name, selected_chain_name, sector_selection):
        return sector_selection and len(sector_selection) >= 2
    
    filter_values = compare_filter_manager.render_all_filters(
        combined_data, session_state_key_prefix="sector_compare_", use_sector_comparison=True,
        retailer_filter_disabled=True, location_enable_conditions=sectors_location_enable_conditions
    )
    
    if len(filter_values["sector_selection"]) < 2:
        st.info("Please select at least two sectors to start comparison")
    else:
        # Apply same "No Parent Retailer" transformation for consistency
        opened_df_copy = opened_df.copy()
        closed_df_copy = closed_df.copy()
        opened_df_copy['ParentName_Coresight'] = opened_df_copy['ParentName_Coresight'].where(
            pd.notna(opened_df_copy['ParentName_Coresight']), "No Parent Retailer"
        )
        closed_df_copy['ParentName_Coresight'] = closed_df_copy['ParentName_Coresight'].where(
            pd.notna(closed_df_copy['ParentName_Coresight']), "No Parent Retailer"
        )
        opened_filtered = compare_filter_manager.apply_all_filters(opened_df_copy, session_state_key_prefix="sector_compare_")
        closed_filtered = compare_filter_manager.apply_all_filters(closed_df_copy, session_state_key_prefix="sector_compare_")
        
        opened_filtered['data_from'] = 'opened'
        closed_filtered['data_from'] = 'closed'
        filtered_data = pd.concat([opened_filtered, closed_filtered])
        filtered_data = tabs.add_square_footage_column(filtered_data, square_footage_data)
        if len(filtered_data) > 0:
            tabs.render_compare_sectors_charts(filtered_data, filter_values["sector_selection"], chart_title_prefix="Net")
            # tabs.render_data_table(filtered_data, table_title="Net Stores - Compare Sectors")
ui.render_data_disclaimer(latest_ts)
include_html("footer.html")
