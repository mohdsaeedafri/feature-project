import os
import logging
import pandas as pd
import streamlit as st
import mysql.connector
from dotenv import load_dotenv

# Helpers expected in your repo
from streamlit_cookies_controller import CookieController
from html_utils import include_html

# AgGrid
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

load_dotenv()
st.set_page_config(
    page_title="Store Intelligence Platform Retailers List",
    layout="wide",
    page_icon="https://coresight.com/wp-content/uploads/2019/03/cropped-CoreSightTransparent_Logo_favico-32x32.png",
)

st.markdown(
    """
    <style>
      header[data-testid="stHeader"], div[data-testid="stHeader"], div[role="banner"] { display: none !important; }
      #MainMenu, div[data-testid="stToolbar"], footer { visibility: hidden !important; }
      html, body, [data-testid="stApp"], [data-testid="stAppViewContainer"], .block-container { margin: 0 !important; padding: 0 !important; }
      section[data-testid="stSidebar"], [data-testid="stSidebarCollapsedControl"] { display: none !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

LOG_LEVEL_STR = os.getenv("LOG_LEVEL") or "INFO"
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL_STR, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
)

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
SSL_CA = os.getenv("SSL_CA")  # optional path to ca.pem
ENABLE_SSL = (os.getenv("ENABLE_SSL", "false").lower() == "true")

# st.markdown(
#     """
#     <style>
#     :root { --header-h: 85px; --footer-h: 60px; }
#     header[data-testid="stHeader"] { display: none !important; }
#     .block-container {
#         padding-top: var(--header-h) !important;
#         padding-bottom: var(--footer-h) !important;
#         max-width: 1300px !important;
#         margin-left: auto !important;
#         margin-right: auto !important;
#     }
#     .stApp { overflow-x: hidden; background: #fff !important; }
#     </style>
#     """,
#     unsafe_allow_html=True,
# )

cookie_controller = CookieController()

# include_html("header.html")

@st.cache_data(show_spinner=False)
def fetch_retailers() -> pd.DataFrame:
    params = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "host": DB_HOST,
        "database": DB_NAME,
    }
    if ENABLE_SSL and SSL_CA:
        params["ssl_ca"] = SSL_CA
        logging.info("Using SSL for MySQL connection")

    conn = mysql.connector.connect(**params)
    try:
        q = "SELECT * FROM parent_chain_names_data where is_active = 1"
        df = pd.read_sql(q, conn)
        # Normalize key columns
        for c in ["Sector_Coresight", "ParentName_Coresight", "ChainName_Coresight", "UpdateCycle_ChainXY","Chain_ID"]:
            if c in df.columns:
                df[c] = df[c].fillna("").astype(str).str.strip()
        return df
    finally:
        conn.close()

df = fetch_retailers()
if df.empty:
    st.warning("No retailers found.")
    include_html("footer.html")
    st.stop()

if "selected_sector" not in st.session_state:
    st.session_state["selected_sector"] = "All"
if "selected_parents" not in st.session_state:
    st.session_state["selected_parents"] = ["All"]
if "selected_chains" not in st.session_state:
    st.session_state["selected_chains"] = ["All"]

def normalize_parents():
    sel = st.session_state.get("selected_parents", [])
    if not sel:
        st.session_state["selected_parents"] = ["All"]
        return
    if "All" in sel and len(sel) > 1:
        if sel[-1] == "All":
            st.session_state["selected_parents"] = ["All"]
        else:
            st.session_state["selected_parents"] = [p for p in sel if p != "All"]

def normalize_chains():
    sel = st.session_state.get("selected_chains", [])
    if not sel:
        st.session_state["selected_chains"] = ["All"]
        return
    if "All" in sel and len(sel) > 1:
        if sel[-1] == "All":
            st.session_state["selected_chains"] = ["All"]
        else:
            st.session_state["selected_chains"] = [c for c in sel if c != "All"]

st.subheader("Store Intelligence Platform Retailers List")
st.html("<style>[data-testid='stHeaderActionElements'] {display: none;}</style>")

# Build sector options: All + alphabetical, with "Other(s)" at the end
all_sectors = sorted([s for s in df["Sector_Coresight"].unique() if s], key=str.lower)
others = [s for s in all_sectors if s.lower() in ("other", "others")]
regular = [s for s in all_sectors if s.lower() not in ("other", "others")]
sector_options = ["All"] + regular + others

c1, c2, c3 = st.columns([1, 1, 1])

with c3:
    st.selectbox(
        "Select Sector",
        sector_options,
        key="selected_sector",
    )

selected_sector = st.session_state["selected_sector"]
df_sector = df if selected_sector == "All" else df[df["Sector_Coresight"] == selected_sector]

parent_options = ["All"] + sorted([p for p in df_sector["ParentName_Coresight"].unique() if p], key=str)

# PRE-SANITIZE PARENT STATE before widget (important when options change)
_parent_state = st.session_state.get("selected_parents", ["All"])
_parent_clean = [p for p in _parent_state if p in parent_options]
if not _parent_clean:
    _parent_clean = ["All"] if "All" in parent_options else ([parent_options[0]] if parent_options else [])
st.session_state["selected_parents"] = _parent_clean

with c2:
    st.multiselect(
        "Select Company",
        parent_options,
        key="selected_parents",
        on_change=normalize_parents,
    )

parents_for_filter = st.session_state["selected_parents"]
df_parent = df_sector if parents_for_filter == ["All"] else df_sector[df_sector["ParentName_Coresight"].isin(parents_for_filter)]

chain_options = ["All"] + sorted([c for c in df_parent["ChainName_Coresight"].unique() if c], key=str)

_chain_state = st.session_state.get("selected_chains", ["All"])
_chain_clean = [c for c in _chain_state if c in chain_options]
if not _chain_clean:
    _chain_clean = ["All"] if "All" in chain_options else ([chain_options[0]] if chain_options else [])
st.session_state["selected_chains"] = _chain_clean

with c1:
    st.multiselect(
        "Select Banner/Brand",
        chain_options,
        key="selected_chains",
        on_change=normalize_chains,
    )

chains_for_filter = st.session_state["selected_chains"]
df_final = df_parent if chains_for_filter == ["All"] else df_parent[df_parent["ChainName_Coresight"].isin(chains_for_filter)]
df_final['Chain_ID'] = pd.to_numeric(df_final['Chain_ID'], errors='coerce')  

rename_map = {
    'Chain_ID': 'Retailer ID',
    'ChainName_Coresight': 'Banner/Brand Name',
    'ParentName_Coresight': 'Company Name',
    'Sector_Coresight': 'Sector',
    'UpdateCycle_ChainXY': 'Update Frequency (in days)*',
}

df_named = df_final.rename(columns=rename_map)

display_cols = list(rename_map.values())
show_cols = [c for c in display_cols if c in df_named.columns]

df_show = df_named[show_cols].copy()
sort_cols = [c for c in show_cols if c != 'Update Frequency (in days)*']
if sort_cols:
    df_show = df_show.sort_values(sort_cols).reset_index(drop=True)
else:
    df_show = df_show.reset_index(drop=True)

st.caption(f"Retailers Selected: {len(df_show)}")

gb = GridOptionsBuilder.from_dataframe(df_show)

# No auto fitting, keep it resizable=False if you want the user not to change widths
gb.configure_default_column(
    sortable=True,
    filter=False,
    floatingFilter=False,
    resizable=False, 
)

# Helper to lock a column width exactly
def lock_col(col, w):
    gb.configure_column(
        col,
        width=w,
        minWidth=w,
        maxWidth=w,
        suppressSizeToFit=True,
        flex=0,
        filter=False, 
        floatingFilter=False
    )

lock_col("Retailer ID", 100)
lock_col("Sector", 300)
lock_col("Company Name", 280)
lock_col("Banner/Brand Name", 280)
lock_col("Update Frequency (in days)*", 230)

gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=100)
gb.configure_grid_options(animateRows=True, sideBar=False)

grid_options = gb.build()

# Extra safety: ensure no columnDefs have 'flex'
for coldef in grid_options.get("columnDefs", []):
    coldef["flex"] = 0
    coldef["suppressSizeToFit"] = True

AgGrid(
    df_show,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.NO_UPDATE,
    data_return_mode="AS_INPUT",
    fit_columns_on_grid_load=False,   # <= do NOT call sizeColumnsToFit
    theme="balham",
    enable_enterprise_modules=False,
    allow_unsafe_jscode=False,
)

st.markdown(f"<i>*This is how often (in days) each retailer’s data is typically updated</i>",unsafe_allow_html=True)


# include_html("footer.html")
