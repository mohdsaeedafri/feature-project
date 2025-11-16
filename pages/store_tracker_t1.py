import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder

# Database credentials
DB_NAME = "dwh_stg"
DB_USER = "dwh_app_access"
DB_PASS = "fai3agiPhooxo8Ja5aSha1ohx6a_"
DB_HOST = "csr-mysql8-flex-stg.mysql.database.azure.com"
DB_PORT = "3306"
SSL_CA  = os.getenv("SSL_CA")
# SSL_CA = "/Users/shashankgupta/Downloads/Coresight Research/SIP/2025/code_shashank/DigiCertGlobalRootCA.crt.pem"

@st.cache_data(ttl='1d', show_spinner=False)
def load_data():
    """
    Load explicit columns from store openings and closings.
    """
    conn_str = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    connect_args = {"ssl_ca": SSL_CA} if SSL_CA else {}
    engine = create_engine(conn_str, connect_args=connect_args)
    with engine.connect() as conn:
        closings_query = """
            SELECT
                id,
                upload_id,
                retailer_id,
                year,
                Week_Number,
                country,
                company,
                Sector,
                `Parent Company` AS parent_company,
                `Confirmed Closings` AS confirmed_closings,
                `Planned Closings`   AS planned_closings,
                `Total Store Closings`   AS total_store_closings,
                `Total Base Store Count` AS total_base_store_count,
                `Square Footage`        AS square_footage,
                Notes,
                `Location Type`         AS location_type
            FROM store_closings where year !=0 and Sector != '0' and country = 'US'
        """
        openings_query = """
            SELECT
                id,
                upload_id,
                retailer_id,
                year,
                Week_Number,
                country,
                company,
                Sector,
                `Parent Company` AS parent_company,
                `Confirmed Openings` AS confirmed_openings,
                `Planned Openings`   AS planned_openings,
                `Total Store Openings`   AS total_store_openings,
                `Total Base Store Count` AS total_base_store_count,
                `Square Footage`         AS square_footage,
                Notes,
                `Location Type`          AS location_type
            FROM store_openings where year !=0 and Sector != '0' and country = 'US'
        """
        closings = pd.read_sql(closings_query, conn)
        openings = pd.read_sql(openings_query, conn)
    return closings, openings



def main():
    st.set_page_config(page_title="Store Tracker", layout="wide")
    hide_streamlit_style = """
    <style>
    #MainMenu {visibility: hidden;}
    header {padding-top: 0rem;} /* Adjusts padding for the header */
    .css-1y0tads, .block-container {
        padding-top: 2.1rem !important;
    </style>

    """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True) 
    st.markdown(
        """
        <style>
          /* 1️⃣  Shrink the clickable header bar */
          div[data-testid="stExpander"] > details > summary {
            padding:     2px 8px !important;
            margin:      0       !important;
            min-height:  1.1rem  !important;
            line-height: 1rem    !important;
            display:     flex    !important;
            align-items: center  !important;
          }

          /* 2️⃣  Tighten the label text */
          div[data-testid="stExpander"] > details > summary span {
            font-size:   0.75rem !important;
            line-height: 1rem    !important;
          }

          /* 3️⃣  Shrink the ▼ chevron */
          div[data-testid="stExpander"] > details > summary svg {
            width:  0.75rem !important;
            height: 0.75rem !important;
            margin-left: 4px  !important;
          }

          /* Optional: remove extra gap below the header */
          div[data-testid="stExpander"] {
            margin-bottom: 0 !important;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Load data
    closings, openings = load_data()
# build a descending list of all years
    closings['year'] = closings['year'].astype(int)
    openings['year'] = openings['year'].astype(int)
    years = pd.concat([closings['year'], openings['year']])
    all_years = sorted(years.unique().tolist(), reverse=True)

    # if you want to default to the current year:
    current_year = datetime.now().year
    try:
        default_idx = all_years.index(current_year)
    except ValueError:
        # if current year isn’t in the data, fall back to newest (first) year
        default_idx = 0

    # Top filters as expanders
    c1, c2, c3 = st.columns([0.2,0.2,0.6])
    with c1:
        with st.expander("Year", expanded=False):
            selected_year = st.selectbox(
                "Select Year", 
                all_years, 
                index=default_idx, 
                key="year"
            )
    def normalize_sector(x):
        if pd.isna(x):
            return x
        s = x.strip()
        # unify “and” vs “&”
        s = s.replace(" and ", " & ")
        # collapse plurals
        if s.lower() in ("others",):
            s = "Other"
        return s

# apply to both dataframes
    for df in (closings, openings):
        df["Sector"] = df["Sector"].astype(str).apply(normalize_sector)
    with c2:
        with st.expander("Sector", expanded=False):
            sector_opts = ['All'] + sorted(
                pd.concat([closings['Sector'], openings['Sector']]).dropna().unique()
            )
            selected_sector = st.selectbox("Select Sector", sector_opts, key="sector")
    # with c3:
    #     with st.expander("Retailer", expanded=False):
    #         retailer_opts = ['All'] + sorted(
    #             pd.concat([closings['company'], openings['company']]).dropna().unique()
    #         )
    #         selected_retailer = st.selectbox("Select Retailer", retailer_opts, key="retailer")
    # with c3:
    #     with st.expander("Location", expanded=False):
    #         location_opts = ['All'] + sorted(
    #             pd.concat([closings['country'], openings['country']]).dropna().unique()
    #         )
    #         selected_location = st.selectbox("Select Location", location_opts, key="location")

    # Filter helper
    def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['year'] == selected_year]
        if selected_sector != 'All':
            df = df[df['Sector'] == selected_sector]
        # if selected_retailer != 'All':
        #     df = df[df['company'] == selected_retailer]
        # if selected_location != 'All':
        #     df = df[df['country'] == selected_location]
        return df

    # Apply filters
    filtered_openings = apply_filters(openings)
    filtered_closings = apply_filters(closings)

    filtered_closings['total_store_closings'] = (
    pd.to_numeric(filtered_closings['total_store_closings'], errors='coerce')
      .fillna(0)
      .astype(int)
    )
    filtered_openings['total_store_openings'] = (
        pd.to_numeric(filtered_openings['total_store_openings'], errors='coerce')
          .fillna(0)
          .astype(int)
    )

    open_cols = [
        'company','Sector','confirmed_openings','planned_openings',
        'total_store_openings','total_base_store_count',
        'square_footage','Notes'
    ]
    open_rename = {
        'company': 'Company',
        'Sector': 'Sector',
        'confirmed_openings': 'Confirmed Openings',
        'planned_openings': 'Planned Openings',
        'total_store_openings': 'Total Store Openings',
        'total_base_store_count': 'Total Store Base Count',
        'square_footage': 'Estimated Openings in Gross Square Feet (Thous.)',
        'Notes': 'Notes'
    }

    close_cols = [
        'company','Sector','confirmed_closings','planned_closings',
        'total_store_closings','total_base_store_count',
        'square_footage','Notes'
    ]
    close_rename = {
        'company': 'Company',
        'Sector': 'Sector',
        'confirmed_closings': 'Confirmed Closings',
        'planned_closings': 'Planned Closings',
        'total_store_closings': 'Total Store Closings',
        'total_base_store_count': 'Total Store Base Count',
        'square_footage': 'Estimated Closures in Gross Square Feet (Thous.)',
        'Notes': 'Notes'
    }

    year_label = f"{selected_year} YTD" if selected_year == current_year else str(selected_year)

    st.subheader(f"{year_label} Store Closings")

    df_close_disp = (
        filtered_closings[close_cols]
          .sort_values(by="total_store_closings", ascending=False)
          .head(50)
          .rename(columns=close_rename)
          .reset_index(drop=True)
    )

    gb = GridOptionsBuilder.from_dataframe(df_close_disp)
    gb.configure_default_column(
        enableSorting=True,
        enableFilter=True,
        resizable=True,
        wrapText=True,
        wrapHeaderText=True,
        autoHeight=True
    )
    grid_options = gb.build()
    grid_options["headerHeight"] = 60

    st.markdown(
        """
        <style>
          /* allow header text to break onto multiple lines */
          .ag-header-cell-label .ag-header-cell-text {
            white-space: normal !important;
            line-height: 1.2em !important;
          }
          /* ensure the header row expands */
          .ag-header {
            height: auto !important;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )
    AgGrid(
        df_close_disp,
        gridOptions=grid_options,
        height=430,
        fit_columns_on_grid_load=True
    )

    # st.subheader(f"{year_label} Store Openings")

    # df_open_disp = (
    # filtered_openings[open_cols]
    #     .sort_values(by='total_store_openings', ascending=False)
    #     .head(50)
    #     .rename(columns=open_rename)
    #     .reset_index(drop=True)
    # )
    # df_open_disp = df_open_disp.style.hide(axis="index")

    # st.dataframe(df_open_disp, use_container_width=True)


if __name__ == "__main__":
    main()
