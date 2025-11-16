import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
from datetime import datetime
import time
import logging
import os

# if "authorized" not in st.session_state:
#     st.session_state.authorized = False
# EXPECTED = (
#     st.secrets.get("STORE_TRACKER_TOKEN")
#     or os.getenv("STORE_TRACKER_TOKEN")
# )

# # If they’re not yet authorized, prompt
# if not st.session_state.authorized:
#     entered = st.text_input(
#         "Enter access token to view this page", 
#         type="password", 
#         key="auth_input"
#     )

#     if entered:
#         if entered == EXPECTED:
#             st.session_state.authorized = True
#             st.rerun() 
#         else:
#             st.error("Invalid token, please try again.")
#     st.stop()

# DB_HOST = os.getenv("DB_HOST")
# DB_NAME = os.getenv("DB_NAME")
# DB_USER = os.getenv("DB_USER")
# DB_PASS = os.getenv("DB_PASSWORD")
SSL_CA = os.getenv("SSL_CA")

# Database credentials
DB_NAME = "dwh_stg"
DB_USER = "dwh_app_access"
DB_PASS = "fai3agiPhooxo8Ja5aSha1ohx6a_"
DB_HOST = "csr-mysql8-flex-stg.mysql.database.azure.com"
DB_PORT = "3306"
DB_SCHEMA = "dwh_stg"
# SSL_CA = "/Users/shashankgupta/Downloads/Coresight Research/SIP/2025/code_shashank/DigiCertGlobalRootCA.crt.pem"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)

@st.cache_data(ttl = "1d", show_spinner=False)
def fetch_store_data():
    
    closing_query = """
    SELECT 
        sc.id,
        sc.upload_id,
        sc.retailer_id,
        sc.year,
        sc.Week_Number,
        sc.country,
        sc.company,
        sc.Sector,
        sc.`Parent Company` AS parent_company,
        sc.`1` AS week_1,
        sc.`2` AS week_2,
        sc.`3` AS week_3,
        sc.`4` AS week_4,
        sc.`5` AS week_5,
        sc.`6` AS week_6,
        sc.`7` AS week_7,
        sc.`8` AS week_8,
        sc.`9` AS week_9,
        sc.`10` AS week_10,
        sc.`11` AS week_11,
        sc.`12` AS week_12,
        sc.`13` AS week_13,
        sc.`14` AS week_14,
        sc.`15` AS week_15,
        sc.`16` AS week_16,
        sc.`17` AS week_17,
        sc.`18` AS week_18,
        sc.`19` AS week_19,
        sc.`20` AS week_20,
        sc.`21` AS week_21,
        sc.`22` AS week_22,
        sc.`23` AS week_23,
        sc.`24` AS week_24,
        sc.`25` AS week_25,
        sc.`26` AS week_26,
        sc.`27` AS week_27,
        sc.`28` AS week_28,
        sc.`29` AS week_29,
        sc.`30` AS week_30,
        sc.`31` AS week_31,
        sc.`32` AS week_32,
        sc.`33` AS week_33,
        sc.`34` AS week_34,
        sc.`35` AS week_35,
        sc.`36` AS week_36,
        sc.`37` AS week_37,
        sc.`38` AS week_38,
        sc.`39` AS week_39,
        sc.`40` AS week_40,
        sc.`41` AS week_41,
        sc.`42` AS week_42,
        sc.`43` AS week_43,
        sc.`44` AS week_44,
        sc.`45` AS week_45,
        sc.`46` AS week_46,
        sc.`47` AS week_47,
        sc.`48` AS week_48,
        sc.`49` AS week_49,
        sc.`50` AS week_50,
        sc.`51` AS week_51,
        sc.`52` AS week_52,
        sc.`53` AS week_53
    FROM 
        store_closings sc
    ORDER BY 
        sc.year, sc.Week_Number, sc.company;
    """
    
    opening_query = """
    SELECT 
        so.id,
        so.upload_id,
        so.retailer_id,
        so.year,
        so.Week_Number,
        so.country,
        so.company,
        so.Sector,
        so.`Parent Company` AS parent_company,
        so.`1` AS week_1,
        so.`2` AS week_2,
        so.`3` AS week_3,
        so.`4` AS week_4,
        so.`5` AS week_5,
        so.`6` AS week_6,
        so.`7` AS week_7,
        so.`8` AS week_8,
        so.`9` AS week_9,
        so.`10` AS week_10,
        so.`11` AS week_11,
        so.`12` AS week_12,
        so.`13` AS week_13,
        so.`14` AS week_14,
        so.`15` AS week_15,
        so.`16` AS week_16,
        so.`17` AS week_17,
        so.`18` AS week_18,
        so.`19` AS week_19,
        so.`20` AS week_20,
        so.`21` AS week_21,
        so.`22` AS week_22,
        so.`23` AS week_23,
        so.`24` AS week_24,
        so.`25` AS week_25,
        so.`26` AS week_26,
        so.`27` AS week_27,
        so.`28` AS week_28,
        so.`29` AS week_29,
        so.`30` AS week_30,
        so.`31` AS week_31,
        so.`32` AS week_32,
        so.`33` AS week_33,
        so.`34` AS week_34,
        so.`35` AS week_35,
        so.`36` AS week_36,
        so.`37` AS week_37,
        so.`38` AS week_38,
        so.`39` AS week_39,
        so.`40` AS week_40,
        so.`41` AS week_41,
        so.`42` AS week_42,
        so.`43` AS week_43,
        so.`44` AS week_44,
        so.`45` AS week_45,
        so.`46` AS week_46,
        so.`47` AS week_47,
        so.`48` AS week_48,
        so.`49` AS week_49,
        so.`50` AS week_50,
        so.`51` AS week_51,
        so.`52` AS week_52,
        so.`53` AS week_53
    FROM 
        store_openings so
    ORDER BY 
        so.year, so.Week_Number, so.company;
    """
    
    import time

    max_retries = 5000  # You can adjust this number or set it to None for infinite retries
    retry_delay = 1  # seconds between retries
    retry_count = 0

    while True:
        try:
            engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}",
            connect_args={"ssl_ca": SSL_CA})

            with engine.connect() as conn:
                closings_df = pd.read_sql(closing_query, conn)
                openings_df = pd.read_sql(opening_query, conn)
                closings_df['year'] = closings_df['year'].astype(int)
                openings_df['year'] = openings_df['year'].astype(int)
                logger.info(f"Successfully loaded {len(closings_df)} closures and {len(openings_df)} openings")
                return closings_df, openings_df
        
        except Exception as e:
            logger.error(f"{e}")
            retry_count += 1
            if max_retries is not None and retry_count >= max_retries:
                logger.error(f"Max retries ({max_retries}) reached. Giving up.")
                raise
            
            time.sleep(retry_delay)

def apply_filters(df, filters):
    """Apply filters to the dataset with logging"""
    logger.info("Applying filters to dataset")
    df['year'] = df['year'].astype(int)

    # Date range filter
    mask = (
        (df['year'] > filters["start_year"]) |
        ((df['year'] == filters["start_year"]) & (df['Week_Number'] >= filters["start_week"]))
    ) & (
        (df['year'] < filters["end_year"]) |
        ((df['year'] == filters["end_year"]) & (df['Week_Number'] <= filters["end_week"]))
    )
    filtered = df.loc[mask]
    logger.info(f"After date filtering: {len(filtered)} records")

    # Sector filter
    if filters["sector"] != "All":
        filtered = filtered[filtered['Sector'] == filters["sector"]]
        logger.info(f"After sector filtering: {len(filtered)} records")

    # Parent companies filter
    if filters["parent_companies"] and not (len(filters["parent_companies"]) == 1 and filters["parent_companies"][0] == "All"):
        filtered = filtered[filtered['parent_company'].isin(filters["parent_companies"])]
        logger.info(f"After parent companies filtering: {len(filtered)} records")

    # Companies filter
    if filters["companies"] and not (len(filters["companies"]) == 1 and filters["companies"][0] == "All"):
        filtered = filtered[filtered['company'].isin(filters["companies"])]
        logger.info(f"After companies filtering: {len(filtered)} records")

    # Country filter
    if filters["country"] != "All":
        filtered = filtered[filtered['country'] == filters["country"]]
        logger.info(f"After country filtering: {len(filtered)} records")

    return filtered

def get_filter_options(closings_df, openings_df):
    """Get all distinct filter values from the combined data"""
    logger.info("Generating filter options from dataset")
    
    # Combine both dataframes for filter options
    combined_df = pd.concat([closings_df, openings_df])
    combined_df['year'] = combined_df['year'].astype(int)
    
    return {
        'years': sorted(combined_df['year'].unique().tolist()),
        'sectors': ["All"] + sorted(combined_df['Sector'].dropna().unique().tolist()),
        'parent_companies': ["All"] + sorted(combined_df['parent_company'].dropna().unique().tolist()),
        'companies': ["All"] + sorted(combined_df['company'].dropna().unique().tolist()),
        'countries': ["All"] + sorted(combined_df['country'].dropna().unique().tolist()),
    }

def get_dynamic_filter_options(df, current_filters):
    """Get filter options based on current selections"""
    options = {
        'sectors': ["All"],
        'parent_companies': ["All"],
        'companies': ["All"],
        'countries': ["All"]
    }
    
    # Apply existing filters to get subset of data
    filtered = df.copy()
    
    # Date range filter
    if current_filters.get("start_year"):
        mask = (
            (filtered['year'] > current_filters["start_year"]) |
            ((filtered['year'] == current_filters["start_year"]) & 
            (filtered['Week_Number'] >= current_filters.get("start_week", 1)))
        ) & (
            (filtered['year'] < current_filters["end_year"]) |
            ((filtered['year'] == current_filters["end_year"]) & 
            (filtered['Week_Number'] <= current_filters.get("end_week", 53)))
        )
        filtered = filtered.loc[mask]
    
    # Sector filter
    if current_filters.get("sector") and current_filters["sector"] != "All":
        filtered = filtered[filtered['Sector'] == current_filters["sector"]]
    
    # Parent companies filter
    if (current_filters.get("parent_companies") and 
        not (len(current_filters["parent_companies"]) == 1 and 
        current_filters["parent_companies"][0] == "All")):
        filtered = filtered[filtered['parent_company'].isin(current_filters["parent_companies"])]
    
    # Get unique values from filtered data
    options['sectors'] += sorted(filtered['Sector'].dropna().unique().tolist())
    options['parent_companies'] += sorted(filtered['parent_company'].dropna().unique().tolist())
    options['companies'] += sorted(filtered['company'].dropna().unique().tolist())
    options['countries'] += sorted(filtered['country'].dropna().unique().tolist())
    
    return options


# First, let's modify the calculate_weekly_totals function to include retailer info
def calculate_weekly_totals(filtered_closings, filtered_openings, start_year, start_week, end_year, end_week):
    """Calculate cumulative weekly closures and openings with retailer info"""
    logger.info("Calculating weekly cumulative totals")
    
    current_iso_week = datetime.now().isocalendar()[1]
    current_year = datetime.now().year
    
    # Validate week ranges
    if end_year > current_year or (end_year == current_year and end_week > current_iso_week):
        logger.warning(f"Adjusting end week from {end_week} to current week {current_iso_week}")
        end_week = current_iso_week
        end_year = current_year
    
    weekly_results = []
    cumulative_closures = 0
    cumulative_openings = 0
    
    # First calculate cumulative totals up to start week
    for year in range(start_year, end_year + 1):
        year_start_week = 1 if year == start_year else 1
        year_end_week = start_week - 1 if year == start_year else min(53, current_iso_week if year == current_year else 53)
        
        for week_num in range(year_start_week, year_end_week + 1):
            week_col = f'week_{week_num}'
            
            if week_col in filtered_closings.columns:
                cumulative_closures += filtered_closings[week_col].fillna(0).sum()
            if week_col in filtered_openings.columns:
                cumulative_openings += filtered_openings[week_col].fillna(0).sum()
    
    # Now calculate for the requested period and collect retailer info
    retailer_info = {}
    for _, row in pd.concat([filtered_closings, filtered_openings]).iterrows():
        retailer_id = row['retailer_id']
        if retailer_id not in retailer_info:
            retailer_info[retailer_id] = {
                'retailer_name': row['company'],
                'parent_company': row['parent_company']
            }
    
    # Now calculate for the requested period
    for year in range(start_year, end_year + 1):
        year_start_week = start_week if year == start_year else 1
        year_end_week = end_week if year == end_year else min(53, current_iso_week if year == current_year else 53)
        
        for week_num in range(year_start_week, year_end_week + 1):
            week_col = f'week_{week_num}'
            
            # Weekly values
            week_closures = filtered_closings[week_col].fillna(0).sum() if week_col in filtered_closings.columns else 0
            week_openings = filtered_openings[week_col].fillna(0).sum() if week_col in filtered_openings.columns else 0
            
            # Cumulative totals
            cumulative_closures += week_closures
            cumulative_openings += week_openings
            
            weekly_results.append({
                'year': year,
                'week': week_num,
                'week_label': f"{year}-W{week_num:02d}",
                'weekly_closures': week_closures,
                'cumulative_closures': cumulative_closures,
                'weekly_openings': week_openings,
                'cumulative_openings': cumulative_openings,
                'retailer_ids': list(retailer_info.keys()),
                'retailer_names': [info['retailer_name'] for info in retailer_info.values()],
                'parent_companies': [info['parent_company'] for info in retailer_info.values()]
            })
    
    return pd.DataFrame(weekly_results)

def main():
    st.set_page_config(layout="wide", page_title="Store Tracker Dashboard")

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
    try:
        with st.spinner("Loading data..."):
            closings_df, openings_df = fetch_store_data()
            if closings_df.empty or openings_df.empty:
                st.error("No data loaded from database")
                st.stop()
    except Exception as e:
        st.error(f"Failed to load data: {str(e)}")
        st.stop()
    
    # Get initial filter options
    initial_options = get_filter_options(closings_df, openings_df)
    current_year = datetime.now().year
    current_week = datetime.now().isocalendar()[1]
        
    # Initialize session state for filters
    if 'filters' not in st.session_state:
        st.session_state.filters = {
            "start_year": current_year,
            "start_week": 1,
            "end_year": current_year,
            "end_week": current_week,
            "sector": "All",
            "parent_companies": ["All"],
            "companies": ["All"],
            "country": "All"
        }
    
    # Create filter UI
    with st.container():
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            with st.expander("Date Range", expanded=False):
                valid_years = [year for year in initial_options['years'] if year != 0]
                valid_years = sorted(valid_years)

                start_col1, start_col2 = st.columns(2)
                with start_col1:
                    st.session_state.filters["start_year"] = st.selectbox(
                        "Start Year", valid_years,
                        index=valid_years.index(current_year),
                        key="start_year_select"
                    )
                with start_col2:
                    st.session_state.filters["start_week"] = st.selectbox(
                        "Start Week", list(range(1, 54)),
                        index=0,
                        key="start_week_select"
                    )

                end_col1, end_col2 = st.columns(2)
                with end_col1:
                    end_year_options = [y for y in valid_years if y >= st.session_state.filters["start_year"]]
                    st.session_state.filters["end_year"] = st.selectbox(
                        "End Year", end_year_options,
                        index=end_year_options.index(current_year),
                        key="end_year_select"
                    )
                with end_col2:
                    if st.session_state.filters["end_year"] == st.session_state.filters["start_year"]:
                        ew_opts = list(range(st.session_state.filters["start_week"], 54))
                        ew_idx = min(current_week - st.session_state.filters["start_week"], len(ew_opts) - 1)
                    else:
                        ew_opts = list(range(1, 54))
                        ew_idx = current_week - 1
                    st.session_state.filters["end_week"] = st.selectbox(
                        "End Week", ew_opts,
                        index=ew_idx,
                        key="end_week_select"
                    )

        date_filtered = pd.concat([closings_df, openings_df])
        date_filtered['year'] = date_filtered['year'].astype(int)
        date_mask = (
            (date_filtered['year'] > st.session_state.filters["start_year"]) |
            ((date_filtered['year'] == st.session_state.filters["start_year"]) & (date_filtered['Week_Number'] >= st.session_state.filters["start_week"]))
        ) & (
            (date_filtered['year'] < st.session_state.filters["end_year"]) |
            ((date_filtered['year'] == st.session_state.filters["end_year"]) & (date_filtered['Week_Number'] <= st.session_state.filters["end_week"]))
        )
        date_filtered = date_filtered.loc[date_mask]

        def clean_options(options):
            return [opt for opt in options if opt not in [0, None, "0", "None"]]

        dynamic_options = get_dynamic_filter_options(date_filtered, st.session_state.filters)
        for key in ['sectors', 'parent_companies', 'companies', 'countries']:
            dynamic_options[key] = ["All"] + clean_options(dynamic_options[key][1:])

        with col2:
            with st.expander("Sector", expanded=False):
                st.session_state.filters["sector"] = st.selectbox(
                    "Select Sector", dynamic_options['sectors'], index=0, key="sector_select"
                )

        if st.session_state.filters["sector"] != "All":
            sector_filtered = date_filtered[date_filtered['Sector'] == st.session_state.filters["sector"]]
            dynamic_options = get_dynamic_filter_options(sector_filtered, st.session_state.filters)
            for key in ['parent_companies', 'companies', 'countries']:
                dynamic_options[key] = ["All"] + clean_options(dynamic_options[key][1:])

        with col3:
            with st.expander("Retailers", expanded=False):
                st.session_state.filters["parent_companies"] = st.multiselect(
                    "Parent Companies", dynamic_options['parent_companies'],
                    default=["All"], key="parent_companies_select"
                )
                st.session_state.filters["companies"] = st.multiselect(
                    "Brands/Chains", dynamic_options['companies'],
                    default=["All"], key="companies_select"
                )

        with col4:
            with st.expander("Location", expanded=False):
                st.session_state.filters["country"] = st.selectbox(
                    "Country", dynamic_options['countries'], index=0, key="country_select"
                )

    # Apply filters
    filtered_closings = apply_filters(closings_df, st.session_state.filters)
    filtered_openings = apply_filters(openings_df, st.session_state.filters)
    
    # Rest of your visualization code remains the same...
    weekly_data = calculate_weekly_totals(filtered_closings, filtered_openings, 
                                        st.session_state.filters["start_year"], 
                                        st.session_state.filters["start_week"],
                                        st.session_state.filters["end_year"], 
                                        st.session_state.filters["end_week"])
    
    # Visualization
    if not weekly_data.empty:
        fig = go.Figure()
        
        # Add cumulative closures
        fig.add_trace(go.Scatter(
            x=weekly_data['week_label'],
            y=weekly_data['cumulative_closures'],
            name='Cumulative Closures',
            mode='lines+markers',
            line=dict(color='#d62728', width=2),
            marker=dict(size=8)
        ))
        
        # Add cumulative openings
        fig.add_trace(go.Scatter(
            x=weekly_data['week_label'],
            y=weekly_data['cumulative_openings'],
            name='Cumulative Openings',
            mode='lines+markers',
            line=dict(color='#2ca02c', width=2),
            marker=dict(size=8)
        ))
        
        fig.update_layout(
            #title=f"Cumulative Store Closures & Openings",
            xaxis_title="Week",
            yaxis_title="Number of Stores",
            hovermode="x unified",
            xaxis=dict(tickangle=45),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=40, r=40, t=40, b=0),   # ← here!
        )
        
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        
        # Show data table
        # st.write("Weekly Data with Retailer Information:")
        # display_df = weekly_data[['week_label', 'weekly_closures', 'cumulative_closures', 
        #                         'weekly_openings', 'cumulative_openings']].rename(
        #     columns={
        #         'week_label': 'Week',
        #         'weekly_closures': 'Weekly Closures',
        #         'cumulative_closures': 'Total Closures',
        #         'weekly_openings': 'Weekly Openings',
        #         'cumulative_openings': 'Total Openings'
        #     }
        # )
        # st.dataframe(display_df)
        
        # # Summary metrics
        # col1, col2 = st.columns(2)
        # with col1:
        #     st.metric("Total Closures in Period", weekly_data['cumulative_closures'].iloc[-1])
        # with col2:
        #     st.metric("Total Openings in Period", weekly_data['cumulative_openings'].iloc[-1])
    else:
        st.warning("No data available for the selected filters")
        logger.warning("No data available after filtering")

if __name__ == "__main__":
    main()