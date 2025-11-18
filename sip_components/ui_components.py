from plotly.graph_objs.isosurface.caps import Z
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import calendar
import math
import logging
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder

class UIComponents:
    @staticmethod
    def render_metric_card(title, value, subtitle="", column=None):
        """Render a standardized metric card"""
        print("Rendering metric card:", title, value, subtitle)
        # Handle different value types (numbers vs strings)
        if isinstance(value, (int, float)):
            formatted_value = f"{value:,}"
        else:
            formatted_value = str(value)
            
        if column:
            with column:
                st.markdown(f"""
                <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{formatted_value}</h1>
                <h6 style='text-align: left; margin-top: 0px;'>{title}</h6>
                """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <h1 style='font-size: 40px; text-align: left; margin-bottom: 0px;'>{formatted_value}</h1>
            <h6 style='text-align: left; margin-top: 0px;'>{title}</h6>
            """, unsafe_allow_html=True)
            
    @staticmethod
    def render_horizontal_line():
        """Render a standardized horizontal line"""
        st.markdown("""<style>.custom-hr {border: none;border-top: 1px solid #808080; width: 100%; margin: -10px; padding: 0; margin-left: auto; margin-right: auto;
        }
        </style>
        """,
        unsafe_allow_html=True
        )
        st.markdown('<hr class="custom-hr">', unsafe_allow_html=True)
        
    @staticmethod
    def calculate_yaxis_limit(value):
        """Calculate the next highest rounded limit for y-axis"""
        try:
            if pd.isna(value):
                logging.warning("No data available to display the chart.")
                return None
            if value <= 10:
                return 10
            increased_value = value + (value * 0.1)
            return math.ceil(increased_value / 10) * 10
        except ValueError:
            return None
            
    @staticmethod
    def render_active_stores_over_time_chart(data, column=None):
        """Render the active stores over time chart"""
        try:
            data['PeriodMonth'] = data['Period'].dt.to_period('M').dt.to_timestamp()
            active_stores_over_time = data.groupby('PeriodMonth').size().reset_index(name='ActiveCount')
            active_stores_over_time.rename(columns={'PeriodMonth': 'MonthStart'}, inplace=True)

            # Create lists to store metadata for each data point
            num_sectors_list = []
            num_banners_list = []
            num_states_list = []
            num_msa_list = []

            # Calculate metadata for each month
            for month in active_stores_over_time['MonthStart']:
                month_data = data[data['PeriodMonth'] == month]
                num_sectors_list.append(month_data['Sector_Coresight'].nunique())
                num_banners_list.append(month_data['ChainName_Coresight'].nunique())
                num_states_list.append(month_data['State'].nunique())
                num_msa_list.append(month_data['MsaName'].nunique())

            # Create the figure with visible counts
            fig_active_line = go.Figure()

            # Add trace with both permanent labels and hover info
            fig_active_line.add_trace(go.Scatter(
                x=active_stores_over_time['MonthStart'],
                y=active_stores_over_time['ActiveCount'],
                name='Active Stores',
                mode='lines+markers+text',
                text=[f"{x:,}" for x in active_stores_over_time['ActiveCount']],
                textposition='top center',
                line=dict(color='#CBCACA', width=2),
                marker=dict(size=7, color='#CBCACA'),
                hovertemplate=(
                    "%{x|%b %Y}<br>"
                    "<span style='color:#CBCACA'>●</span> "
                    "<b>Active Store Count:</b> <b>%{y:,}</b><br><br>"
                    "<span style='color:#CBCACA'>•</span> "
                    "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                    "<span style='color:#CBCACA'>•</span> "
                    "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                    "<span style='color:#CBCACA'>•</span> "
                    "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                    "<span style='color:#CBCACA'>•</span> "
                    "<span style='color:black'>Number of MSA: %{customdata[3]}</span><br>"
                    "<extra></extra>"
                ),
                customdata=list(zip(num_sectors_list, num_banners_list, num_states_list, num_msa_list)),
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor="#CBCACA",
                    font=dict(size=11, color="black", family="Arial")
                )
            ))

            # Update layout with text styling
            fig_active_line.update_layout(
                yaxis_title="Active Stores",
                margin=dict(l=10, r=10, t=60, b=40),
                height=400,
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
                uniformtext_minsize=8,
                uniformtext_mode='hide'
            )

            # Same config as before
            config = {
                'displayModeBar': True,
                'displaylogo': False,
                'toImageButtonOptions': {
                    'format': 'png',
                    'filename': 'active_stores_over_time',
                    'height': 400,
                    'width': 700,
                    'scale': 1
                }
            }

            # Display the chart
            if column:
                with column:
                    st.markdown("<h4 style='font-size: 20px;text-align: center;'>Active Stores Over Time</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_active_line, use_container_width=True, config=config)
            else:
                st.markdown("<h4 style='font-size: 20px;text-align: center;'>Active Stores Over Time</h4>", unsafe_allow_html=True)
                st.plotly_chart(fig_active_line, use_container_width=True, config=config)
                
        except Exception as e:
            st.write(f"Error creating chart: {e}")
            
    @staticmethod
    def render_active_chains_bar_chart(data, column=None):
        """Render the active chains bar chart (Recent Month Only)"""
        try:
            # Get the most recent period from the filtered data
            recent_period = data['Period'].max()

            # Filter for only the most recent period's data
            recent_active_data = data[data['Period'] == recent_period]

            # Count stores by chain for the recent period only
            active_store_counts = recent_active_data['ChainName_Coresight'].value_counts().head(15)

            if not active_store_counts.empty:
                y_max_active = UIComponents.calculate_yaxis_limit(active_store_counts.max())

                # Calculate metadata for the recent period
                recent_period_str = recent_period.strftime('%b %Y')
                num_states = recent_active_data['State'].nunique()
                num_msa = recent_active_data['MsaName'].nunique()

                # Create figure with consistent styling
                fig_active_bar = go.Figure()

                fig_active_bar.add_trace(go.Bar(
                    x=active_store_counts.index,
                    y=active_store_counts.values,
                    marker_color='#CBCACA',  # Using your specified gray color
                    text=[f"{x:,}" for x in active_store_counts.values],  # Formatted with thousands separator
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"  # Banner name
                        "<span style='color:#CBCACA'>•</span> "
                        "<b>Active Stores:</b> %{y:,}<br><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Date Period: {recent_period_str}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Number of States: {num_states:,}</span><br>"
                        "<span style='color:#CBCACA'>•</span> "
                        f"<span style='color:black'>Number of MSA: {num_msa:,}</span><br>"
                        "<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor='#CBCACA',
                        font=dict(size=11, color="black", family="Arial")
                    )
                ))

                # Update layout
                fig_active_bar.update_layout(
                    yaxis_title="Active Stores",
                    margin=dict(l=10, r=10, t=40, b=40),
                    height=400,
                    xaxis=dict(
                        showline=True,
                        zeroline=False,
                        title=""
                    ),
                    yaxis=dict(
                        showline=True,
                        zeroline=False,
                        range=[0, y_max_active] if y_max_active else None
                    ),
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )

                config = {
                    'displayModeBar': True,
                    'displaylogo': False,
                    'toImageButtonOptions': {
                        'format': 'png',
                        'filename': 'top_15_active_chains',
                        'height': 400,
                        'width': 700,
                        'scale': 1
                    }
                }

                if column:
                    with column:
                        st.markdown("<h4 style='font-size: 20px; text-align: center;'>Top 15 Active Chains</h4>", unsafe_allow_html=True)
                        st.plotly_chart(fig_active_bar, use_container_width=True, config=config)
                else:
                    st.markdown("<h4 style='font-size: 20px; text-align: center;'>Top 15 Active Chains</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_active_bar, use_container_width=True, config=config)
            else:
                if column:
                    with column:
                        st.write("No data available for the selected filters.")
                else:
                    st.write("No data available for the selected filters.")
        except Exception as e:
            st.write(f"Error creating chart: {e}")
            
    @staticmethod
    def render_opened_stores_over_time_chart(data, column=None, store_type="Opened", color="#A3C0CE", store_label="Store Opened Count"):
        """Render the opened stores over time chart"""
        try:
            data['PeriodMonth'] = data['Period'].dt.to_period('M').dt.to_timestamp()
            opened_stores_over_time = data.groupby('PeriodMonth').size().reset_index(name='OpenedCount')
            opened_stores_over_time.rename(columns={'PeriodMonth': 'MonthStart'}, inplace=True)

            # Create lists to store metadata for each data point
            num_sectors_list = []
            num_banners_list = []
            num_states_list = []
            num_msa_list = []

            # Calculate metadata for each month
            for month in opened_stores_over_time['MonthStart']:
                month_data = data[data['PeriodMonth'] == month]
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
                name=f'{store_type} Stores',
                mode='lines+markers+text',
                text=[f"{x:,}" for x in opened_stores_over_time['OpenedCount']],
                textposition='top center',
                line=dict(color=color, width=2),
                marker=dict(size=7),
                hovertemplate=(
                    "%{x|%b %Y}<br>"
                    f"<span style='color:{color}'>•</span> "
                    f"<b>{store_label}:</b> <b>%{{y:,}}</b><br><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of MSA: %{customdata[3]}</span><br>"
                    "<extra></extra>"
                ),
                customdata=list(zip(num_sectors_list, num_banners_list, num_states_list, num_msa_list)),
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor=color,
                    font=dict(size=11, color="black", family="Arial")
                )
            ))

            # Update layout with text styling
            fig_opened_line.update_layout(
                yaxis_title=f"{store_type} Stores",
                margin=dict(l=10, r=10, t=60, b=40),
                height=400,
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
                uniformtext_minsize=8,
                uniformtext_mode='hide'
            )

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

            if column:
                with column:
                    st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>{store_type} Stores Over Time</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_opened_line, use_container_width=True, config=config)
            else:
                st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>{store_type} Stores Over Time</h4>", unsafe_allow_html=True)
                st.plotly_chart(fig_opened_line, use_container_width=True, config=config)
                
        except Exception as e:
            st.write(f"Error creating chart: {e}")
            
    @staticmethod
    def render_opened_chains_bar_chart(data, column=None, store_type="Opened", color="#A3C0CE"):
        """Render the opened chains bar chart"""
        try:
            if store_type == "Net":
                opened_by_chain = data[data['data_from'] == 'opened']['ChainName_Coresight'].value_counts()
                closed_by_chain = data[data['data_from'] == 'closed']['ChainName_Coresight'].value_counts()
                net_by_chain = opened_by_chain.subtract(closed_by_chain, fill_value=0).astype(int)
                opened_store_counts = net_by_chain.sort_values(ascending=False).head(15)
            else:
                opened_store_counts = data['ChainName_Coresight'].value_counts().head(15)
            
            if not opened_store_counts.empty:
                # Calculate y-axis limits to accommodate negative values for Net stores
                if store_type == "Net":
                    y_max_opened = UIComponents.calculate_yaxis_limit(opened_store_counts.max())
                    y_min_opened = UIComponents.calculate_yaxis_limit(abs(opened_store_counts.min()))
                    y_range = max(y_max_opened or 0, y_min_opened or 0) or 1
                else:
                    y_max_opened = UIComponents.calculate_yaxis_limit(opened_store_counts.max())
                    y_range = y_max_opened or 1

                # Calculate dynamic date period
                start_date = data['Period'].min()
                end_date = data['Period'].max()
                start_date_str = start_date.strftime("%b %Y")
                end_date_str = end_date.strftime("%b %Y")
                date_period = f"{start_date_str} to {end_date_str}"

                # Calculate metadata for the entire dataset
                num_states = data['State'].nunique()
                num_msa = data['MsaName'].nunique()

                # Create figure with consistent styling
                fig_opened_bar = go.Figure()

                fig_opened_bar.add_trace(go.Bar(
                    x=opened_store_counts.index,
                    y=opened_store_counts.values,
                    marker_color=color,
                    text=[f"{x:,}" for x in opened_store_counts.values],
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"
                        f"<span style='color:{color}'>•</span> "
                        f"<b>{store_type} Stores:</b> %{{y:,}}<br><br>"
                        f"<span style='color:{color}'>•</span> "
                        f"<span style='color:black'>Date Period: {date_period}</span><br>"
                        f"<span style='color:{color}'>•</span> "
                        f"<span style='color:black'>Number of States: {num_states:,}</span><br>"
                        f"<span style='color:{color}'>•</span> "
                        f"<span style='color:black'>Number of MSA: {num_msa:,}</span><br>"
                        "<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor=color,
                        font=dict(size=11, color="black", family="Arial")
                    )
                ))

                # Calculate dynamic height based on number of banners
                dynamic_height = 400 + (20 * (len(opened_store_counts) - 10)) if len(opened_store_counts) > 10 else 400

                fig_opened_bar.update_layout(
                    xaxis_title="Banner Name",
                    yaxis_title=f"{store_type} Stores",
                    yaxis=dict(
                        range=[-y_range if store_type == 'Net' else 0, y_range],
                        autorange=False,
                        showline=True,
                        zeroline=False
                    ),
                    xaxis=dict(
                        showline=True,
                        zeroline=False,
                        tickangle=45 if len(opened_store_counts) > 5 else 0,
                        tickfont=dict(size=11)
                    ),
                    height=dynamic_height,
                    margin=dict(l=20, r=20, t=60, b=40 + (10 * len(opened_store_counts))),
                    uniformtext_minsize=8,
                    uniformtext_mode='hide'
                )

                # Adjust for many banners
                if len(opened_store_counts) > 8:
                    fig_opened_bar.update_layout(
                        xaxis=dict(tickangle=60),
                        margin=dict(b=60 + (10 * len(opened_store_counts)))
                    )

                config = {
                    'displayModeBar': True,
                    'displaylogo': False,
                    'toImageButtonOptions': {
                        'format': 'png',
                        'filename': 'opened_chains_bar_chart',
                        'height': dynamic_height,
                        'width': 700,
                        'scale': 1
                    }
                }

                if column:
                    with column:
                        num_banners = min(15, opened_store_counts.shape[0])
                        st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {num_banners} Banners</h4>", unsafe_allow_html=True)
                        st.plotly_chart(fig_opened_bar, use_container_width=True, config=config)
                else:
                    num_banners = min(15, opened_store_counts.shape[0])
                    st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {num_banners} Banners</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_opened_bar, use_container_width=True, config=config)
            else:
                if column:
                    with column:
                        st.write("No data available for the selected filters.")
                else:
                    st.write("No data available for the selected filters.")
        except Exception as e:
            st.write(f"Error creating chart: {e}")
            
    @staticmethod
    def render_opened_stores_by_state_map(data, column=None, store_type="Opened", color="#A3C0CE"):
        """Render the opened stores by state map"""
        try:
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

            # Function to map full state names to abbreviations
            def map_state_names(df, state_col='State'):
                df[state_col] = df[state_col].map(lambda x: state_abbreviations.get(x, x))
                return df

            # Apply the mapping
            mapped_data = map_state_names(data.copy())

            # For Net type, calculate opened - closed; otherwise just count all records
            if store_type == "Net":
                opened_data = mapped_data[mapped_data['data_from'] == 'opened']
                closed_data = mapped_data[mapped_data['data_from'] == 'closed']
                opened_by_state_counts = opened_data.groupby('State').size()
                closed_by_state_counts = closed_data.groupby('State').size()
                net_by_state = opened_by_state_counts.subtract(closed_by_state_counts, fill_value=0).astype(int)
                opened_by_state = net_by_state.reset_index(name='Opened Stores')
            else:
                opened_by_state = mapped_data.groupby('State').size().reset_index(name='Opened Stores')
            opened_by_state = opened_by_state.dropna()  # Drop any rows with unmapped states

            # Check if there's no data to display
            has_data = not opened_by_state.empty and not opened_by_state['State'].isna().all()
            if not has_data:
                if column:
                    with column:
                        st.write("No state data available for map display.")
                else:
                    st.write("No state data available for map display.")
            else:
                import math

                def calculate_distance(lat1, lon1, lat2, lon2):
                    """Calculate distance between two points on map"""
                    return math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)

                def detect_overlapping_states_by_count(opened_by_state, state_centers, min_distance=1.5):
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
                abbreviation_to_full = {v: k for k, v in state_abbreviations.items()}

                # Add full state name column to the dataframe
                # --- 1) Build a full 50-state dataframe (0 for states with no data) ---
                all_states = pd.DataFrame({"State": sorted(abbreviation_to_full.keys())})
                opened_by_state_full = (
                    all_states
                    .merge(opened_by_state[["State", "Opened Stores"]], on="State", how="left")
                    .fillna({"Opened Stores": 0})
                )
                
                # Use apply instead of map to avoid type issues
                opened_by_state_full["State_Full"] = opened_by_state_full["State"].apply(lambda x: abbreviation_to_full.get(x, x))

                # --- Per-state metadata (0s for empty states) ---
                state_metadata = {}
                for s in opened_by_state_full["State"]:
                    sd = mapped_data[mapped_data["State"] == s]
                    state_metadata[s] = {
                        "num_sectors": sd["Sector_Coresight"].nunique() if "Sector_Coresight" in sd.columns else 0,
                        "num_banners": sd["ChainName_Coresight"].nunique() if "ChainName_Coresight" in sd.columns else 0,
                        "num_msa":     sd["MsaName"].nunique() if "MsaName" in sd.columns else 0,
                    }

                # --- Date period ---
                start_date = mapped_data["Period"].min()
                end_date   = mapped_data["Period"].max()
                start_date_str = start_date.strftime("%b %Y")
                end_date_str   = end_date.strftime("%b %Y")
                date_period = f"{start_date_str} to {end_date_str}"

                # --- Helper functions for color handling (Net stores only) ---
                def hex_to_rgb(hex_color):
                    h = hex_color.lstrip('#')
                    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))

                def rgb_to_hex(rgb_tuple):
                    return "#{:02x}{:02x}{:02x}".format(*rgb_tuple)

                def generate_shades(hex_color, n=19, lightest="#F4F6F9"):
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

                # --- Choropleth over ALL states (zeros included) ---
                if store_type == "Net":
                    # For Net stores, handle negative values with a diverging color scale
                    zmax_abs = int(opened_by_state_full["Opened Stores"].abs().max()) or 1
                    color_scale_net = generate_shades("#2D2A29", n=20, lightest="#F4F2F1")
                    
                    fig_opened_map = px.choropleth(
                        opened_by_state_full,
                        locations="State",
                        locationmode="USA-states",
                        color="Opened Stores",
                        range_color=(-zmax_abs, zmax_abs),
                        color_continuous_scale=color_scale_net,
                        scope="usa",
                        hover_name="State_Full",
                        hover_data={"State": False}  # keep hover clean; we'll control with hovertemplate
                    )
                else:
                    # For non-Net stores, use the original color scale
                    zmax = int(opened_by_state_full["Opened Stores"].max()) or 1
                    fig_opened_map = px.choropleth(
                        opened_by_state_full,
                        locations="State",
                        locationmode="USA-states",
                        color="Opened Stores",
                        range_color=(0, zmax),
                        color_continuous_scale=["#ffffff", color],
                        scope="usa",
                        hover_name="State_Full",
                        hover_data={"State": False}  # keep hover clean; we'll control with hovertemplate
                    )

                # Hover matches your style (with metadata + date period)
                fig_opened_map.update_traces(
                    hovertemplate=(
                        "<b>%{hovertext}</b><br>"
                        f"<span style='color:{color}'>•</span> <b>{store_type} Stores:</b> %{{z:,}}<br><br>"
                        f"<span style='color:{color}'>•</span> <span style='color:black'>Date Period: " + date_period + "</span><br>"
                        f"<span style='color:{color}'>•</span> <span style='color:black'>Number of Sectors: %{{customdata[0]:,}}</span><br>"
                        f"<span style='color:{color}'>•</span> <span style='color:black'>Number of Banners: %{{customdata[1]:,}}</span><br>"
                        f"<span style='color:{color}'>•</span> <span style='color:black'>Number of MSA: %{{customdata[2]:,}}</span><br>"
                        "<extra></extra>"
                    ),
                    customdata=[[state_metadata[s]["num_sectors"], state_metadata[s]["num_banners"], state_metadata[s]["num_msa"]] for s in opened_by_state_full["State"]],
                    hoverlabel=dict(bgcolor="white", bordercolor=color, font=dict(size=11, color="black", family="Arial")),
                )

                # Colorbar title like the reference
                fig_opened_map.update_coloraxes(colorbar_title=f"{store_type} Stores")

                # Layout to match the reference's look
                fig_opened_map.update_layout(
                    margin=dict(l=0, r=0, t=30, b=0),
                    geo=dict(
                        projection_scale=1.2,
                        center=dict(lat=37.5, lon=-95),
                        showlakes=True, lakecolor="rgb(255,255,255)",
                        showframe=False, showcoastlines=False
                    )
                )

                # --- Text labels ONLY for states with data (Net: all non-zero, Others: >0), avoiding overlaps ---
                if store_type == "Net":
                    # For Net stores, show all non-zero values (including negative)
                    ann_df = opened_by_state_full[opened_by_state_full["Opened Stores"] != 0]
                else:
                    # For non-Net stores, show only positive values
                    ann_df = opened_by_state_full[opened_by_state_full["Opened Stores"] > 0]
                
                excluded_states = detect_overlapping_states_by_count(ann_df, state_centers)  # your helper
                
                # Convert excluded_states to a list for pandas isin method
                excluded_states_list = list(excluded_states)
                
                # Convert ann_df["State"] to a pandas Series to use isin method
                state_series = pd.Series(ann_df["State"])
                ann_df = ann_df[~state_series.isin(excluded_states_list)]

                # Determine text colors based on background for better visibility
                if store_type == "Net":
                    # For Net stores, use black text for very negative values, white otherwise
                    text_colors = [
                        "black" if val < -150 else "white"
                        for state, val in zip(ann_df["State"], ann_df["Opened Stores"])
                        if state in state_centers
                    ]
                else:
                    # For non-Net stores, use black text
                    text_colors = ["black"] * len(ann_df)

                annotations_opened = go.Scattergeo(
                    locationmode="USA-states",
                    lon=[state_centers[s][0] for s in ann_df["State"] if s in state_centers],
                    lat=[state_centers[s][1] for s in ann_df["State"] if s in state_centers],
                    text=[f"{s}<br>{int(cnt):,}" for s, cnt in zip(ann_df["State"], ann_df["Opened Stores"]) if s in state_centers],
                    mode="text",
                    showlegend=False,
                    textfont=dict(size=12, color=text_colors if store_type == "Net" else "black"),
                    hoverinfo="skip"
                )
                fig_opened_map.add_trace(annotations_opened)

                if column:
                    with column:
                        st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>{store_type} Stores by State</h4>", unsafe_allow_html=True)
                        st.plotly_chart(fig_opened_map, use_container_width=True)
                else:
                    st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>{store_type} Stores by State</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_opened_map, use_container_width=True)
        except Exception as e:
            if column:
                with column:
                    st.write(f"Error creating map: {e}")
            else:
                st.write(f"Error creating map: {e}")
                
    @staticmethod
    def render_opened_cities_bar_chart(data, column=None, store_type="Opened", color="#A3C0CE"):
        """Render the opened cities bar chart"""
        try:
            if store_type == "Net":
                opened_by_city = data[data['data_from'] == 'opened']['City'].value_counts()
                closed_by_city = data[data['data_from'] == 'closed']['City'].value_counts()
                net_by_city = opened_by_city.subtract(closed_by_city, fill_value=0).astype(int)
                opened_store_counts = net_by_city.sort_values(ascending=False).head(15)
            else:
                opened_store_counts = data['City'].value_counts().head(15)
            
            # Calculate y-axis limits to accommodate negative values for Net stores
            if store_type == "Net":
                y_max_opened = UIComponents.calculate_yaxis_limit(opened_store_counts.max())
                y_min_opened = UIComponents.calculate_yaxis_limit(abs(opened_store_counts.min()))
                y_range = max(y_max_opened or 0, y_min_opened or 0) or 1
            else:
                y_max_opened = UIComponents.calculate_yaxis_limit(opened_store_counts.max())
                y_range = y_max_opened or 1

            # Calculate metadata for each city
            city_metadata = {}
            for city in opened_store_counts.index:
                city_data = data[data['City'] == city]
                city_metadata[city] = {
                    'num_sectors': city_data['Sector_Coresight'].nunique(),
                    'num_banners': city_data['ChainName_Coresight'].nunique(),
                    'num_states': city_data['State'].nunique(),
                    'num_msa': city_data['MsaName'].nunique()
                }

            # Calculate overall date range
            start_date = data['Period'].min()
            end_date   = data['Period'].max()
            start_date_str = start_date.strftime("%B %Y")
            end_date_str   = end_date.strftime("%B %Y")
            date_period = f"{start_date_str} to {end_date_str}"

            # Create figure with consistent styling
            fig_opened_bar = go.Figure()

            fig_opened_bar.add_trace(go.Bar(
                x=opened_store_counts.index,
                y=opened_store_counts.values,
                marker_color=color,
                text=[f"{x:,}" for x in opened_store_counts.values],
                textposition='outside',
                hovertemplate=(
                    "<b>%{x}</b><br>"  # City name
                    f"<span style='color:{color}'>•</span> "
                    f"<b>{store_type} Stores:</b> %{{y:,}}<br><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Date Period: " + f"{date_period}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of MSA: %{customdata[3]}</span><br>"
                    "<extra></extra>"
                ),
                customdata=[[city_metadata[city]['num_sectors'], city_metadata[city]['num_banners'], city_metadata[city]['num_states'], city_metadata[city]['num_msa']] for city in opened_store_counts.index],
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor=color,
                    font=dict(size=11, color="black", family="Arial")
                )
            ))

            fig_opened_bar.update_layout(
                xaxis_title="City",
                yaxis_title=f"{store_type} Stores",
                yaxis=dict(
                    range=[-y_range if store_type == 'Net' else 0, y_range],
                    autorange=True,
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
            if column:
                with column:
                    st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {opened_store_counts.shape[0]} Cities</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_opened_bar, use_container_width=True)
            else:
                st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {opened_store_counts.shape[0]} Cities</h4>", unsafe_allow_html=True)
                st.plotly_chart(fig_opened_bar, use_container_width=True)
        except Exception as e:
            if column:
                with column:
                    st.write(f"Error creating chart: {e}")
            else:
                st.write(f"Error creating chart: {e}")
                
    @staticmethod
    def render_opened_stores_per_capita_map(data, column=None, store_type="Opened", color="#A3C0CE"):
        """Render the opened stores per capita map"""
        try:
            from sip_components.database_manager import DatabaseManager
            import streamlit as st
            
            # Abbreviation to full state name mapping
            abbrev_to_full_name = {
                'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
                'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
                'DC': 'District of Columbia', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii',
                'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
                'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
                'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
                'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
                'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
                'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
                'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
                'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
                'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
                'WI': 'Wisconsin', 'WY': 'Wyoming', 'PR': 'Puerto Rico'
            }
            
            # Fetch population data from database
            db = DatabaseManager()
            pop_df = db.fetch_population_data()
            
            # Clean population data
            pop_df['usps_state_name'] = pop_df['usps_state_name'].astype(str).str.strip()
            pop_df['zip_code'] = pop_df['zip_code'].astype(str).str.strip()
            pop_df['estimate_total_population'] = pop_df['estimate_total_population'].astype(str).str.replace('(X)', 'NaN').str.replace('*', 'NaN')
            pop_df['estimate_total_population'] = pd.to_numeric(pop_df['estimate_total_population'], errors='coerce')
            
            # Map state abbreviations to full names
            pop_df['usps_state_name'] = pop_df['usps_state_name'].map(abbrev_to_full_name)
            
            # Create state population lookup
            pop_df_filtered = pop_df.dropna(subset=['usps_state_name','zip_code','estimate_total_population']).copy()
            pop_df_filtered['estimate_total_population'] = pd.to_numeric(pop_df_filtered['estimate_total_population'], errors='coerce')
            pop_df_filtered = pop_df_filtered.dropna(subset=['estimate_total_population'])
            pop_df_filtered = pop_df_filtered[pop_df_filtered['estimate_total_population'] > 0]
            pop_df_filtered = pop_df_filtered[~pop_df_filtered['zip_code'].isin(['0','00000',''])]
            
            state_pop_lookup = (
                pop_df_filtered
                      .drop_duplicates(subset=['usps_state_name','zip_code'])
                      .groupby('usps_state_name', as_index=False)['estimate_total_population']
                      .sum()
                      .rename(columns={'usps_state_name': 'State',
                                       'estimate_total_population': 'State_Population'})
            )
            
            # Count stores by state
            if store_type == "Net":
                opened = data[data['data_from'] == 'opened']
                closed = data[data['data_from'] == 'closed']
                
                opened_by_state_count = (
                    opened.dropna(subset=['State'])
                          .assign(State=lambda df: df['State'].astype(str).str.strip())
                          .groupby('State', as_index=False)
                          .size()
                          .rename(columns={'size': 'Openings'})
                )
                opened_by_state_count['Openings'] = opened_by_state_count['Openings'].astype(int)
                
                closed_by_state_count = (
                    closed.dropna(subset=['State'])
                          .assign(State=lambda df: df['State'].astype(str).str.strip())
                          .groupby('State', as_index=False)
                          .size()
                          .rename(columns={'size': 'Closings'})
                )
                closed_by_state_count['Closings'] = closed_by_state_count['Closings'].astype(int)
                
                opened_by_state = opened_by_state_count.merge(closed_by_state_count, on='State', how='outer').fillna(0)
                opened_by_state['Openings'] = opened_by_state['Openings'].astype(int)
                opened_by_state['Closings'] = opened_by_state['Closings'].astype(int)
                opened_by_state['Net_Stores'] = opened_by_state['Openings'] - opened_by_state['Closings']
            else:
                opened_by_state = (
                    data.dropna(subset=['State'])
                        .assign(State=lambda df: df['State'].astype(str).str.strip())
                        .groupby('State', as_index=False)
                        .size()
                        .rename(columns={'size': 'Openings'})
                )
                opened_by_state['Openings'] = opened_by_state['Openings'].astype(int)
            
            # Merge with population data
            opened_by_state = opened_by_state.merge(state_pop_lookup, on='State', how='left')
            opened_by_state = opened_by_state[
                opened_by_state['State_Population'].notna() & (opened_by_state['State_Population'] > 0)
            ].copy()
            
            # Calculate per capita
            if store_type == "Net":
                opened_by_state['Opened Stores per Capita'] = (
                    opened_by_state['Net_Stores'] / opened_by_state['State_Population'] * 1_000_000
                ).astype(float)
            else:
                opened_by_state['Opened Stores per Capita'] = (
                    opened_by_state['Openings'] / opened_by_state['State_Population'] * 1_000_000
                ).astype(float)

            opened_by_state = opened_by_state.dropna(subset=['Opened Stores per Capita'])

            # Check if there's no data to display
            has_data = not opened_by_state.empty and not opened_by_state['State'].isna().all()  # type: ignore[truthy-bool]
            if not has_data:
                end_date_str = data['Period'].max().strftime('%B %Y')
                date_suffix = f" for {end_date_str}" if store_type == "Active" else ""
                if column:
                    with column:
                        st.markdown(f"<h4 style='font-size: 20px;text-align: center; color: red; font-weight: bold; margin-top: 100px;'>No data available for per capita map{date_suffix}</h4>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<h4 style='font-size: 20px;text-align: center; color: red; font-weight: bold; margin-top: 100px;'>No data available for per capita map{date_suffix}</h4>", unsafe_allow_html=True)
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

                # Create reverse dictionary to convert abbreviations to full names
                abbreviation_to_full = {v: k for k, v in state_abbreviations.items()}

                # Function to map full state names to abbreviations
                def map_state_names(df, state_col='State'):
                    df[state_col] = df[state_col].map(lambda x: state_abbreviations.get(x, x))
                    return df

                # Apply the mapping
                mapped_data = map_state_names(data.copy())
                mapped_opened_by_state = map_state_names(opened_by_state.copy())
                



                # Create the choropleth map
                fig_opened_map = px.choropleth(
                    mapped_opened_by_state,
                    locations="State",
                    locationmode="USA-states",
                    color="Opened Stores per Capita",
                    color_continuous_scale= ["#ffffff", color],
                    scope="usa",
                    hover_name="State",
                    hover_data={"State": False}  # Only show what we specify in hovertemplate
                )

                # Create mapping from abbreviation to full state name
                abbrev_to_full = {v: k for k, v in state_abbreviations.items()}
                
                # Calculate metadata for each state
                state_metadata = {}
                for state in opened_by_state['State']:
                    state_data = data[data['State'] == state]
                    full_state_name = abbrev_to_full.get(state, state)
                    if store_type == "Net":
                        filtered_data = opened_by_state[opened_by_state['State'] == state]
                        net_stores = int(filtered_data.iloc[0]['Net_Stores']) if len(filtered_data) > 0 else 0
                    else:
                        filtered_data = opened_by_state[opened_by_state['State'] == state]
                        net_stores = int(filtered_data.iloc[0]['Openings']) if len(filtered_data) > 0 else 0
                    state_metadata[state] = {
                        'num_sectors': state_data['Sector_Coresight'].nunique() if 'Sector_Coresight' in state_data.columns else 0,
                        'num_banners': state_data['ChainName_Coresight'].nunique() if 'ChainName_Coresight' in state_data.columns else 0,
                        'num_msa': state_data['MsaName'].nunique() if 'MsaName' in state_data.columns else 0,
                        'full_name': full_state_name,
                        'net_stores': int(net_stores)
                    }

                # Calculate overall date range
                start_date = mapped_data['Period'].min()
                end_date = mapped_data['Period'].max()
                start_date_str = start_date.strftime("%b %Y")
                end_date_str   = end_date.strftime("%b %Y")

                date_period = f"{start_date_str} to {end_date_str}"

                # Customize hover template with state-specific metadata
                per_capita_label = "Net per Million" if store_type == "Net" else "Stores per Million"
                hover_template = (
                    "<b>%{customdata[4]}</b><br>"  # Full state name
                    f"<span style='color:{color}'>●</span> "
                    f"<b>{per_capita_label}:</b> %{{z:,.2f}}<br>"
                )
                if store_type == "Net":
                    hover_template += (
                        f"<span style='color:{color}'>●</span> "
                        "<b>Net Openings:</b> %{customdata[5]:,}<br><br>"
                    )
                else:
                    hover_template += "<br>"
                
                hover_template += (
                    f"<span style='color:{color}'>•</span> "
                    f"<span style='color:black'>Date Period: {date_period}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of Sectors: %{customdata[0]:,}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of Banners: %{customdata[1]:,}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of MSA: %{customdata[2]:,}</span><br>"
                    "<extra></extra>"
                )
                
                
                fig_opened_map.update_traces(
                    hovertemplate=hover_template,
                    customdata=[[state_metadata[state]['num_sectors'], state_metadata[state]['num_banners'], state_metadata[state]['num_msa'], state, state_metadata[state]['full_name'], state_metadata[state]['net_stores']] for state in opened_by_state['State']],
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor=color,
                        font=dict(size=11, color="black", family="Arial")
                    )
                )

                colorbar_title = "Net per Million" if store_type == "Net" else f"{store_type} Stores per Million"
                fig_opened_map.update_coloraxes(
                    colorbar_title=colorbar_title
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

                # State annotations (with hover disabled)
                excluded_states = detect_overlapping_states(mapped_opened_by_state, state_centers)
                states_to_annotate = mapped_opened_by_state[~mapped_opened_by_state['State'].isin(list(excluded_states))]

                if store_type == "Net":
                    # For Net stores, use black text for very negative values, white otherwise
                    text_colors = [
                        "black" if val < -150 else "white"
                        for state, val in zip(mapped_opened_by_state["State"], mapped_opened_by_state["Opened Stores per Capita"])
                        if state in state_abbreviations
                    ]
                else:
                    # For non-Net stores, use black text
                    text_colors = ["black"] * len(mapped_opened_by_state)
                
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
                if column:
                    with column:
                        # Add end month to title for Active page
                        end_date_str = data['Period'].max().strftime('%B %Y')
                        title_suffix = f" ({end_date_str})" if store_type == "Active" else ""
                        st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>{store_type} Stores per Capita by State (per Million){title_suffix}</h4>", unsafe_allow_html=True)
                        st.plotly_chart(fig_opened_map, use_container_width=True)
                else:
                    # Add end month to title for Active page
                    end_date_str = data['Period'].max().strftime('%B %Y')
                    title_suffix = f" ({end_date_str})" if store_type == "Active" else ""
                    st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>{store_type} Stores per Capita by State (per Million){title_suffix}</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_opened_map, use_container_width=True)
        except Exception as e:
            if column:
                with column:
                    st.write(f"Error creating per capita map: {e}")
            else:
                st.write(f"Error creating per capita map: {e}")
                
    @staticmethod
    def render_opened_sectors_bar_chart(data, column=None, store_type="Opened", color="#A3C0CE"):
        """Render the opened sectors bar chart"""
        try:
            if store_type == "Net":
                opened_by_sector = data[data['data_from'] == 'opened']['Sector_Coresight'].value_counts()
                closed_by_sector = data[data['data_from'] == 'closed']['Sector_Coresight'].value_counts()
                opened_store_counts = opened_by_sector.subtract(closed_by_sector, fill_value=0).astype(int).sort_values(ascending=False).head(15).dropna()
            else:
                opened_store_counts = data['Sector_Coresight'].value_counts().head(15).dropna()
            if opened_store_counts.empty:
                if column:
                    with column:
                        st.markdown(
                            "<h4 style='font-size: 20px;text-align: center; top-align: center; color: red;'>By Sector data not available</h4>",
                            unsafe_allow_html=True
                        )
                else:
                    st.markdown(
                        "<h4 style='font-size: 20px;text-align: center; top-align: center; color: red;'>By Sector data not available</h4>",
                        unsafe_allow_html=True
                    )
            else:
                # Calculate y-axis limits to accommodate negative values for Net stores
                if store_type == "Net":
                    y_max_opened = UIComponents.calculate_yaxis_limit(opened_store_counts.max())
                    y_min_opened = UIComponents.calculate_yaxis_limit(abs(opened_store_counts.min()))
                    y_range = max(y_max_opened or 0, y_min_opened or 0) or 1
                else:
                    y_max_opened = UIComponents.calculate_yaxis_limit(opened_store_counts.max())
                    y_range = y_max_opened or 1

                # Calculate dynamic date period
                start_date = data['Period'].min()
                end_date = data['Period'].max()
                start_date_str = start_date.strftime("%b %Y")
                end_date_str   = end_date.strftime("%b %Y")

                date_period = f"{start_date_str} to {end_date_str}"

                # Calculate metadata for the entire dataset
                num_banners = data['ChainName_Coresight'].nunique()
                num_states = data['State'].nunique()
                num_msa = data['MsaName'].nunique()

                # Create figure with consistent styling
                fig_opened_bar = go.Figure()

                fig_opened_bar.add_trace(go.Bar(
                    x=opened_store_counts.index,
                    y=opened_store_counts.values,
                    marker_color=color,
                    text=[f"{x:,}" for x in opened_store_counts.values],
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"  # Sector name
                        f"<span style='color:{color}'>•</span> "
                        f"<b>{store_type} Stores:</b> %{{y:,}}<br><br>"
                        f"<span style='color:{color}'>•</span> "
                        f"<span style='color:black'>Date Period: {date_period}</span><br>"
                        f"<span style='color:{color}'>•</span> "
                        f"<span style='color:black'>Number of Banners: {num_banners:,}</span><br>"
                        f"<span style='color:{color}'>•</span> "
                        f"<span style='color:black'>Number of States: {num_states:,}</span><br>"
                        f"<span style='color:{color}'>•</span> "
                        f"<span style='color:black'>Number of MSA: {num_msa:,}</span><br>"
                        "<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor=color,
                        font=dict(size=11, color="black", family="Arial")
                    )
                ))

                fig_opened_bar.update_layout(
                    xaxis_title="Sector",
                    yaxis_title=f"{store_type} Stores",
                    yaxis=dict(
                        range=[-y_range if store_type == 'Net' else 0, y_range],
                        autorange=True,
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
                if column:
                    with column:
                        st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {opened_store_counts.shape[0]} Sectors</h4>", unsafe_allow_html=True)
                        st.plotly_chart(fig_opened_bar, use_container_width=True)
                else:
                    st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>Top {opened_store_counts.shape[0]} Sectors</h4>", unsafe_allow_html=True)
                    st.plotly_chart(fig_opened_bar, use_container_width=True)
        except Exception as e:
            if column:
                with column:
                    st.write(f"Error creating sectors chart: {e}")
            else:
                st.write(f"Error creating sectors chart: {e}")
    
    @staticmethod
    def render_square_footage_over_time_chart(data, column=None, color="#A3C0CE",chart_title="Opened"):
        print("iiiiii",chart_title)
        """Render total square footage opened over time (monthly)"""
        try:
            print("🔍 DEBUG: Starting render_square_footage_over_time_chart")
            print(f"🔍 DEBUG: Input data shape: {data.shape}")
            print(f"🔍 DEBUG: Input columns: {list(data.columns)}")
 
            if 'square_footage' not in data.columns or 'Period' not in data.columns:
                raise ValueError("Required columns 'square_footage' or 'Period' missing")
 
            data = data.copy()
            # Ensure square_footage is numeric
            data['square_footage'] = pd.to_numeric(data['square_footage'], errors='coerce').fillna(0)
            print(f"🔍 DEBUG: square_footage dtype after conversion: {data['square_footage'].dtype}")
            print(f"🔍 DEBUG: Sample square_footage values: {data['square_footage'].head().tolist()}")
 
            data['PeriodMonth'] = data['Period'].dt.to_period('M').dt.to_timestamp()
            print(f"🔍 DEBUG: Added PeriodMonth. Sample: {data['PeriodMonth'].head().tolist()}")
 
            # Aggregate square footage and store counts by PeriodMonth
            sqft_over_time = data.groupby('PeriodMonth')['square_footage'].sum().reset_index()
            store_counts = data.groupby('PeriodMonth').size().reset_index(name='StoreCount')
 
            print(f"🔍 DEBUG: sqft_over_time columns: {list(sqft_over_time.columns)}")
            print(f"🔍 DEBUG: store_counts columns: {list(store_counts.columns)}")
            print(f"🔍 DEBUG: sqft_over_time sample:\n{sqft_over_time.head()}")
            print(f"🔍 DEBUG: store_counts sample:\n{store_counts.head()}")
 
            # Merge BEFORE renaming the key column
            combined = sqft_over_time.merge(store_counts, on='PeriodMonth', how='left')
            print(f"🔍 DEBUG: After merge, combined columns: {list(combined.columns)}")
            print(f"🔍 DEBUG: Combined sample:\n{combined.head()}")
 
            combined.rename(columns={'PeriodMonth': 'MonthStart'}, inplace=True)
            print(f"🔍 DEBUG: After rename, columns: {list(combined.columns)}")
            
            num_sectors_list = []
            num_banners_list = []
            num_states_list = []
            num_msa_list = []

            for month in combined['MonthStart']:
                month_data = data[data['PeriodMonth'] == month]
                num_sectors_list.append(month_data['Sector_Coresight'].nunique())
                num_banners_list.append(month_data['ChainName_Coresight'].nunique())
                num_states_list.append(month_data['State'].nunique())
                num_msa_list.append(month_data['MsaName'].nunique())
            
            # Prepare text labels safely - convert to k format
            from sip_components.tab_components import format_large_number
            text_labels = [format_large_number(x) for x in combined['square_footage']]
            print(f"🔍 DEBUG: Generated text labels (first 5): {text_labels[:5]}")
 
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=combined['MonthStart'],
                y=combined['square_footage'],
                mode='lines+markers+text',
                name='Total Square Footage',
                line=dict(color=color, width=2),
                marker=dict(size=7),
                text=text_labels,
                textposition='top center',
                hovertemplate=(
                    "<b>%{x|%b %Y}</b><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<b>Total Square Footage:</b> %{y:,.1f}<br>"
                    f"<span style='color:{color}'>•</span> "
                    "<b>Stores Opened:</b> %{customdata[4]} stores<br><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of MSA: %{customdata[3]}</span><br>"
                    "<extra></extra>"
                ),
                customdata=list(zip(num_sectors_list, num_banners_list, num_states_list, num_msa_list, combined['StoreCount'].astype(str))),
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor=color,
                    font=dict(size=11, color="black", family="Arial")
                )
            ))
 
            fig.update_layout(
                title=f"Total Square Footage {chart_title} Over Time",
                yaxis_title="Square Footage",
                xaxis=dict(tickformat="%b %Y", title="Month"),
                height=400,
                margin=dict(l=10, r=10, t=60, b=40),
                showlegend=False
            )
 
            config = {
                'displayModeBar': True,
                'displaylogo': False,
                'toImageButtonOptions': {
                    'format': 'png',
                    'filename': 'square_footage_over_time',
                    'height': 400,
                    'width': 700,
                    'scale': 1
                }
            }
 
            if column:
                with column:
                    st.plotly_chart(fig, use_container_width=True, config=config)
            else:
                st.plotly_chart(fig, use_container_width=True, config=config)
 
            print("✅ DEBUG: Chart rendered successfully!")
 
        except Exception as e:
            import traceback
            error_msg = f"Square Footage Time Chart Error: {e}"
            print("❌ DEBUG ERROR:", error_msg)
            print("❌ FULL TRACEBACK:")
            traceback.print_exc()
            if column:
                with column:
                    st.error(error_msg)
            else:
                st.error(error_msg)
 
    @staticmethod
    def render_square_footage_by_city_chart(data, column=None, color="#A3C0CE"):
        """Render top cities by total square footage opened"""
        try:
            print("🔍 DEBUG: Starting render_square_footage_by_city_chart")
            print(f"🔍 DEBUG: Input data shape: {data.shape}")
            print(f"🔍 DEBUG: Input columns: {list(data.columns)}")
 
            if 'square_footage' not in data.columns or 'City' not in data.columns:
                raise ValueError("Required columns 'square_footage' or 'City' missing")
 
            # Ensure square_footage is numeric
            data = data.copy()
            data['square_footage'] = pd.to_numeric(data['square_footage'], errors='coerce').fillna(0)
            print(f"🔍 DEBUG: square_footage dtype after conversion: {data['square_footage'].dtype}")
            print(f"🔍 DEBUG: Sample square_footage values: {data['square_footage'].head().tolist()}")
 
            # Aggregate by City
            sqft_by_city = data.groupby('City')['square_footage'].sum().sort_values(ascending=False).head(15)
            print(f"🔍 DEBUG: sqft_by_city top 5:\n{sqft_by_city.head()}")
 
            # Prepare store counts per city (as strings for hover)
            city_store_counts = []
            for city in sqft_by_city.index:
                count = data[data['City'] == city].shape[0]
                city_store_counts.append(str(count))
            print(f"🔍 DEBUG: Generated store counts (first 5): {city_store_counts[:5]}")
            
            city_metadata = {}
            for city in sqft_by_city.index:
                city_data = data[data['City'] == city]
                city_metadata[city] = {
                    'num_sectors': city_data['Sector_Coresight'].nunique(),
                    'num_banners': city_data['ChainName_Coresight'].nunique(),
                    'num_states': city_data['State'].nunique(),
                    'num_msa': city_data['MsaName'].nunique()
                }

            # Calculate overall date range
            start_date = data['Period'].min()
            end_date   = data['Period'].max()
            start_date_str = start_date.strftime("%B %Y")
            end_date_str   = end_date.strftime("%B %Y")
            date_period = f"{start_date_str} to {end_date_str}"
 
            # Prepare text labels safely
            from sip_components.tab_components import format_large_number
            text_labels = [format_large_number(x) for x in sqft_by_city.values]
            print(f"🔍 DEBUG: Generated text labels (first 5): {text_labels[:5]}")
 
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=sqft_by_city.index,
                y=sqft_by_city.values,
                marker_color=color,
                text=text_labels,
                textposition='outside',
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<b>Total Square Footage:</b> %{y:,.1f}<br>"
                    f"<span style='color:{color}'>•</span> "
                    "<b>Stores:</b> %{customdata[4]} stores<br>"
                    "<br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Date Period: " + f"{date_period}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                    f"<span style='color:{color}'>•</span> "
                    "<span style='color:black'>Number of MSA: %{customdata[3]}</span><br>"
                    "<extra></extra>"
                ),
                customdata=[[city_metadata[city]['num_sectors'], city_metadata[city]['num_banners'], city_metadata[city]['num_states'], city_metadata[city]['num_msa'], city_store_counts[i]] for i, city in enumerate(sqft_by_city.index)],
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor=color,
                    font=dict(size=11, color="black", family="Arial")
                )
            ))
 
            fig.update_layout(
                title="Top Cities by Square Footage",
                xaxis_title="City",
                yaxis_title="Square Footage",
                xaxis=dict(tickangle=45),
                height=450,
                margin=dict(l=20, r=20, t=60, b=60),
                showlegend=False
            )
 
            if column:
                with column:
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.plotly_chart(fig, use_container_width=True)
 
            print("✅ DEBUG: City Square Footage Chart rendered successfully!")
 
        except Exception as e:
            import traceback
            error_msg = f"Square Footage by City Chart Error: {e}"
            print("❌ DEBUG ERROR:", error_msg)
            print("❌ FULL TRACEBACK:")
            traceback.print_exc()
            if column:
                with column:
                    st.error(error_msg)
            else:
                st.error(error_msg)
 
 
    def add_square_footage_column(self,filtered_data, square_footage_data):
        """
        Adds a 'square_footage' column to filtered_data by mapping ChainName_Coresight
        to Average_Square_Footage from square_footage_data.
        
        If a ChainName is not found, defaults to 0.
        """
        # Create a mapping dict: ChainName_Coresight -> Average_Square_Footage
        sqft_map = dict(zip(
            square_footage_data['ChainName_Coresight'],
            square_footage_data['Average_Square_Footage']
        ))
        
        # Map to filtered_data; default to 0 if not found
        filtered_data = filtered_data.copy()
        filtered_data['square_footage'] = filtered_data['ChainName_Coresight'].map(sqft_map).fillna(0)
        
        return filtered_data

    @staticmethod
    def render_data_disclaimer(latest_ts, column=None):
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

        st.markdown(f"<i style='margin-top: 10px;'>Data available through <b>{latest_ts}</b></i>",unsafe_allow_html=True)
        st.markdown(
            f"""
            <p style='color: gray; font-size: small;'>
            Disclaimer: Certain data are derived from calculations that use data licensed from third parties, including ChainXY. 
            Coresight Research has made substantial efforts to clean the data and identify potential issues. However, changes to retailers' store locators 
            may impact database-sourced data. See our 
            <a href="{overview_url}" target="_blank">Overview/Retailer List</a> 
            document and <a href="/changelogs#store-intelligence-platform" target="_self">Data Release Notes</a>
            for more details.
            </p>
            """,
            unsafe_allow_html=True,
        )
        st.empty()


 