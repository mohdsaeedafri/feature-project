"""
TabComponents - Common tab structure, navigation, and standardized layouts
"""
from numpy import char
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import logging
import colorsys
from datetime import datetime, timedelta
from sip_components.ui_components import UIComponents

class TabComponents:
    @staticmethod
    def render_tab_navigation(current_page="net"):
        """Render the common tab navigation with Store Intelligence Platform heading"""
        
        # Create two columns for the heading and navigation
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # Title of the app
            st.markdown("<h1 style='font-size: 40px; text-align: left; padding-top: 0px; padding-bottom: 0px;'>Store Intelligence Platform</h1>", unsafe_allow_html=True)
        
        with col2:
            # Determine which tab should have the active class
            active_net = "active" if current_page == "net" else ""
            active_opening = "active" if current_page == "opening" else ""
            active_closing = "active" if current_page == "closing" else ""
            active_active = "active" if current_page == "active" else ""
            
            st.markdown(f"""
            <style>
            .tab-links {{
                display: flex;
                justify-content: flex-end;
                gap: 2px;
            }}
            .tab-links a {{
                text-decoration: none;
                padding: 8px 16px;
                background-color: #d0d0d0;
                color: black;
                border-radius: 5px;
                font-size: 16px;
            }}
            .tab-links a:hover {{
                background-color: #767779;
            }}
            .tab-links a.active {{
                background-color: #d62e2f;
                color: white;
            }}
            .button-container {{
                display: flex;
                flex-direction: column;
                width: 100%;
            }}
            </style>
            <div class="button-container">
                <div class="tab-links">
                    <a href="/net" target="_self" class="{active_net}">Net Openings</a>
                    <a href="/opening" target="_self" class="{active_opening}">Store Openings</a>
                    <a href="/closing" target="_self" class="{active_closing}">Store Closures</a>
                    <a href="/active" target="_self" class="{active_active}">Active Stores</a>
                </div>
            </div>
            """, unsafe_allow_html=True)
    # def render_tab_navigation(current_page="net"):
    #     """Render the common tab navigation with Store Intelligence Platform heading"""
    #     tabs = ["Net", "Opening", "Closing", "Active"]
    #     tab_names = {
    #         "Net": "Net Openings",
    #         "Opening": "Store Openings",
    #         "Closing": "Store Closures",
    #         "Active": "Active Stores"
    #     }
    #     tab_links = {
    #         "Net": "net.py",
    #         "Opening": "opening.py",
    #         "Closing": "closing.py",
    #         "Active": "active.py"
    #     }
        
    #     # # For refactored pages, use the refactored versions
    #     # if current_page == "opening":
    #     #     tab_links["Opening"] = "opening.py"
    #     # elif current_page == "closing":
    #     #     tab_links["Closing"] = "closing.py"
    #     # elif current_page == "active":
    #     #     tab_links["Active"] = "active.py"
    #     # elif current_page == "net":
    #     #     tab_links["Net"] = "net.py"
            
    #     # current_tab = current_page.capitalize()
    #     # print("current_tab", current_tab,"tabs",tabs)
    #     # tab_html = "<div class='tab-links'>"
    
    #     # for tab in tabs:
    #     #     if tab == current_tab:
    #     #         tab_html += f"<a href='{tab_links[tab]}' class='active' target='_self'>{tab_names[tab]}</a>"
    #     #     else:
    #     #         tab_html += f"<a href='{tab_links[tab]}' target='_self'>{tab_names[tab]}</a>"
    #     # tab_html += "</div>"
        
    #     # Create two columns for the heading and navigation
    #     col1, col2 = st.columns([1, 1])
        
    #     with col1:
    #         # Title of the app
    #         st.markdown("<h1 style='font-size: 40px; text-align: left; padding-top: 0px; padding-bottom: 0px;'>Store Intelligence Platform</h1>", unsafe_allow_html=True)
    #     with col2:
    #         st.markdown("""
    #         <style>
    #         .tab-links {
    #             display: flex;
    #             justify-content: flex-end;
    #             gap: 2px;
    #         }
    #         .tab-links a {
    #             text-decoration: none;
    #             padding: 8px 16px;
    #             background-color: #d0d0d0;
    #             color: black;
    #             border-radius: 5px;
    #             font-size: 16px;
    #         }
    #         .tab-links a:hover {
    #             background-color: #767779;
    #         }
    #         .tab-links a.active {
    #             background-color: #d62e2f; /* Active tab color (dark) */
    #             color: white; /* Active tab text color */
    #         }.button-container {
    #             display: flex;
    #             flex-direction: column;
    #             width: 100%;
    #         }
    #         </style>
    #         <div class="button-container">
    #             <div class="tab-links">
    #                 <a href="/net" target="_self" class="active">Net Openings</a>
    #                 <a href="/opening" target="_self">Store Openings</a>
    #                 <a href="/closing" target="_self">Store Closures</a>
    #                 <a href="/active" target="_self">Active Stores</a>
    #             </div>
    #         </div>
    #         """, unsafe_allow_html=True)
    #     # with col2:
    #     #     st.markdown(f"""
    #     #     <style>
    #     #     .tab-links {{
    #     #         display: flex;
    #     #         justify-content: flex-end;
    #     #         gap: 2px;
    #     #     }}
    #     #     .tab-links a {{
    #     #         text-decoration: none;
    #     #         padding: 8px 16px;
    #     #         background-color: #d0d0d0;
    #     #         color: black;
    #     #         border-radius: 5px;
    #     #         font-size: 16px;
    #     #     }}
    #     #     .tab-links a:hover {{
    #     #         background-color: #767779;
    #     #     }}
    #     #     .tab-links a.active {{
    #     #         background-color: #d62e2f; /* Active tab color (dark) */
    #     #         color: white; /* Active tab text color */
    #     #     }}
    #     #     .button-container {{
    #     #         display: flex;
    #     #         flex-direction: column;
    #     #         width: 100%;
    #     #     }}
    #     #     </style>
    #     #     <div class="button-container">
    #     #         {tab_html}
    #     #     </div>
    #     #     """, unsafe_allow_html=True)
        
    @staticmethod
    def render_tab_selector(default_tab="Base Dashboard"):
        """Render the tab selector for dashboard views"""
        top_col1, top_col2, top_col3 = st.columns([78, 10, 12])
        
        with top_col1:
            tabs = ["Base Dashboard", "Compare Retailers", "Compare Sectors"]
            if "selected_tab" not in st.session_state or not st.session_state["selected_tab"]:
                st.session_state["selected_tab"] = default_tab

            selected_tab = st.radio(
                "Analysis", 
                tabs, 
                index=tabs.index(st.session_state["selected_tab"]),
                horizontal=True, 
                label_visibility="collapsed",
            )

            if selected_tab != st.session_state["selected_tab"]:
                st.session_state["selected_tab"] = selected_tab
                st.query_params["tab"] = selected_tab
                st.rerun()
                
        return selected_tab, top_col2, top_col3
        
    @staticmethod
    def render_filter_controls(expand_filters_key="expand_filters"):
        """Render the filter control buttons"""
        all_expanded = st.session_state.get("sector_comparison_expand_filters", True) and \
                       st.session_state.get("retailer_comparison_expand_filters", True) and \
                       st.session_state.get(expand_filters_key, True)
        toggle_label = "Close Filters" if all_expanded else "Expand Filters"

        if st.button(toggle_label, key="all_filters_btn", use_container_width=True):
            new_state = not all_expanded
            st.session_state["sector_comparison_expand_filters"] = new_state
            st.session_state["retailer_comparison_expand_filters"] = new_state
            st.session_state[expand_filters_key] = new_state
            st.rerun()
            
    @staticmethod
    def render_reset_button(cookie_controller, auth_cookie, data, date_range_key="selected_date_range"):
        """Render the reset filters button"""
        if st.button("Reset Filters", key="clear_all_filters_btn", use_container_width=True, type="secondary"):
            logging.info("Reset Filters button clicked - resetting all filters to default")
            
            # Calculate default date range
            max_period = data['Period'].max()
            
            if hasattr(max_period, 'date'):
                max_date = max_period.date()
            else:
                max_date = max_period  # Fixed the unbound variable error
                
            default_end_date = max_date
            
            if hasattr(max_date, 'replace'):
                start_date_temp = max_date.replace(day=1)
                default_start_date = (pd.to_datetime(start_date_temp) - pd.DateOffset(months=11)).date()
            else:
                default_start_date = (pd.to_datetime(max_date) - pd.DateOffset(months=11)).date()
            
            # Reset date range
            st.session_state["start_month"] = default_start_date.month
            st.session_state["start_year"] = default_start_date.year
            st.session_state["end_month"] = default_end_date.month
            st.session_state["end_year"] = default_end_date.year
            st.session_state[date_range_key] = (default_start_date, default_end_date)
            
            # Reset filter selections
            user_filters = auth_cookie.get("filters", {}) if isinstance(auth_cookie, dict) else {}
            user_filters["selected_sector_name"] = "All"
            user_filters["selected_chain_name"] = ["All"]
            user_filters["parent_chain_name"] = ["All"]
            user_filters["selected_state_name"] = ["All"]
            user_filters["selected_msa_name"] = ["All"]
            user_filters["selected_zip_code"] = ["All"]
            
            # Reset comparison tab specific filters
            st.session_state["selected_parent_names_v2"] = []
            st.session_state["selected_chain_names_v2"] = []
            st.session_state["selected_state_v2"] = ["All"]
            st.session_state["selected_msa_v2"] = ["All"]
            st.session_state["selected_zip_v2"] = ["All"]
            
            st.session_state["sector_comparison_selected_sectors"] = []
            st.session_state["sector_compare_parent_chain_name"] = []
            st.session_state["sector_compare_selected_chain_name"] = []
            st.session_state["sector_compare_selected_state_name"] = ["All"]
            st.session_state["sector_compare_selected_msa_name"] = ["All"]
            st.session_state["sector_compare_selected_zip_name"] = ["All"]
            
            # Save to cookie
            try:
                cookie_controller.set(
                    "auth_data",
                    json.dumps(auth_cookie),
                    expires=datetime.utcnow() + timedelta(days=30),
                    path="/",
                    domain="",  # Will be set by get_current_domain in real implementation
                )
            except Exception as e:
                logging.warning(f"cookie write failed: {e}")
            
            st.rerun()
            
    @staticmethod
    def render_metrics_section(filtered_data, population_column='Population', metrics_title="Opened", previous_data=None, show_extended_metrics=True, is_active_page=False, filtered_data_recent=None, square_footage_data=None, chain_name_selected=None, parent_chain_name_selected=None,active_sq_footage=None):
        """Render the metrics section with standardized cards"""
        if show_extended_metrics and is_active_page:
            col1, col2, col3, col4, col5= st.columns(5)
        elif show_extended_metrics:
            # 6 columns for Base Dashboard (with extended metrics including square footage)
            col1, col2, col3, col4, col5, col6 = st.columns(6)
        else:
            # 3 columns for Compare tabs (without extended metrics)
            col1, col2, col3, col4 = st.columns(4)
            col5, col6 = None, None
        
        # Calculate metrics
        # For Active page, use filtered_data_recent (end month only) for total count
        if is_active_page and filtered_data_recent is not None and len(filtered_data_recent) > 0:
            total_stores = len(filtered_data_recent)
        else:
            total_stores = len(filtered_data)
        
        total_retailers = filtered_data['ChainName_Coresight'].nunique() if 'ChainName_Coresight' in filtered_data.columns else 0
        total_sectors = filtered_data['Sector_Coresight'].nunique() if 'Sector_Coresight' in filtered_data.columns else 0
        formatted_square_footage = "N/A"
        if square_footage_data is not None and not square_footage_data.empty:
                    if active_sq_footage:
                        filtered_data = filtered_data_recent
                    try:
                        # Normalize types upfront
                        square_footage_data['Average_Square_Footage'] = pd.to_numeric(
                            square_footage_data['Average_Square_Footage'], errors='coerce'
                        )
 
                        # ------------ 1) Decide mode: chain vs parent ------------
                        # chain_name_selected is an array/list; ignore 'All' and empties
                        selected_chains = []
                        if chain_name_selected:
                            selected_chains = [c for c in chain_name_selected if c and c != 'All']
 
                        # We'll only use parent selection if NO specific chains were chosen
                        use_chain_mode = len(selected_chains) > 0
                        # ------------ 2) CHAIN MODE ------------
                        if use_chain_mode:
                            # Build Chain -> AvgSqFt map (unique per your data)
                            avg_map_chain = (
                                square_footage_data
                                .dropna(subset=['ChainName_Coresight'])
                                .set_index('ChainName_Coresight')['Average_Square_Footage']
                            )
 
                            # Keep only chains that exist in the avg map
                            selected_chains = [c for c in selected_chains if c in avg_map_chain.index]
 
                            if selected_chains:
                                # Count opened stores per chain from filtered_data
                                counts_chain = (
                                    filtered_data[filtered_data['ChainName_Coresight'].isin(selected_chains)]
                                    .value_counts('ChainName_Coresight')
                                    .rename('store_count')
                                    .rename_axis('ChainName_Coresight')
                                    .reset_index()
                                )
 
                                # Attach avg sqft and compute total
                                counts_chain['Average_Square_Footage'] = counts_chain['ChainName_Coresight'].map(avg_map_chain)
                                counts_chain['Average_Square_Footage'] = pd.to_numeric(counts_chain['Average_Square_Footage'], errors='coerce').fillna(0)
                                counts_chain['store_count'] = pd.to_numeric(counts_chain['store_count'], errors='coerce').fillna(0)
 
                                total_square_footage = float((counts_chain['Average_Square_Footage'] * counts_chain['store_count']).sum())
                            else:
                                total_square_footage = 0.0
 
                        # ------------ 3) PARENT MODE ------------
                        else:
                            # Parent selection array; ignore 'All' and empties
                            selected_parents = []
                            if parent_chain_name_selected:
                                selected_parents = [p for p in parent_chain_name_selected if p and p != 'All']
 
                            # If nothing explicitly selected (or only 'All'), default to all parents present in filtered_data
                            if not selected_parents:
                                selected_parents = (
                                    filtered_data['ParentName_Coresight']
                                    .dropna()
                                    .unique()
                                    .tolist()
                                )
 
                            # Build Parent -> (SUM of AvgSqFt across all chains under that parent)
                            parent_avg_map = (
                                square_footage_data
                                .dropna(subset=['ParentName_Coresight'])
                                .groupby('ParentName_Coresight', as_index=True)['Average_Square_Footage']
                                .sum(min_count=1)  # if all NaN -> NaN
                                .fillna(0)
                            )
                            # Keep only parents that exist in parent_avg_map
                            selected_parents = [p for p in selected_parents if p in parent_avg_map.index]
 
                            if selected_parents:
                                # Count opened stores per parent from filtered_data
                                counts_parent = (
                                    filtered_data[filtered_data['ParentName_Coresight'].isin(selected_parents)]
                                    .value_counts('ParentName_Coresight')
                                    .rename('store_count')
                                    .rename_axis('ParentName_Coresight')
                                    .reset_index()
                                )
 
                                # Attach summed parent avg sqft and compute total
                                counts_parent['Average_Square_Footage'] = counts_parent['ParentName_Coresight'].map(parent_avg_map)
                                counts_parent['Average_Square_Footage'] = pd.to_numeric(counts_parent['Average_Square_Footage'], errors='coerce').fillna(0)
                                counts_parent['store_count'] = pd.to_numeric(counts_parent['store_count'], errors='coerce').fillna(0)
 
                                # Example:
                                # Tractor Supply Company has rows:
                                #   Tractor Supply Company | Tractor Supply | 35
                                #   Tractor Supply Company | Petsense       | 5.5
                                # parent_avg_map['Tractor Supply Company'] = 35 + 5.5 = 40.5
                                # total += 40.5 * (opened store count for that parent)
                                total_square_footage = float((counts_parent['Average_Square_Footage'] * counts_parent['store_count']).sum())
                            else:
                                total_square_footage = 0.0
 
                        # ------------ 4) Format metric ------------
                        # formatted_square_footage = f"{total_square_footage:,.0f}" if total_square_footage > 0 else "N/A"
                        if total_square_footage > 0:
                            # Convert to proper format (if it's already in thousands, we multiply by 1000 for display)
                            actual_square_footage = total_square_footage * 1000
                            formatted_square_footage = UIComponents.format_large_number(actual_square_footage)
                        else:
                            formatted_square_footage = "N/A"

 
                    except Exception as e:
                        print(f"Error calculating square footage: {str(e)}")
                        formatted_square_footage = "N/A"
        if metrics_title == "Net":
            return formatted_square_footage
        # Render metric cards using UIComponents
        
        ui = UIComponents()
        ui.render_metric_card(f"Total {metrics_title} Stores", total_stores, column=col1)
        ui.render_metric_card("Total Affected Banners", total_retailers, column=col2)
        ui.render_metric_card("Total Affected Sectors", total_sectors, column=col3)
        if not show_extended_metrics and square_footage_data is not None and not square_footage_data.empty:
            ui.render_metric_card(f"{metrics_title} Square Footage", formatted_square_footage, column=col4)
        
        # Only show extended metrics in Base Dashboard
        if show_extended_metrics:
            # For Active page, skip "% Change Compared to Previous Period" and show stores per 10k
            if is_active_page:
                # Calculate Stores per 10,000 people
                total_population = filtered_data[population_column].sum() if population_column in filtered_data.columns else 0
                if total_population > 0:
                    stores_per_10k = (total_stores / total_population) * 10000
                    stores_per_10k_display = f"{stores_per_10k:.3f}"
                else:
                    stores_per_10k_display = "N/A"
                
                ui.render_metric_card(f"{metrics_title} Stores per 10,000 people", stores_per_10k_display, column=col4)
                ui.render_metric_card(f"{metrics_title} Square Footage", formatted_square_footage, column=col5)
            else:
                # For Opening/Closing, show % Change Compared to Previous Period
                # Calculate % Change Compared to Previous Period
                if previous_data is not None and len(previous_data) > 0:
                    previous_stores = len(previous_data)
                    if previous_stores > 0:
                        percent_change = ((total_stores - previous_stores) / previous_stores) * 100
                        percent_display = f"{percent_change:.1f}%"
                    else:
                        percent_display = "N/A"
                else:
                    percent_display = "N/A"
                
                ui.render_metric_card("% Change Compared to Previous Period", percent_display, column=col4)
                
                # Calculate Stores per 10,000 people
                total_population = filtered_data[population_column].sum() if population_column in filtered_data.columns else 0
                if total_population > 0:
                    stores_per_10k = (total_stores / total_population) * 10000
                    stores_per_10k_display = f"{stores_per_10k:.3f}"
                else:
                    stores_per_10k_display = "N/A"
                
                ui.render_metric_card(f"{metrics_title} Stores per 10,000 people", stores_per_10k_display, column=col5)
 
                ui.render_metric_card(f"{metrics_title} Square Footage", formatted_square_footage, column=col6)
 
        
        return total_stores, total_retailers, total_sectors
        
        
    @staticmethod
    def render_standard_charts(filtered_data, filtered_data_recent="", chart_title_prefix=""):
        """Render the standard chart section with time series and top retailers"""
        from sip_components.ui_components import UIComponents
        from sip_components.styling_manager import StylingManager
        ui = UIComponents()
        styling = StylingManager()

        
        # Render horizontal line
        styling.render_horizontal_line()
        
        # Create chart columns
        chart_col1, chart_col2 = st.columns([7, 3])
        
        # Render charts using UI components
        # For opening page, use opening-specific charts
        if chart_title_prefix == "Opened":
            ui.render_opened_stores_over_time_chart(filtered_data, column=chart_col1, store_type="Opened", color="#A3C0CE", store_label="Store Opened Count")
            ui.render_opened_chains_bar_chart(filtered_data, column=chart_col2, store_type="Opened", color="#A3C0CE")
        elif chart_title_prefix == "Closed":
            ui.render_opened_stores_over_time_chart(filtered_data, column=chart_col1, store_type="Closed", color="#d62e2f", store_label="Store Closed Count")
            ui.render_opened_chains_bar_chart(filtered_data, column=chart_col2, store_type="Closed", color="#d62e2f")
        elif chart_title_prefix == "Active":
            # For Active page - use the same rendering method as Opening/Closing
            ui.render_opened_stores_over_time_chart(filtered_data, column=chart_col1, store_type="Active", color="#CBCACA", store_label="Active Store Count")
            ui.render_active_chains_bar_chart(filtered_data, column=chart_col2)
        else:   
            print("Invalid chart_title_prefix:")
        # Render additional charts for opening and closing pages
        if chart_title_prefix == "Opened":
            # Render state maps
            map_col1, map_col2 = st.columns([7, 3])
            ui.render_opened_stores_by_state_map(filtered_data, column=map_col1, store_type="Opened", color="#A3C0CE")
            ui.render_opened_cities_bar_chart(filtered_data, column=map_col2, store_type="Opened", color="#A3C0CE")
            
            # Render per capita map and sectors chart
            per_capita_col1, per_capita_col2 = st.columns([7, 3])
            ui.render_opened_stores_per_capita_map(filtered_data, column=per_capita_col1, store_type="Opened", color="#A3C0CE")
            ui.render_opened_sectors_bar_chart(filtered_data, column=per_capita_col2, store_type="Opened", color="#A3C0CE")
        elif chart_title_prefix == "Closed":
            # Render state maps
            map_col1, map_col2 = st.columns([7, 3])
            ui.render_opened_stores_by_state_map(filtered_data, column=map_col1, store_type="Closed", color="#d62e2f")
            ui.render_opened_cities_bar_chart(filtered_data, column=map_col2, store_type="Closed", color="#d62e2f")
            
            # Render per capita map and sectors chart
            per_capita_col1, per_capita_col2 = st.columns([7, 3])
            ui.render_opened_stores_per_capita_map(filtered_data, column=per_capita_col1, store_type="Closed", color="#d62e2f")
            ui.render_opened_sectors_bar_chart(filtered_data, column=per_capita_col2, store_type="Closed", color="#d62e2f")
        elif chart_title_prefix == "Active":
            # Render state maps
            map_col1, map_col2 = st.columns([7, 3])
            ui.render_opened_stores_by_state_map(filtered_data_recent, column=map_col1, store_type="Active", color="#CBCACA")
            ui.render_opened_cities_bar_chart(filtered_data_recent, column=map_col2, store_type="Active", color="#CBCACA") 

            # Add spacing between sections
            st.markdown("<div style='margin-top: 50px;'></div>", unsafe_allow_html=True)

            # Render per capita map and sectors chart
            per_capita_col1, per_capita_col2 = st.columns([7, 3])
            ui.render_opened_stores_per_capita_map(filtered_data_recent, column=per_capita_col1, store_type="Active", color="#CBCACA")
            ui.render_opened_sectors_bar_chart(filtered_data_recent, column=per_capita_col2,color="#CBCACA", store_type="Active") 
        elif chart_title_prefix == "Net":
            map_col1, map_col2 = st.columns([7, 3])
            ui.render_opened_stores_by_state_map(filtered_data, column=map_col1, store_type="Net", color="#2D2A29")
            ui.render_opened_cities_bar_chart(filtered_data, column=map_col2, store_type="Net", color="#2D2A29")
            
            # Render per capita map and sectors chart
            per_capita_col1, per_capita_col2 = st.columns([7, 3])
            ui.render_opened_stores_per_capita_map(filtered_data, column=per_capita_col1, store_type="Net", color="#2D2A29")
            ui.render_opened_sectors_bar_chart(filtered_data, column=per_capita_col2, store_type="Net", color="#2D2A29")
        if chart_title_prefix == "Opened":
                color="#A3C0CE"
        elif chart_title_prefix == "Closed":
                color="#d62e2f"
        elif chart_title_prefix == "Active":
                color="#CBCACA"
        elif chart_title_prefix == "Net":
                color="#2D2A29"
        if 'square_footage' in filtered_data.columns:
            st.markdown("### Square Footage Analysis")
            sqft_col1, sqft_col2 = st.columns([7, 3])
            ui.render_square_footage_over_time_chart(filtered_data, column=sqft_col1, color=color, chart_title=chart_title_prefix)
            ui.render_square_footage_by_city_chart(filtered_data, column=sqft_col2, color=color)
    @staticmethod
    def render_compare_retailers_charts(filtered_data, net_filtered_data, selected_parent_names, selected_chain_names, chart_title_prefix="Opened"):
        """Render charts for Compare Retailers tab"""
        from sip_components.ui_components import UIComponents
        from sip_components.styling_manager import StylingManager
        ui = UIComponents()
        styling = StylingManager()
        
        # Determine colors and labels based on chart_title_prefix
        if chart_title_prefix == "Closed":
            store_type = "Closed"
            main_color = "#d62e2f"
        else:
            store_type = "Opened"
            main_color = "#A3C0CE"
        
        # Render horizontal line
        styling.render_horizontal_line()
        
        # Create a unified list of retailers for coloring
        color_groups = filtered_data[['ParentName_Coresight', 'ChainName_Coresight']].drop_duplicates()
        group_labels = (
            color_groups['ParentName_Coresight'] + " | " + color_groups['ChainName_Coresight']
        ).tolist()
        palette = px.colors.qualitative.Plotly
        while len(palette) < len(group_labels):  # Loop through palette if not enough colors
            palette += palette
        color_map = {lab: palette[i] for i, lab in enumerate(group_labels)}
        
        # --- LINE CHART ---
        gcol1, gcol2 = st.columns([7, 3])
        with gcol1:
            st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>{store_type} Stores Over Time<br></h4>", unsafe_allow_html=True)
            time_grouped = (
                filtered_data.groupby(['Period', 'ParentName_Coresight', 'ChainName_Coresight'])
                .size().reset_index(name='StoreCount')
            )
            
            metadata_dict = {}
            for period in time_grouped['Period'].unique():
                period_data = filtered_data[filtered_data['Period'] == period]
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
                    y=grp_df['StoreCount'],
                    mode='lines+markers',
                    name=legend_grp,
                    line=dict(color=color, width=2),
                    marker=dict(size=7, color=color),
                    hovertemplate=(
                        "<b>%{x|%b %Y}</b><br>"  # Formatted date
                        "<span style='color:" + str(color) + "'>●</span> "
                        "<b>" + str(legend_grp) + ":</b> %{y:,}<br><br>"
                        "<span style='color:" + str(color) + "'>•</span> "
                        "<span style='color:black'>Number of Sectors: %{customdata[0]}</span><br>"
                        "<span style='color:" + str(color) + "'>•</span> "
                        "<span style='color:black'>Number of Banners: %{customdata[1]}</span><br>"
                        "<span style='color:" + str(color) + "'>•</span> "
                        "<span style='color:black'>Number of States: %{customdata[2]}</span><br>"
                        "<span style='color:" + str(color) + "'>•</span> "
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
                yaxis_title=f"{store_type} Stores",
                xaxis_title="Period",
                height=400,
                showlegend=True,
                legend=dict(
                    x=0.99, y=0.99,
                    xanchor="right",
                    yanchor="top",
                    bgcolor='rgba(0,0,0,0)',
                    bordercolor='rgba(0,0,0,0)',
                    font=dict(size=13),
                    orientation="v"
                ),
                margin=dict(l=10, r=10, t=60, b=10),
                xaxis=dict(
                    tickformat="%b %Y",
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
                'displaylogo': False,
                'toImageButtonOptions': {
                    'format': 'png',
                    'filename': 'opened_stores_over_time_compare_retailers',
                    'height': 400,
                    'width': 700,
                    'scale': 1
                }
            }
            
            st.plotly_chart(fig_line, use_container_width=True, config=config)
            
        # --- BAR CHART ---
        with gcol2:
            st.markdown(f"<h4 style='font-size: 20px;text-align: center;'>{store_type} Stores Counts<br></h4>", unsafe_allow_html=True)
            bar_grouped = (
                filtered_data.groupby(['ParentName_Coresight', 'ChainName_Coresight'])
                .size().reset_index(name='StoreCount')
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
                    y=[row['StoreCount']],
                    name=parent,
                    marker_color=color,
                    text=[f"{row['StoreCount']:,}"],
                    textposition='outside',
                    hovertemplate=(
                        f"<b>{banner}</b><br>"
                        f"<span style='color:{color}'>●</span> "
                        f"<b>{store_type} Stores:</b> %{{y:,}}<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor=color,
                        font=dict(size=13, color="black", family="Arial")
                    )
                ))

            fig_bar.update_layout(
                xaxis_title="Banner",
                yaxis_title=f"{store_type} Stores",
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
            
        # --- Opened Stores by Retailer Chart ---
        st.markdown("---")
        st.markdown(f"<h4 style='font-size: 20px; text-align: left;'>{store_type} Stores by Retailer</h4>", unsafe_allow_html=True)
        if chart_title_prefix == "Net":
            # For Net stores, we need to calculate the net difference by retailer
            # Group opened stores by retailer
            opened_by_retailer = net_filtered_data[net_filtered_data['data_from'] == 'opened'].groupby(['ParentName_Coresight']).size().reset_index(name='Opened_Count')
            # Group closed stores by retailer
            closed_by_retailer = net_filtered_data[net_filtered_data['data_from'] == 'closed'].groupby(['ParentName_Coresight']).size().reset_index(name='Closed_Count')
            # Merge and calculate net
            retailer_agg = pd.merge(opened_by_retailer, closed_by_retailer, on='ParentName_Coresight', how='outer').fillna(0)
            retailer_agg['StoreCount'] = retailer_agg['Opened_Count'] - retailer_agg['Closed_Count']
            retailer_agg = retailer_agg[['ParentName_Coresight', 'StoreCount']]
        else:
            retailer_agg = filtered_data.groupby(['ParentName_Coresight']).size().reset_index(name='StoreCount')
        
        # Sort in descending order by StoreCount
        retailer_agg = pd.DataFrame(retailer_agg).sort_values('StoreCount', ascending=False).reset_index(drop=True)

        # Generate shades of the approved color
        def generate_shades(base_hex, num_shades):
            base_rgb = [int(base_hex[i:i+2], 16)/255. for i in (1, 3, 5)]
            h, l, s = colorsys.rgb_to_hls(*base_rgb)
            # Shades: make some lighter, some darker
            l_values = [min(0.9, max(0.2, l * (0.80 + i*0.20/(max(num_shades-1,1))))) for i in range(num_shades)]
            return [f'#{int(c[0]*255):02x}{int(c[1]*255):02x}{int(c[2]*255):02x}' for c in [colorsys.hls_to_rgb(h, lval, s) for lval in l_values]]

        n = len(retailer_agg)
        my_palette = generate_shades(main_color, n)
        color_map_retailer = {name: my_palette[i] for i, name in enumerate(retailer_agg['ParentName_Coresight'])}

        # Create figure with consistent styling
        fig_retailer = go.Figure()

        for _, row in retailer_agg.iterrows():
            retailer = row['ParentName_Coresight']
            color = color_map_retailer[retailer]
            
            fig_retailer.add_trace(go.Bar(
                x=[retailer],
                y=[row['StoreCount']],
                name=retailer,
                marker_color=color,
                text=[f"{row['StoreCount']:,}"],  # Formatted with thousands separator
                textposition='outside',
                hovertemplate=(
                    "<b>%{x}</b><br>"  # Retailer name
                    "<span style='color:" + str(color) + "'>●</span> "
                    f"<b>{store_type} Stores:</b> %{{y:,}}<extra></extra>"
                ),
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor=color,
                    font=dict(size=13, color="black", family="Arial")
                )
            ))

        fig_retailer.update_layout(
            xaxis_title="Retailer",
            yaxis_title=f"{store_type} Stores",
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
        if chart_title_prefix == "Opened":
                color="#A3C0CE"
        elif chart_title_prefix == "Closed":
                color="#d62e2f"
        elif chart_title_prefix == "Active":
                color="#CBCACA"
        elif chart_title_prefix == "Net":
                color="#2D2A29"
        print("charttt", chart_title_prefix)
        print("filtered_data", filtered_data.columns)
        if 'square_footage' in filtered_data.columns:
                print("helll")
                st.markdown("### Square Footage Analysis")
                sqft_col1, sqft_col2 = st.columns([7, 3])
                ui.render_square_footage_over_time_chart(filtered_data, column=sqft_col1, color=color, chart_title=chart_title_prefix)
                ui.render_square_footage_by_city_chart(filtered_data, column=sqft_col2, color=color)
        
        
    @staticmethod
    def render_compare_sectors_charts(filtered_data, selected_sectors, chart_title_prefix="Opened"):
        """Render charts for Compare Sectors tab"""
        from sip_components.ui_components import UIComponents
        from sip_components.styling_manager import StylingManager
        ui = UIComponents()
        styling = StylingManager()
        
        # Render horizontal line
        styling.render_horizontal_line()
        
        # # Display the sector comparison title (like in opening.py)
        # st.write(f"Comparing sectors: {' vs '.join(selected_sectors)}")
        
        # if chart_title_prefix == "Net":
        #     # Render KPI metrics for Net Openings and Sectors Shown
        #     kpi1, kpi2 = st.columns([1, 1])
        #     with kpi1:
        #         st.metric("Net Openings", f"{(len(opened_filtered) - len(closed_filtered)):,}")
        #     with kpi2:
        #         st.metric("Sectors Shown", f"{len(selected_sectors)}")


        
        # Define a consistent color map for sectors - using the same color scheme as opening.py
        color_map = {
            sector: color for sector, color in zip(
                selected_sectors,
                px.colors.qualitative.Plotly  # Using Plotly's qualitative color palette
            )
        }
        
        # Check if we need to calculate net stores (for net page)
        is_net_calculation = chart_title_prefix == "Net"
          # Display the sector comparison title (like in opening.py)
        st.write(f"Comparing sectors: {' vs '.join(selected_sectors)}")
        
        # Add net column if not present and we need net calculations
        if is_net_calculation and 'net' not in filtered_data.columns:
            filtered_data = filtered_data.copy()
            filtered_data['is_opened'] = (filtered_data['data_from'] == 'opened').astype(int)
            filtered_data['is_closed'] = (filtered_data['data_from'] == 'closed').astype(int)
            filtered_data['net'] = filtered_data['is_opened'] - filtered_data['is_closed']
            # Render KPI metrics for Net Openings and Sectors Shown
            kpi1, kpi2 = st.columns([1, 1])
            with kpi1:
                st.metric("Net Openings", f"{filtered_data['net'].sum():,}")
            with kpi2:
                st.metric("Sectors Shown", f"{len(selected_sectors)}")
        
        # Visualization logic - Line and Bar charts (without duplicate headings)
        line_col, bar_col = st.columns([4, 2])

        with line_col:
            if is_net_calculation:
                # Calculate net data for monthly chart
                monthly_data = (
                    filtered_data
                    .groupby(['Period', 'Sector_Coresight'])['net']
                    .sum()
                    .reset_index(name='Stores')
                )
                y_column = 'Stores'
            else:
                monthly_data = (
                    filtered_data
                    .groupby(['Period', 'Sector_Coresight'])
                    .size()
                    .reset_index(name='Stores')
                )
                y_column = 'Stores'
            
            # Get date range from filtered data
            start_date = filtered_data['Period'].min()
            end_date = filtered_data['Period'].max()
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
                period_data = filtered_data[filtered_data['Period'] == period]
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
                    y=grp_df[y_column],
                    mode='lines+markers',
                    name=str(sector),
                    line=dict(color=color, width=2),
                    marker=dict(size=7, color=color),
                    hovertemplate=(
                        "<b>%{x|%b %Y}</b><br>"  # Formatted month-year
                        "<span style='color:" + str(color) + "'>●</span> "
                        "<b>" + str(sector) + ":</b> %{y:,}<br><br>"
                        "<span style='color:" + str(color) + "'>•</span> "
                        "<span style='color:black'>Number of Banners: %{customdata[0]}</span><br>"
                        "<span style='color:" + str(color) + "'>•</span> "
                        "<span style='color:black'>Number of States: %{customdata[1]}</span><br>"
                        "<span style='color:" + str(color) + "'>•</span> "
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
                title=f'Monthly {chart_title_prefix} Stores by Sector',
                xaxis_title="Month",
                # yaxis_title=f"{chart_title_prefix} Stores",
                yaxis_title = f"{chart_title_prefix} Openings" if is_net_calculation else f"{chart_title_prefix} Stores",
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
                height=400
            )

            config = {
                'displayModeBar': True,
                'displaylogo': False,
                'toImageButtonOptions': {
                    'format': 'png',
                    'filename': f'monthly_{chart_title_prefix.lower()}_stores_by_sector',
                    'height': 400,
                    'width': 700,
                    'scale': 1
                }
            }

            st.plotly_chart(fig_line, use_container_width=True, config=config)

        with bar_col:
            if is_net_calculation:
                total_counts = (
                    filtered_data
                    .groupby('Sector_Coresight')['net']
                    .sum()
                    .reset_index(name='Stores')
                    .sort_values('Stores', ascending=False)
                    .head(15)
                )
                y_column = 'Stores'
                hover_label = f"Net Openings:"
            else:
                total_counts = (
                    filtered_data
                    .groupby('Sector_Coresight')
                    .size()
                    .reset_index(name='Stores')
                    .sort_values('Stores', ascending=False)
                    .head(15)
                )
                y_column = 'Stores'
                hover_label = f"{chart_title_prefix} Stores:"
            
            # Calculate metadata for each sector
            sector_metadata = {}
            for sector in total_counts['Sector_Coresight']:
                sector_data = filtered_data[filtered_data['Sector_Coresight'] == sector]
                sector_metadata[sector] = {
                    'num_banners': sector_data['ChainName_Coresight'].nunique(),
                    'num_states': sector_data['State'].nunique(),
                    'num_msa': sector_data['MsaName'].nunique()
                }
            
            # Create figure with consistent styling
            fig_bar = go.Figure()
            
            for _, row in total_counts.iterrows():
                sector = row['Sector_Coresight']
                color = color_map.get(sector, "#333")  # Get color from your mapping
                
                fig_bar.add_trace(go.Bar(
                    x=[sector],
                    y=[row[y_column]],
                    name=sector,
                    marker_color=color,
                    text=[f"{row[y_column]:,}"],
                    textposition='outside',
                    hovertemplate=(
                        "<b>%{x}</b><br>"  # Sector name
                        "<span style='color:" + color + "'>●</span> "
                        "<b>" + hover_label + "</b> %{y:,}<extra></extra>"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor=color,
                        font=dict(size=13, color="black", family="Arial")
                    )
                ))
            
            fig_bar.update_layout(
                title=f'Total {chart_title_prefix} Stores by Sector',
                xaxis_title="Sector",
                yaxis_title = f"{chart_title_prefix} Openings" if is_net_calculation else f"{chart_title_prefix} Stores",
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
        
        # Create a copy for retailer-specific filtering
        display_data = filtered_data.copy()
        
        for sector in selected_sectors:
            sector_data = display_data[display_data['Sector_Coresight'] == sector]
            if sector_data.empty:
                continue

            if is_net_calculation:
                retailer_counts = (
                    sector_data.groupby('ChainName_Coresight')['net']
                    .sum()
                    .reset_index()
                    .rename(columns={'net': 'Stores'})
                    .sort_values('Stores', ascending=False)
                )
                retailer_counts = retailer_counts[retailer_counts['ChainName_Coresight'].notnull()]
                chart_title = 'Net Stores'
            else:
                retailer_counts = (
                    sector_data['ChainName_Coresight']
                    .value_counts()
                    .reset_index()
                )
                retailer_counts.columns = ['Banners', 'Stores']
                chart_title = f'{chart_title_prefix} Stores'

            title = f'Top Banners in {sector}'
            color = color_map[sector]  # Get the sector color
            
            # Create figure with consistent styling
            fig_retailer = go.Figure()
            
            fig_retailer.add_trace(go.Bar(
                x=retailer_counts['Banners' if not is_net_calculation else 'ChainName_Coresight'],
                y=retailer_counts['Stores'],
                name=sector,
                marker_color=color,
                text=retailer_counts['Stores'],
                textposition='outside',
                hovertemplate=(
                    "<b>%{x}</b><br>"  # Banner name
                    "<span style='color:" + color + "'>●</span> "
                    "<b>" + chart_title + ":</b> %{y:,}<extra></extra>"
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
        if chart_title_prefix == "Opened":
            color="#A3C0CE"
        elif chart_title_prefix == "Closed":
            color="#d62e2f"
        elif chart_title_prefix == "Active":
            color="#CBCACA"
        elif chart_title_prefix == "Net":
            color="#2D2A29"
        if 'square_footage' in filtered_data.columns:
            st.markdown("### Square Footage Analysis")
            sqft_col1, sqft_col2 = st.columns([7, 3])
            ui.render_square_footage_over_time_chart(filtered_data, column=sqft_col1, color=color, chart_title=chart_title_prefix)
            ui.render_square_footage_by_city_chart(filtered_data, column=sqft_col2, color=color)

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
    @staticmethod
    def render_data_table(filtered_data, table_title="Stores Table"):
        """Render a standardized data table with download functionality"""
        st.subheader(table_title)
        
        # Prepare table data
        table_data = filtered_data.copy()
        MAX_ROWS = 2000
        if(len(filtered_data) > MAX_ROWS):
            table_data = filtered_data.head(MAX_ROWS)
        
        if 'Period' in table_data.columns:
            table_data['Year'] = table_data['Period'].dt.year
            table_data['Month'] = table_data['Period'].dt.month
            table_data['Opening Month/Year'] = (table_data['Month'].astype(str) + '-' + table_data['Year'].astype(str))
        
        # Select and rename common columns
        column_mapping = {
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
        }
        
        # Only include columns that exist in the data
        available_columns = [col for col in column_mapping.keys() if col in table_data.columns]
        display_columns = available_columns + ([col for col in ['Opening Month/Year'] if col in table_data.columns and col in column_mapping.keys()])
        
        if display_columns:
            renamed_data = table_data[display_columns].rename(columns=column_mapping)
            
            # Remove duplicate columns by keeping only the first occurrence
            renamed_data = renamed_data.loc[:, ~renamed_data.columns.duplicated(keep='first')]
            
            # Create download button
            col1, col2 = st.columns([7, 1.05])
            with col1:
                st.write("")

            with col2:
                if not renamed_data.empty:
                    # Convert DataFrame to CSV for download (simpler approach)
                    csv = renamed_data.to_csv(index=False)
                    st.download_button(
                        label="Download Data",
                        data=csv,
                        file_name=f"{table_title.replace(' ', '_')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )

            # Display table
            st.dataframe(renamed_data, height=600)
        else:
            st.write("No data available for display")