"""
FilterManager - Dynamic filter creation with session state persistence
"""
import streamlit as st
import pandas as pd
import calendar
import logging
from datetime import date, datetime

class FilterManager:
    def __init__(self, auth_cookie, page_prefix="",base_page=None):
        """Initialize filter manager with auth cookie and optional page prefix
        
        Args:
            auth_cookie: The authentication cookie containing filter state
            page_prefix: Optional prefix for filter keys to isolate filters per page (e.g., "net_", "opening_")
        """
        self.auth_cookie = auth_cookie
        self.page_prefix = page_prefix
        self.base_page = base_page
        
        # Get filters with page prefix - creates isolated filter namespace per page
        filter_key = f"{page_prefix}filters" if page_prefix else "filters"
        self.user_filters = auth_cookie.get(filter_key, {}) if isinstance(auth_cookie, dict) else {}
        
    def save_filters(self):
        """Save current filters to auth cookie using page-specific key"""
        filter_key = f"{self.page_prefix}filters" if self.page_prefix else "filters"
        self.auth_cookie[filter_key] = self.user_filters
         # CRITICAL FIX: Actually save to cookie!
        if self.base_page:
            self.base_page.save_auth_cookie()
        else:
            logging.warning("FilterManager: base_page not provided, filters not persisted to cookie!")

        # Note: This requires the parent class to have a save_auth_cookie method
        
    def get_synchronized_date_range(self, data, default_months_back=12):
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
            # Convert to datetime and then to date
            start_datetime = pd.to_datetime(start_str)
            # Handle different return types from pd.to_datetime
            if isinstance(start_datetime, pd.DatetimeIndex):
                start_date_str = str(start_datetime[0].strftime('%Y-%m-%d')) if len(start_datetime) > 0 else str(default_start.isoformat())
            else:
                start_date_str = str(start_datetime.strftime('%Y-%m-%d'))
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        except Exception as e:
            start_date = default_start

        try:
            # Convert to datetime and then to date
            end_datetime = pd.to_datetime(end_str)
            # Handle different return types from pd.to_datetime
            if isinstance(end_datetime, pd.DatetimeIndex):
                end_date_str = str(end_datetime[0].strftime('%Y-%m-%d')) if len(end_datetime) > 0 else str(max_date.isoformat())
            else:
                end_date_str = str(end_datetime.strftime('%Y-%m-%d'))
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except Exception as e:
            end_date = max_date

        # Clamp to bounds
        start_date = max(start_date, min_date)
        end_date = min(end_date, max_date)

        return (start_date, end_date), min_date, max_date
        
    def render_date_filters(self, data, session_state_key="selected_date_range"):
        """Render date filter controls in an expander"""
        # Extract prefix from session_state_key for downstream filter reset
        session_state_key_prefix = ""
        if session_state_key != "selected_date_range":
            # Extract prefix from keys like "sector_compare_date_range"
            if "_date_range" in session_state_key:
                session_state_key_prefix = session_state_key.replace("_date_range", "") + "_"
            elif session_state_key.endswith("date_range"):
                session_state_key_prefix = session_state_key[:-10]  # Remove "date_range"
                if session_state_key_prefix and not session_state_key_prefix.endswith("_"):
                    session_state_key_prefix += "_"
        
        with st.expander("Date Range", expanded=st.session_state.get("expand_filters", True)):
            selected_date_range, min_date, max_date = self.get_synchronized_date_range(data)
            start_date, end_date = selected_date_range

            qparams = st.query_params
            start_date_param = qparams.get("start_date", [None])[0]
            end_date_param = qparams.get("end_date", [None])[0]

            # Initialize session state from query params or defaults
            try:
                # Handle list parameters and ensure they're strings
                start_param = str(start_date_param[0]) if isinstance(start_date_param, list) else str(start_date_param)
                end_param = str(end_date_param[0]) if isinstance(end_date_param, list) else str(end_date_param)
                
                parsed_start = pd.to_datetime(start_param, format='%Y-%m-%d')
                parsed_end = pd.to_datetime(end_param, format='%Y-%m-%d')
                
                # Convert to date objects
                if isinstance(parsed_start, pd.DatetimeIndex):
                    start_date_str = str(parsed_start[0].strftime('%Y-%m-%d')) if len(parsed_start) > 0 else str(start_date.isoformat())
                else:
                    start_date_str = str(parsed_start.strftime('%Y-%m-%d'))
                start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                
                if isinstance(parsed_end, pd.DatetimeIndex):
                    end_date_str = str(parsed_end[0].strftime('%Y-%m-%d')) if len(parsed_end) > 0 else str(end_date.isoformat())
                else:
                    end_date_str = str(parsed_end.strftime('%Y-%m-%d'))
                end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                
                st.session_state.setdefault("start_month", start_date_obj.month)
                st.session_state.setdefault("start_year", start_date_obj.year)
                st.session_state.setdefault("end_month", end_date_obj.month)
                st.session_state.setdefault("end_year", end_date_obj.year)
            except Exception as e:
                st.session_state.setdefault("start_month", start_date.month)
                st.session_state.setdefault("start_year", start_date.year)
                st.session_state.setdefault("end_month", end_date.month)
                st.session_state.setdefault("end_year", end_date.year)

            min_year, max_year = min_date.year, max_date.year
            all_months = list(range(1, 13))
            all_years = list(range(min_year, max_year + 1))

            # --- START DATE PICKERS ---
            sm_col, sy_col = st.columns([1.3, 1])
            with sm_col:
                # Valid start months depending on selected year
                if st.session_state["start_year"] == min_year:
                    valid_start_months = list(range(min_date.month, 13))
                elif st.session_state["start_year"] == max_year:
                    valid_start_months = list(range(1, max_date.month + 1))
                else:
                    valid_start_months = all_months

                st.selectbox(
                    "Start Month",
                    options=valid_start_months,
                    format_func=lambda x: str(calendar.month_name[x]),
                    index=valid_start_months.index(st.session_state["start_month"]),
                    key="start_month_select"
                )
            with sy_col:
                st.selectbox(
                    "Start Year",
                    options=all_years,
                    index=all_years.index(st.session_state["start_year"]),
                    key="start_year_select"
                )
            st.session_state["start_month"] = st.session_state["start_month_select"]
            st.session_state["start_year"] = st.session_state["start_year_select"]

            # --- END DATE PICKERS ---
            em_col, ey_col = st.columns([1.3, 1])

            # Valid end years should be >= start year and <= max year
            valid_end_years = [y for y in all_years if st.session_state["start_year"] <= y <= max_year]
            if st.session_state["end_year"] not in valid_end_years:
                st.session_state["end_year"] = valid_end_years[0]

            # Valid end months depend on selected start and end years
            if st.session_state["end_year"] == st.session_state["start_year"]:
                valid_end_months = list(range(st.session_state["start_month"], 13))
            elif st.session_state["end_year"] == max_year:
                valid_end_months = list(range(1, max_date.month + 1))
            else:
                valid_end_months = all_months

            if st.session_state["end_month"] not in valid_end_months:
                st.session_state["end_month"] = valid_end_months[0]

            with em_col:
                st.selectbox(
                    "End Month",
                    options=valid_end_months,
                    format_func=lambda x: str(calendar.month_name[x]),
                    index=valid_end_months.index(st.session_state["end_month"]),
                    key="end_month_select"
                )
            with ey_col:
                st.selectbox(
                    "End Year",
                    options=valid_end_years,
                    index=valid_end_years.index(st.session_state["end_year"]),
                    key="end_year_select"
                )
            st.session_state["end_month"] = st.session_state["end_month_select"]
            st.session_state["end_year"] = st.session_state["end_year_select"]

            # Final date construction with boundary clamping
            new_start_date = max(date(st.session_state["start_year"], st.session_state["start_month"], 1), min_date)
            end_day = calendar.monthrange(st.session_state["end_year"], st.session_state["end_month"])[1]
            new_end_date = min(date(st.session_state["end_year"], st.session_state["end_month"], end_day), max_date)

            if (new_start_date != start_date or new_end_date != end_date):
                st.session_state[session_state_key] = (new_start_date, new_end_date)
                st.query_params.update({
                    "start_date": new_start_date.isoformat(),
                    "end_date": new_end_date.isoformat()
                })
                # Reset downstream filters when date range changes
                # This ensures that sector, retailer, and location filters are updated based on the new date range
                self.reset_downstream_filters(session_state_key_prefix)
                st.rerun()

            st.session_state.update({
                session_state_key: (new_start_date, new_end_date)
            })
            
    def reset_downstream_filters(self, session_state_key_prefix=""):
        """Reset downstream filters when date range changes"""
        # Reset sector filter
        sector_key = f"{session_state_key_prefix}selected_sector_name" if session_state_key_prefix else "selected_sector_name"
        if sector_key in self.user_filters:
            self.user_filters[sector_key] = "All"
            
        # Reset sector comparison filter
        sector_comparison_key = f"{session_state_key_prefix}sector_comparison_selected_sectors" if session_state_key_prefix else "sector_comparison_selected_sectors"
        if sector_comparison_key in self.user_filters:
            self.user_filters[sector_comparison_key] = []
            
        # Reset retailer filters
        parent_chain_key = f"{session_state_key_prefix}parent_chain_name" if session_state_key_prefix else "parent_chain_name"
        self.user_filters[parent_chain_key] = ["All"] if not session_state_key_prefix else []
        
        selected_chain_key = f"{session_state_key_prefix}selected_chain_name" if session_state_key_prefix else "selected_chain_name"
        self.user_filters[selected_chain_key] = ["All"] if not session_state_key_prefix else []
        
        # Reset location filters
        selected_state_key = f"{session_state_key_prefix}selected_state_name" if session_state_key_prefix else "selected_state_name"
        self.user_filters[selected_state_key] = ["All"]
        
        selected_msa_key = f"{session_state_key_prefix}selected_msa_name" if session_state_key_prefix else "selected_msa_name"
        self.user_filters[selected_msa_key] = ["All"]
        
        selected_zip_key = f"{session_state_key_prefix}selected_zip_code" if session_state_key_prefix else "selected_zip_code"
        self.user_filters[selected_zip_key] = ["All"]
        
        # Save the updated filters
        self.save_filters()
        
    def render_sector_filter(self, data, session_state_key="selected_sector_name", session_state_key_prefix=""):
        """Render sector filter dropdown"""
        # Use the prefix if provided
        actual_session_key = f"{session_state_key_prefix}{session_state_key}" if session_state_key_prefix else session_state_key
        
        filtered_sector_data = data
        start_date, end_date = st.session_state.get("selected_date_range", (data['Period'].min(), data['Period'].max()))

        start_timestamp = pd.Timestamp(start_date)
        end_timestamp = pd.Timestamp(end_date)
        filtered_sector_data = filtered_sector_data[
            (filtered_sector_data['Period'] >= start_timestamp) &
            (filtered_sector_data['Period'] <= end_timestamp)
        ]

        # Get unique sector names and sort them properly
        sector_names = filtered_sector_data["Sector_Coresight"].dropna().unique().tolist()
        
        # Separate "Others" from the rest (case-insensitive)
        others_items = [s for s in sector_names if s.lower() in ["other", "others"]]
        regular_items = [s for s in sector_names if s.lower() not in ["other", "others"]]
        
        # Sort regular items alphabetically (case-insensitive)
        regular_items = sorted(regular_items, key=lambda x: x.lower())
        sector_names = ["All"] + regular_items + others_items

        # --- COOKIFIED FILTER STATE ---
        current_sector = self.user_filters.get(actual_session_key, "All")
        if current_sector not in sector_names:
            current_sector = "All"
            self.user_filters[actual_session_key] = "All"

        current_index = sector_names.index(current_sector) if current_sector in sector_names else 0
        with st.expander("Sector", expanded=st.session_state.get("expand_filters", True)):
            selected_sector_name = st.selectbox(
                "Select Sector",
                sector_names,
                index=current_index,
                key=f"sector_selectbox_{actual_session_key}"
            )

            # Update cookie if user changed selection, reset children, rerun
            if selected_sector_name != current_sector:
                self.user_filters[actual_session_key] = selected_sector_name
                parent_chain_key = f"{session_state_key_prefix}parent_chain_name" if session_state_key_prefix else "parent_chain_name"
                self.user_filters[parent_chain_key] = ["All"]
                selected_chain_key = f"{session_state_key_prefix}selected_chain_name" if session_state_key_prefix else "selected_chain_name"
                self.user_filters[selected_chain_key] = ["All"]
                selected_state_key = f"{session_state_key_prefix}selected_state_name" if session_state_key_prefix else "selected_state_name"
                self.user_filters[selected_state_key] = ["All"]
                selected_msa_key = f"{session_state_key_prefix}selected_msa_name" if session_state_key_prefix else "selected_msa_name"
                self.user_filters[selected_msa_key] = ["All"]
                selected_zip_key = f"{session_state_key_prefix}selected_zip_code" if session_state_key_prefix else "selected_zip_code"
                self.user_filters[selected_zip_key] = ["All"]
                self.save_filters()
                st.rerun()
                
    def render_sector_comparison_filters(self, data, session_state_key_prefix=""):
        """Render sector comparison filters (multiple sector selection)"""
        # Get current filter values from user_filters
        sector_comparison_key = f"{session_state_key_prefix}sector_comparison_selected_sectors" if session_state_key_prefix else "sector_comparison_selected_sectors"
        
        # Apply date filters
        date_range_key = f"{session_state_key_prefix}date_range" if session_state_key_prefix else "selected_date_range"
        start_date, end_date = st.session_state.get(date_range_key, (data['Period'].min(), data['Period'].max()))
        start_timestamp = pd.Timestamp(start_date)
        end_timestamp = pd.Timestamp(end_date)
        filtered_data = data[
            (data['Period'] >= start_timestamp) &
            (data['Period'] <= end_timestamp)
        ]
        
        # Get unique sector names and sort them properly
        sector_names = filtered_data["Sector_Coresight"].dropna().unique().tolist()
        
        # Separate "Others" from the rest (case-insensitive)
        others_items = [s for s in sector_names if s.lower() in ["other", "others"]]
        regular_items = [s for s in sector_names if s.lower() not in ["other", "others"]]
        
        # Sort regular items alphabetically (case-insensitive)
        regular_items = sorted(regular_items, key=lambda x: x.lower())
        sector_names = regular_items + others_items  # No "All" for sector comparison
        
        # Get current sector selection
        current_sectors = self.user_filters.get(sector_comparison_key, [])
        if "All" in current_sectors:
            current_sectors = []
        else:
            valid_selected = [s for s in current_sectors if s in sector_names]
            current_sectors = valid_selected if valid_selected else []

        with st.expander("Sector Comparison", expanded=st.session_state.get("expand_filters", True)):
            sector_comparison_selected_sectors = st.multiselect(
                "Select Sectors to Compare (minimum 2)",
                sector_names,
                default=current_sectors,
                key=f"sector_multiselect_{session_state_key_prefix}" if session_state_key_prefix else "sector_multiselect"
            )

            # Update filters if changed
            if sector_comparison_selected_sectors != current_sectors:
                self.user_filters[sector_comparison_key] = sector_comparison_selected_sectors
                parent_chain_key = f"{session_state_key_prefix}parent_chain_name" if session_state_key_prefix else "parent_chain_name"
                self.user_filters[parent_chain_key] = []
                selected_chain_key = f"{session_state_key_prefix}selected_chain_name" if session_state_key_prefix else "selected_chain_name"
                self.user_filters[selected_chain_key] = []
                selected_state_key = f"{session_state_key_prefix}selected_state_name" if session_state_key_prefix else "selected_state_name"
                self.user_filters[selected_state_key] = ["All"]
                selected_msa_key = f"{session_state_key_prefix}selected_msa_name" if session_state_key_prefix else "selected_msa_name"
                self.user_filters[selected_msa_key] = ["All"]
                selected_zip_key = f"{session_state_key_prefix}selected_zip_code" if session_state_key_prefix else "selected_zip_code"
                self.user_filters[selected_zip_key] = ["All"]
                self.save_filters()
                st.rerun()
                
        return sector_comparison_selected_sectors
                
    def update_options(self, selected_options, session_key):
        """Update multiselect options to handle 'All' selection"""
        if "All" in selected_options and len(selected_options) > 1:
            if "All" not in st.session_state[session_key]:
                return ["All"]
            return [opt for opt in selected_options if opt != "All"]
        return selected_options
        
    def render_retailer_filters(self, data, session_state_key_prefix="", disabled=False):
        """Render retailer filters (Parent Company and Banner/Brand)"""
        # Get current filter values from user_filters
        # For comparison tabs, we need to get the sector selection from the correct session key
        if session_state_key_prefix:
            # For comparison tabs, check if we're in sector comparison mode
            if session_state_key_prefix == "sector_compare_":
                # Get sector comparison selection
                sector_comparison_key = f"{session_state_key_prefix}sector_comparison_selected_sectors"
                sector_comparison_selected_sectors = self.user_filters.get(sector_comparison_key, [])
                selected_sector_name = sector_comparison_selected_sectors
            else:
                # For other comparison tabs, use the standard sector key
                sector_key = f"{session_state_key_prefix}selected_sector_name"
                selected_sector_name = self.user_filters.get(sector_key, "All")
        else:
            # For base dashboard, use the standard sector key
            selected_sector_name = self.user_filters.get("selected_sector_name", "All")
        
        # Apply date filters
        date_range_key = f"{session_state_key_prefix}date_range" if session_state_key_prefix else "selected_date_range"
        start_date, end_date = st.session_state.get(date_range_key, (data['Period'].min(), data['Period'].max()))
        start_timestamp = pd.Timestamp(start_date)
        end_timestamp = pd.Timestamp(end_date)
        filtered_data = data[
            (data['Period'] >= start_timestamp) &
            (data['Period'] <= end_timestamp)
        ]
        
        # IMPORTANT: Filter by sector to show only retailers in the selected sector
        # Handle both single sector selection and multiple sector selection
        if isinstance(selected_sector_name, list) and len(selected_sector_name) > 0 and selected_sector_name != ["All"]:
            # Multiple sectors selected (sector comparison mode)
            filtered_data = filtered_data[
                filtered_data["Sector_Coresight"].isin(selected_sector_name)
            ]
        elif selected_sector_name != "All" and selected_sector_name != ["All"] and not (isinstance(selected_sector_name, list) and len(selected_sector_name) == 0):
            # Single sector selected
            filtered_data = filtered_data[
                filtered_data["Sector_Coresight"] == selected_sector_name
            ]
            
        # Prepare parent company names
        filtered_data['ParentName_Coresight'] = filtered_data['ParentName_Coresight'].where(
            pd.notna(filtered_data['ParentName_Coresight']), "No Parent Retailer")
        parent_names = filtered_data['ParentName_Coresight'].unique().tolist()
        parent_names.sort()
        if "No Parent Retailer" in parent_names:
            parent_names.remove("No Parent Retailer")
            parent_names.append("No Parent Retailer")
        parent_names = parent_names  # Don't add "All" for Compare Retailers tab
        
        # Get current parent selection
        parent_chain_key = f"{session_state_key_prefix}parent_chain_name" if session_state_key_prefix else "parent_chain_name"
        current_parent = self.user_filters.get(parent_chain_key, ["All"])  # Default to ["All"] for Base Dashboard
        if "All" in current_parent:
            current_parent = [] if session_state_key_prefix else ["All"]  # Empty for Compare tabs, ["All"] for Base
        else:
            valid_selected = [p for p in current_parent if p in parent_names]
            current_parent = valid_selected if valid_selected else ([] if session_state_key_prefix else ["All"])
        with st.expander("Retailers", expanded=st.session_state.get("expand_filters", True)):
            parent_chain_name = st.multiselect(
                "Select Company", 
                parent_names if session_state_key_prefix else ["All"] + parent_names,  # Add "All" for Base Dashboard
                default=current_parent,
                max_selections=10, 
                help="Parent company that owns one or more store banners.", 
                key=f"parent_multiselect_1_{session_state_key_prefix}" if session_state_key_prefix else "parent_multiselect_1",
                disabled=disabled
            )
            # Handle "All" selection logic for Base Dashboard
            if not session_state_key_prefix:  # Base Dashboard
                if len(parent_chain_name) > 1 and "All" in parent_chain_name:
                    if parent_chain_name[-1] == "All":
                        parent_chain_name = ["All"]
                    else:
                        parent_chain_name = [p for p in parent_chain_name if p != "All"]
                elif not parent_chain_name:
                    parent_chain_name = ["All"]
            
            # Update filters if changed
            if parent_chain_name != current_parent:
                self.user_filters[parent_chain_key] = parent_chain_name
                selected_chain_key = f"{session_state_key_prefix}selected_chain_name" if session_state_key_prefix else "selected_chain_name"
                self.user_filters[selected_chain_key] = [] if session_state_key_prefix else ["All"]  # Reset chain
                selected_state_key = f"{session_state_key_prefix}selected_state_name" if session_state_key_prefix else "selected_state_name"
                self.user_filters[selected_state_key] = ["All"]
                selected_msa_key = f"{session_state_key_prefix}selected_msa_name" if session_state_key_prefix else "selected_msa_name"
                self.user_filters[selected_msa_key] = ["All"]
                self.save_filters()
                st.rerun()

            st.markdown(
                """
                <div style='text-align: center; font-size: 0.9rem; font-weight: 500; margin-top: -0.4rem; margin-bottom: -0.2rem; color: #444;'>or</div>
                """,
                unsafe_allow_html=True
            )
            # Filter data by parent selection for chain filter
            if len(parent_chain_name) >= 1 and "All" not in parent_chain_name:
                filtered_chain_data = filtered_data[filtered_data["ParentName_Coresight"].isin(parent_chain_name)]
            else:
                # If no parents selected or "All" selected, use all data in this sector
                filtered_chain_data = filtered_data
            # Apply date filters to chain data (sector filter already applied above)
            filtered_chain_data = filtered_chain_data[
                (filtered_chain_data['Period'] >= start_timestamp) &
                (filtered_chain_data['Period'] <= end_timestamp)
            ]  
            # Prepare chain names
            chain_names = sorted(
                [x for x in filtered_chain_data['ChainName_Coresight'].unique().tolist() if x is not None], 
                key=lambda x: str(x)
            )
            # Add "All" for Base Dashboard, not for Compare Retailers tab
            if not session_state_key_prefix:
                chain_names = ["All"] + chain_names
            else:
                chain_names =  chain_names
            # Get current chain selection
            selected_chain_key = f"{session_state_key_prefix}selected_chain_name" if session_state_key_prefix else "selected_chain_name"
            current_chain = self.user_filters.get(selected_chain_key, ["All"])  # Default to ["All"] for Base Dashboard
            if "All" in current_chain:
                current_chain = [] if session_state_key_prefix else ["All"]  # Empty for Compare tabs, ["All"] for Base
            else:
                valid_chains = [c for c in current_chain if c in chain_names]
                current_chain = valid_chains if valid_chains else ([] if session_state_key_prefix else ["All"])
            selected_chain_name = st.multiselect(
                "Select Banner/Brand", 
                chain_names, 
                default=current_chain,
                max_selections=10, 
                help="The specific retail banner or storefront name customers see.",
                key=f"multiselect_selected_chain_{session_state_key_prefix}" if session_state_key_prefix else "multiselect_selected_chain",
                disabled=disabled
            )

            # Handle "All" selection logic for Base Dashboard
            if not session_state_key_prefix:  # Base Dashboard
                if len(selected_chain_name) > 1 and "All" in selected_chain_name:
                    if selected_chain_name[-1] == "All":
                        selected_chain_name = ["All"]
                    else:
                        selected_chain_name = [c for c in selected_chain_name if c != "All"]
                elif not selected_chain_name:
                    selected_chain_name = ["All"]
            
            # Update filters if changed
            if selected_chain_name != current_chain:
                self.user_filters[selected_chain_key] = selected_chain_name
                selected_state_key = f"{session_state_key_prefix}selected_state_name" if session_state_key_prefix else "selected_state_name"
                self.user_filters[selected_state_key] = ["All"]
                selected_msa_key = f"{session_state_key_prefix}selected_msa_name" if session_state_key_prefix else "selected_msa_name"
                self.user_filters[selected_msa_key] = ["All"]
                self.save_filters()
                st.rerun()
                
        return parent_chain_name, selected_chain_name
        
    def render_location_filters(self, data, disabled=False, session_state_key_prefix=""):
        """Render location filters (State, MSA, ZIP)"""
        # Get current filter values
        parent_chain_key = f"{session_state_key_prefix}parent_chain_name" if session_state_key_prefix else "parent_chain_name"
        parent_chain_name = self.user_filters.get(parent_chain_key, ["All"])
        selected_chain_key = f"{session_state_key_prefix}selected_chain_name" if session_state_key_prefix else "selected_chain_name"
        selected_chain_name = self.user_filters.get(selected_chain_key, ["All"])
        selected_sector_name = self.user_filters.get("selected_sector_name", "All")
        
        # Create filtered location data
        filtered_location_data = data.copy()

        # Apply date filters
        date_range_key = f"{session_state_key_prefix}date_range" if session_state_key_prefix else "selected_date_range"
        start_date, end_date = st.session_state.get(date_range_key, (data['Period'].min(), data['Period'].max()))
        start_timestamp = pd.Timestamp(start_date)
        end_timestamp = pd.Timestamp(end_date)
        filtered_location_data = filtered_location_data[
            (filtered_location_data['Period'] >= start_timestamp) &
            (filtered_location_data['Period'] <= end_timestamp)
        ]

        # Apply retailer filters
        if "All" not in parent_chain_name and parent_chain_name != []:
            filtered_location_data = filtered_location_data[
                filtered_location_data["ParentName_Coresight"].isin(parent_chain_name)]

        if "All" not in selected_chain_name and selected_chain_name != []:
            filtered_location_data = filtered_location_data[
                filtered_location_data["ChainName_Coresight"].isin(selected_chain_name)]

        if selected_sector_name != "All":
            filtered_location_data = filtered_location_data[
                filtered_location_data["Sector_Coresight"] == selected_sector_name
            ]

        with st.expander("Location", expanded=st.session_state.get("expand_filters", True)):
            # -----------------
            # STATE FILTER
            # -----------------
            states = sorted(filtered_location_data["State"].dropna().unique().tolist())
            states = ["All"] + states

            # Get current state from user_filters
            selected_state_key = f"{session_state_key_prefix}selected_state_name" if session_state_key_prefix else "selected_state_name"
            current_state = self.user_filters.get(selected_state_key, ["All"])
            if "All" in current_state:
                current_state = ["All"]
            else:
                valid_states = [s for s in current_state if s in states]
                current_state = valid_states if valid_states else ["All"]

            selected_state_name = st.multiselect(
                "Select State", 
                states, 
                default=current_state, 
                help="U.S. state where the store is located.",
                key=f"multiselect_state_{session_state_key_prefix}" if session_state_key_prefix else "multiselect_state",
                disabled=disabled
            )

            # Handle "All" selection logic for states
            if len(selected_state_name) > 1 and "All" in selected_state_name:
                if selected_state_name[-1] == "All":
                    selected_state_name = ["All"]
                else:
                    selected_state_name = [s for s in selected_state_name if s != "All"]
            elif not selected_state_name:
                selected_state_name = ["All"]

            # Update filters if changed
            if selected_state_name != current_state:
                self.user_filters[selected_state_key] = selected_state_name
                selected_msa_key = f"{session_state_key_prefix}selected_msa_name" if session_state_key_prefix else "selected_msa_name"
                self.user_filters[selected_msa_key] = ["All"]  # reset MSA on state change
                selected_zip_key = f"{session_state_key_prefix}selected_zip_code" if session_state_key_prefix else "selected_zip_code"
                self.user_filters[selected_zip_key] = ["All"]  # Reset postal code on state change
                self.save_filters()
                st.rerun()

            # Filter data for MSA based on state selection
            if "All" in selected_state_name or not selected_state_name:
                filtered_data_msa = filtered_location_data
            else:
                filtered_data_msa = filtered_location_data[filtered_location_data['State'].isin(selected_state_name)]

            # -----------------
            # MSA FILTER
            # -----------------
            msa_names = sorted(filtered_data_msa['MsaName'].dropna().unique().tolist())
            msa_names = ["All"] + msa_names

            selected_msa_key = f"{session_state_key_prefix}selected_msa_name" if session_state_key_prefix else "selected_msa_name"
            current_msa = self.user_filters.get(selected_msa_key, ["All"])
            if "All" not in msa_names:
                msa_names.append("All")
            if "All" in current_msa:
                current_msa = ["All"]
            else:
                valid_msa = [m for m in current_msa if m in msa_names]
                current_msa = valid_msa if valid_msa else ["All"]

            selected_msa_name = st.multiselect(
                "Select Metropolitan Statistical Area (MSA)",
                msa_names, 
                default=current_msa,
                help="U.S. postal area where the store is located.",
                key=f"multiselect_msa_{session_state_key_prefix}" if session_state_key_prefix else "multiselect_msa",
                disabled=disabled
            )

            # Handle "All" selection logic for MSA
            if len(selected_msa_name) > 1 and "All" in selected_msa_name:
                if selected_msa_name[-1] == "All":
                    selected_msa_name = ["All"]
                else:
                    selected_msa_name = [m for m in selected_msa_name if m != "All"]
            elif not selected_msa_name:
                selected_msa_name = ["All"]

            # Update filters if changed
            if selected_msa_name != current_msa:
                self.user_filters[selected_msa_key] = selected_msa_name
                selected_zip_key = f"{session_state_key_prefix}selected_zip_code" if session_state_key_prefix else "selected_zip_code"
                self.user_filters[selected_zip_key] = ["All"]  # Reset postal code on MSA change
                self.save_filters()
                st.rerun()

            # -----------------
            # ZIP CODE FILTER
            # -----------------
            # Restrict zip codes by selected_msa_name if MSA is selected, else by selected_state_name if State is selected, else all
            if "All" not in selected_msa_name and selected_msa_name:
                zip_pool = filtered_location_data[filtered_location_data['MsaName'].isin(selected_msa_name)]
            elif "All" not in selected_state_name and selected_state_name:
                zip_pool = filtered_location_data[filtered_location_data['State'].isin(selected_state_name)]
            else:
                zip_pool = filtered_location_data
                
            zip_codes = sorted(zip_pool['PostalCode'].dropna().astype(str).str.strip().unique().tolist())
            zip_codes = ["All"] + [zc for zc in zip_codes if zc != "0"]

            selected_zip_key = f"{session_state_key_prefix}selected_zip_code" if session_state_key_prefix else "selected_zip_code"
            current_zip = self.user_filters.get(selected_zip_key, ["All"])
            if "All" in current_zip:
                current_zip = ["All"]
            else:
                valid_zip = [z for z in current_zip if z in zip_codes]
                current_zip = valid_zip if valid_zip else ["All"]

            selected_zip_code = st.multiselect(
                "Select Zip Code",
                zip_codes,
                default=current_zip,
                help="U.S. postal code where the store is located.",
                key=f"multiselect_zip_code_{session_state_key_prefix}" if session_state_key_prefix else "multiselect_zip_code",
                disabled=disabled
            )

            # Handle "All" selection logic for ZIP
            if len(selected_zip_code) > 1 and "All" in selected_zip_code:
                if selected_zip_code[-1] == "All":
                    selected_zip_code = ["All"]
                else:
                    selected_zip_code = [z for z in selected_zip_code if z != "All"]
            elif not selected_zip_code:
                selected_zip_code = ["All"]

            # Update filters if changed
            if selected_zip_code != current_zip:
                self.user_filters[selected_zip_key] = selected_zip_code
                self.save_filters()
                st.rerun()
                
        return selected_state_name, selected_msa_name, selected_zip_code
        
    def render_all_filters(self, data, session_state_key_prefix="", location_disabled=False,use_sector_comparison=False, retailer_filter_disabled=False,location_enable_conditions=None):
        """Render all filters for the page with support for special comparison tab requirements
        
        Args:
            data: The data to filter
            session_state_key_prefix: Prefix for session state keys
            location_disabled: Whether location filters should be disabled
            use_sector_comparison: Whether to use sector comparison filters (multiselect) instead of single sector
            retailer_filter_disabled: Whether retailer filters should be disabled
            location_enable_conditions: Function that returns True if location filters should be enabled
        """
        # Create filter columns
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            self.render_date_filters(data, session_state_key=session_state_key_prefix + "date_range" if session_state_key_prefix else "selected_date_range")

        with col2:
            if use_sector_comparison:
                sector_selection = self.render_sector_comparison_filters(
                    data, session_state_key_prefix=session_state_key_prefix)
            else:
                self.render_sector_filter(data, session_state_key_prefix=session_state_key_prefix)
                sector_selection = None
            
        with col3:
            if use_sector_comparison:
                retailer_filter_disabled = not len(sector_selection) >= 2 if sector_selection else True
            parent_chain_name, selected_chain_name = self.render_retailer_filters(
                data, session_state_key_prefix=session_state_key_prefix, disabled=retailer_filter_disabled)
            
        with col4:
            # Determine if location filters should be enabled
            if location_enable_conditions:
                location_disabled = not location_enable_conditions(parent_chain_name, selected_chain_name, sector_selection)
            
            selected_state_name, selected_msa_name, selected_zip_code = self.render_location_filters(
                data, disabled=location_disabled, session_state_key_prefix=session_state_key_prefix)
            
        return {
            "parent_chain_name": parent_chain_name,
            "selected_chain_name": selected_chain_name,
            "selected_state_name": selected_state_name,
            "selected_msa_name": selected_msa_name,
            "selected_zip_code": selected_zip_code,
            "sector_selection": sector_selection
        }
        
    def apply_all_filters(self, data, session_state_key_prefix=""):
        """Apply all filters to the data"""
        # Get filter values with proper session state key prefix
        sector_key = f"{session_state_key_prefix}selected_sector_name" if session_state_key_prefix else "selected_sector_name"
        selected_sector_name = self.user_filters.get(sector_key, "All")
        
        # For sector comparison, we need to check the sector comparison key
        sector_comparison_key = f"{session_state_key_prefix}sector_comparison_selected_sectors" if session_state_key_prefix else "sector_comparison_selected_sectors"
        sector_comparison_selected_sectors = self.user_filters.get(sector_comparison_key, [])
        
        parent_chain_key = f"{session_state_key_prefix}parent_chain_name" if session_state_key_prefix else "parent_chain_name"
        parent_chain_name = self.user_filters.get(parent_chain_key, ["All"])
        # For Base Dashboard (no prefix), treat empty list as ["All"]
        if not session_state_key_prefix and not parent_chain_name:
            parent_chain_name = ["All"]
        
        selected_chain_key = f"{session_state_key_prefix}selected_chain_name" if session_state_key_prefix else "selected_chain_name"
        selected_chain_name = self.user_filters.get(selected_chain_key, ["All"])
        # For Base Dashboard (no prefix), treat empty list as ["All"]
        if not session_state_key_prefix and not selected_chain_name:
            selected_chain_name = ["All"]
        
        selected_state_key = f"{session_state_key_prefix}selected_state_name" if session_state_key_prefix else "selected_state_name"
        selected_state_name = self.user_filters.get(selected_state_key, ["All"])
        
        selected_msa_key = f"{session_state_key_prefix}selected_msa_name" if session_state_key_prefix else "selected_msa_name"
        selected_msa_name = self.user_filters.get(selected_msa_key, ["All"])
        
        selected_zip_key = f"{session_state_key_prefix}selected_zip_code" if session_state_key_prefix else "selected_zip_code"
        selected_zip_code = self.user_filters.get(selected_zip_key, ["All"])
        
        # Apply date filters
        date_range_key = f"{session_state_key_prefix}date_range" if session_state_key_prefix else "selected_date_range"
        start_date, end_date = st.session_state.get(date_range_key, (data['Period'].min(), data['Period'].max()))
        start_timestamp = pd.Timestamp(start_date)
        end_timestamp = pd.Timestamp(end_date)
        filtered_data = data[
            (data['Period'] >= start_timestamp) &
            (data['Period'] <= end_timestamp)
        ]
        
        # Apply sector filter - handle both single sector and sector comparison
        if sector_comparison_selected_sectors and len(sector_comparison_selected_sectors) > 0:
            # Sector comparison mode - filter by multiple sectors
            filtered_data = filtered_data[
                filtered_data['Sector_Coresight'].isin(sector_comparison_selected_sectors)
            ]
        elif selected_sector_name != "All":
            # Single sector mode - filter by single sector
            filtered_data = filtered_data[
                filtered_data['Sector_Coresight'] == selected_sector_name
            ]
            
        # Apply parent company filter
        if "All" not in parent_chain_name and parent_chain_name:
            filtered_data = filtered_data[
                filtered_data['ParentName_Coresight'].isin(parent_chain_name)
            ]
            
        # Apply chain filter
        if "All" not in selected_chain_name and selected_chain_name:
            filtered_data = filtered_data[
                filtered_data['ChainName_Coresight'].isin(selected_chain_name)
            ]
            
        # Apply state filter
        if "All" not in selected_state_name and selected_state_name:
            filtered_data = filtered_data[
                filtered_data['State'].isin(selected_state_name)
            ]
            
        # Apply MSA filter
        if "All" not in selected_msa_name and selected_msa_name:
            filtered_data = filtered_data[
                filtered_data['MsaName'].isin(selected_msa_name)
            ]
            
        # Apply ZIP code filter
        if "All" not in selected_zip_code and selected_zip_code:
            filtered_data = filtered_data[
                filtered_data['PostalCode'].astype(str).isin([str(z) for z in selected_zip_code])
            ]
            
        return filtered_data