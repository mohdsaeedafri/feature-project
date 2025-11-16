# Refactored Net.py - Quick Implementation Guide

## Overview
The refactored `net.py` (435 lines) replaces the original monolithic version (3808+ lines) using the SIP modular architecture. It maintains 100% feature parity while reducing code by 88%.

## Architecture Components Used

### 1. BasePage
Handles:
- Authentication & session management
- Page title & configuration
- Header rendering
- Navigation buttons (Release Portal, Changelogs, Help, Logout)

### 2. FilterManager
Provides:
- `render_all_filters()`: Renders date, sector, retailers, and location filters
- `apply_all_filters()`: Applies all filters to dataset
- Dynamic filter updates based on user selections
- Filter persistence via auth cookies

### 3. TabComponents  
Provides:
- `render_tab_navigation()`: Shows "Net Openings", "Store Openings", "Store Closures", "Active Stores" tabs
- `render_tab_selector()`: Allows choosing between "Base Dashboard", "Compare Retailers", "Compare Sectors"
- `render_metrics_section()`: Renders metric cards with dynamic labels
- `render_standard_charts()`: Renders time series and banner charts
- `render_compare_retailers_charts()`: Renders retailer comparison charts
- `render_compare_sectors_charts()`: Renders sector comparison charts
- `render_data_table()`: Renders filtered data table with download option
- `render_filter_controls()`: Toggle to expand/collapse all filters
- `render_reset_button()`: Reset all filters to defaults

### 4. UIComponents
Provides:
- Metric card rendering
- Chart rendering utilities
- Data visualization components

### 5. StylingManager
Provides:
- `apply_global_styles()`: Global Streamlit CSS
- `hide_streamlit_elements()`: Hide header/footer/menu
- `apply_bootstrap()`: Bootstrap CSS integration

## Key Data Flow

### Data Fetching
```python
# Separate queries for opened and closed stores
opened_data, closed_data = fetch_net_data()
```

### Filtering Process
```
1. User selects date range
2. User selects sector (optional)
3. User selects retailers (parent company and/or chain)
4. User selects location (state, MSA/postal code)
5. FilterManager applies all filters to both opened_data and closed_data
```

### Net Calculation
```python
filtered_opened = filter_manager.apply_all_filters(opened_data)
filtered_closed = filter_manager.apply_all_filters(closed_data)

net_openings = len(filtered_opened) - len(filtered_closed)
```

### Data Combination
```python
filtered_opened['data_from'] = 'opened'
filtered_closed['data_from'] = 'closed'
combined_filtered_data = pd.concat([filtered_opened, filtered_closed])
```

## Metrics Section (6 Columns)

| Column | Calculation | Data Source |
|--------|-----------|-------------|
| 1 | Net Opened Stores | opened_count - closed_count |
| 2 | Total Affected Banners | unique ParentName from combined data |
| 3 | Total Affected Sectors | unique Sector from combined data |
| 4 | Previous Period Net Openings | (prev_opened - prev_closed) with same filters applied |
| 5 | % Change vs Previous | ((current_net - prev_net) / abs(prev_net)) * 100 |
| 6 | Opened per 10K people | (opened_count / total_population) * 10000 |

## Tab Implementation

### Base Dashboard
```python
# Render filters
filter_values = filter_manager.render_all_filters(opened_data)

# Apply filters
filtered_opened = filter_manager.apply_all_filters(opened_data)
filtered_closed = filter_manager.apply_all_filters(closed_data)

# Calculate metrics
net_openings = len(filtered_opened) - len(filtered_closed)

# Render components
- Metrics section (6 columns)
- Charts (time series + top banners)
- Data table (combined with data_from)
```

### Compare Retailers Tab
```python
# Create separate filter manager with "compare_retailers_" prefix
# Require at least 2 retailers selected
# Apply filters to both datasets
# Render comparison charts with color-coding per retailer
```

### Compare Sectors Tab
```python
# Create separate filter manager with "sector_compare_" prefix
# Require at least 2 sectors selected
# Apply filters to both datasets
# Render sector comparison charts
```

## Previous Period Logic

The previous period is calculated to match the current period's duration:

```python
# Current period range
current_min_period = min(filtered_opened['Period'].min(), filtered_closed['Period'].min())
current_max_period = max(filtered_opened['Period'].max(), filtered_closed['Period'].max())
period_range = current_max_period - current_min_period

# Previous period (shifted back by same duration)
previous_min = current_min_period - period_range
previous_max = current_min_period

# Apply SAME filters to previous period data
```

## Database Queries

### Opened Data
- Source tables: all_opened_py, all_opened_cy, all_opened_acquisition
- Includes filters for:
  - Excluded chains (Hoka, Sephora, etc.) with special sub-rules
  - Reopened stores (duration > 365 days or NULL)
  - FirstAppearedDate exclusions
  - Years after 2018
  - Acquisition data from all_active_acquisition

### Closed Data
- Source tables: all_closed_py, all_closed_cy, all_closed_bankruptcy
- Same exclusion filters as opened data
- Years after 2018

## Performance Optimization

### Caching
```python
@st.cache_data(show_spinner=False)
def fetch_net_data():
    # Queries are cached, refreshed daily via session state check
```

### Filter Persistence
- Filters saved to auth cookies
- State restored on page reload
- User's last selections persist across sessions

## Common Issues & Solutions

### Issue: Data mismatch between metrics
**Solution**: Both opened and closed data get identical filters applied
- Date range filter applied first to both
- Then sector, retailers, location filters in order
- Previous period gets same filter sequence

### Issue: Metrics showing wrong values
**Solution**: Check data_from indicator:
- Use filtered_opened only for opened counts
- Use filtered_closed only for closed counts
- Use combined_filtered_data for banner/sector counts

### Issue: Charts not rendering
**Solution**: Ensure combined_filtered_data is passed, not individual datasets

## Integration with Other Refactored Pages

All refactored pages use the same:
- BasePage for common elements
- TabComponents for navigation and layouts
- FilterManager for filter logic
- UIComponents for visual rendering

Pages can be swapped without affecting navigation or functionality.

## Testing the Refactored Page

1. **Navigation**: Click between "Net Openings", "Store Openings", "Store Closures", "Active Stores"
2. **Tab Selection**: Switch between "Base Dashboard", "Compare Retailers", "Compare Sectors"
3. **Filters**: 
   - Change date range (should update all metrics)
   - Select sectors (should filter data)
   - Select retailers (should filter data)
   - Select locations (should filter data)
4. **Metrics**: Verify all 6 metrics update correctly
5. **Previous Period**: Change date range and verify % change calculation
6. **Charts**: Verify charts render with filtered data
7. **Data Table**: Download CSV and verify data matches on-screen

## File Location
`d:\coresight\Latest-SIP-STG\SIP-STG\pages\net.py`

## Next Steps
- Deploy net.py to production
- Update navigation to use net.py (already done in tab_components.py)
- Monitor for any data inconsistencies
- Consider refactoring remaining pages (active.py) if not already done
