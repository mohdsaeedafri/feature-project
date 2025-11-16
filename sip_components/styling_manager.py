"""
StylingManager - Consistent CSS styling and responsive design
"""
import streamlit as st

class StylingManager:
    @staticmethod
    def apply_global_styles():
        """Apply global CSS styles for consistent UI"""
        st.markdown("""
            <style>
            /* Main container styling */
            .block-container {
                max-width: 1300px !important;
                margin-left: auto !important;
                margin-right: auto !important;
                padding-left: 0 !important;
                padding-right: 0 !important;
            }
            .main {
                padding-left: 0 !important;
                padding-right: 0 !important;
            }
            
            /* Remove ALL margin/padding from Streamlit app wrappers/content */
            .stApp, .main, .css-18e3th9, .css-1y0tads, main {
                padding: 0 !important;
                margin: 0 !important;
            }
            
            /* Remove top gutter above content for custom header */
            .css-18e3th9 {
                padding-top: 85px !important; /* match header height exactly */
                margin-top: 0 !important;
            }

            /* Remove Streamlit main bar/header/footer */
            header[data-testid="stHeader"],
            footer { display: none !important; }
            #MainMenu { visibility: hidden !important; }

            body, .stApp { background: #fff !important; }
            
            /* Custom horizontal rule */
            .custom-hr {
                border: none;
                border-top: 1px solid #808080;
                width: 100%;
                margin: -10px;
                padding: 0;
                margin-left: auto;
                margin-right: auto;
            }
            
            /* Custom horizontal rule variant */
            .custom-hr1 {
                border: none;
                border-top: 1px solid #808080;
                width: 100%;
                margin-top: 0px;
                margin-bottom: 10px;
                padding: 0;
                margin-left: auto;
                margin-right: auto;
            }
            
            /* Multiselect tag styling */
            span[data-baseweb="tag"] {
                background-color: #d62e2f !important;
                color: white !important;
            }
            
            /* Hide Streamlit cookies controller wrapper if it has 0 height */
            .st-emotion-cache-8atqhb.e1mlolmg0[style*="height:0"], 
            .st-emotion-cache-1tvzk6f[data-testid="stCustomComponentV1"][height="0"] {
                display: none !important;
                visibility: hidden !important;
                padding: 0 !important;
                margin: 0 !important;
                height: 0 !important;
                min-height: 0 !important;
                max-height: 0 !important;
            }
            
            /* Reduce gap in multiselect components */
            .st-emotion-cache-wfksaw {
                gap: 0.2rem !important;
            }
            
            /* Hide header action elements (link/share icon) */
            [data-testid="stHeaderActionElements"] {
                display: none !important;
                visibility: hidden !important;
            }
            </style>
        """, unsafe_allow_html=True)
        
    @staticmethod
    def hide_streamlit_elements():
        """Hide default Streamlit elements"""
        hide_streamlit_style = """
        <style>
        #MainMenu {visibility: hidden;}
        .css-1y0tads, .block-container {
            padding-top: 2.1rem !important;
        }
        </style>
        """
        st.markdown(hide_streamlit_style, unsafe_allow_html=True)
        
    @staticmethod
    def apply_bootstrap():
        """Apply Bootstrap CSS for additional styling options"""
        st.markdown('<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">', unsafe_allow_html=True)
        
    @staticmethod
    def render_horizontal_line(class_name="custom-hr"):
        """Render a standardized horizontal line"""
        st.markdown(f'<hr class="{class_name}">', unsafe_allow_html=True)
        
    @staticmethod
    def apply_metric_card_styles():
        """Apply styles for metric cards"""
        st.markdown("""
            <style>
            .metric-card h1 {
                font-size: 40px;
                text-align: left;
                margin-bottom: 0px;
            }
            .metric-card h6 {
                text-align: left;
                margin-top: 0px;
            }
            </style>
        """, unsafe_allow_html=True)