# Version 2.1.4
import streamlit as st
from pathlib import Path

def include_html(file_path):
    """Include HTML file content in Streamlit app"""
    with open(file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    st.markdown(html_content, unsafe_allow_html=True)