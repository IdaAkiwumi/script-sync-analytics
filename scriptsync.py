"""
PROJECT: Script-Sync Analytics
VERSION: 1.0.0
AUTHOR: Ida Akiwumi
ROLE: Product Architect | Narrative Strategist | Lead Product Designer
TECH STACK: Python, Streamlit, Pandas, Plotly, TextBlob

DESCRIPTION:
A strategic ROI engine for the film and gaming industries. Script-Sync 
translates narrative scripts and movie metadata into actionable data 
visualizations, helping producers identify Blue Ocean market opportunities.

IDEAL FOR:
- Studio Executives & Greenlight Committees
- Creative Operations & Narrative Strategy
- Data Storytelling & Market Saturation Analysis
"""

__author__ = "Ida Akiwumi"
__version__ = "1.0.0"
__license__ = "Proprietary"
__status__ = "Production / Portfolio"

import streamlit as st
import pandas as pd
import plotly.express as px
from textblob import TextBlob
import time

# --- 1. INITIALIZE STATE ---
def init_state():
    if "data_loaded" not in st.session_state:
        st.session_state.data_loaded = False
    if "analysis_results" not in st.session_state:
        st.session_state.analysis_results = None

init_state()

# --- 2. UI SETUP & BRANDING ---
st.set_page_config(
    page_title="Script-Sync Analytics | Designed by Ida Akiwumi", 
    page_icon="🎬",
    layout="wide"
)

# Custom Akiwumi-Standard CSS
st.markdown("""
    <style>
    [data-testid="stHeader"] { background-color: rgba(0,0,0,0) !important; }
    
    .compact-header {
        margin-top: 35px !important;
        font-family: 'Courier New', Courier, monospace;
        background-color: #262730; 
        padding: 10px 20px; 
        border-radius: 4px;
        color: #ffd600; 
        border: 1px solid #ffd600;
        display: flex;
        justify-content: space-between;
    }

    .metric-card {
        background-color: #1e1e1e;
        padding: 20px;
        border-radius: 8px;
        border-left: 10px solid #ffd600;
        margin-bottom: 20px;
        font-family: 'Courier New', Courier, monospace;
    }
    
    .stMetric { color: #ffd600 !important; }
    footer {visibility: hidden;}
    </style>
""", unsafe_allow_html=True)

# --- 3. LOGIC: DATA ENGINE ---
def analyze_sentiment(text):
    return TextBlob(text).sentiment.polarity

@st.cache_data
def load_placeholder_data():
    # Professional sample set for initial portfolio view
    data = {
        'Project': ['Soul Debt', 'Gone Ghost', 'Market Comp A', 'Market Comp B', 'Market Comp C'],
        'Sentiment_Score': [0.75, 0.45, -0.20, 0.10, 0.55],
        'Market_Potential': [94, 89, 40, 55, 70],
        'Genre': ['Prestige Thriller', 'Action Comedy', 'Horror', 'Drama', 'Sci-Fi'],
        'Budget_Tier': ['High', 'Mid', 'Low', 'Mid', 'High']
    }
    return pd.DataFrame(data)

# --- 4. SIDEBAR STUDIO CONTROLS ---
with st.sidebar:
    st.title("🎬 Strategy Controls")
    st.subheader("📊 Analytical Filters")
    genre_filter = st.multiselect("Filter by Genre", ["Thriller", "Comedy", "Sci-Fi", "Drama"], default=[])
    
    st.markdown("---")
    st.markdown("Follow me on:")
    st.markdown(f"LinkedIn $\\rightarrow$ [Ida Akiwumi](https://www.linkedin.com/in/idaa11)")
    
    st.markdown(f"""
        **Developed by {__author__}**,  
        *Product Architect & Narrative Strategist* Specializing in the intersection of code and story.
    """)

# --- 5. MAIN INTERFACE ---
st.markdown(f'''
    <div class="compact-header">
        <span>🎬 SCRIPT-SYNC ANALYTICS</span>
        <span>STATUS: <span style="background:#ffd600; color:#000; padding:0 5px; border-radius:3px;">LIVE DATA</span></span>
    </div>
''', unsafe_allow_html=True)

st.markdown("### Translating Narrative Friction into Market ROI")

# --- DATA VISUALIZATION SECTION ---
df = load_placeholder_data()

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Avg Sentiment ROI", "High", delta="+14% Portfolio")
    st.markdown('</div>', unsafe_allow_html=True)
with col2:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Market Appetite", "92%", delta="Optimal")
    st.markdown('</div>', unsafe_allow_html=True)
with col3:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Genre Friction", "Low", delta="-5% Saturation")
    st.markdown('</div>', unsafe_allow_html=True)

st.subheader("Narrative Archetype Performance")
fig = px.scatter(
    df, x="Sentiment_Score", y="Market_Potential", 
    size="Market_Potential", color="Genre",
    hover_name="Project", template="plotly_dark",
    color_discrete_sequence=['#ffd600', '#ffffff', '#888888', '#444444']
)
st.plotly_chart(fig, use_container_width=True)

# --- TABLE VIEW FOR EXECUTIVES ---
with st.expander("📂 Raw Market Intelligence Data"):
    st.dataframe(df, use_container_width=True)

st.markdown("---")
st.caption("Script-Sync Analytics v1.0.0 | Built with Python & Strategic Intent.")