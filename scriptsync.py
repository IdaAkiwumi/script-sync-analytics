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

# Refined CSS: Removed redundant border-left (static progress bar lookalike)
st.markdown("""
    <style>
    [data-testid="stHeader"] { background-color: rgba(0,0,0,0) !important; }
    
    .block-container {
        padding-top: 2rem !important;
        padding-bottom: 0rem !important;
    }

    .compact-header {
        margin-top: 0px !important;
        font-family: 'Courier New', Courier, monospace;
        background-color: #262730; 
        padding: 12px 20px; 
        border-radius: 4px;
        color: #ffd600; 
        border: 1px solid #ffd600;
        display: flex;
        justify-content: space-between;
        font-size: 1rem;
        margin-bottom: 15px;
    }

    

    /* Custom styling for progress bars to match brand gold */
    .stProgress > div > div > div > div {
        background-color: #ffd600;
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
    data = {
        'Project': ['Soul Debt', 'Gone Ghost', 'Market Comp A', 'Market Comp B', 'Market Comp C', 'Market Comp D', 'Market Comp E'],
        'Sentiment_Score': [0.75, 0.45, -0.20, 0.10, 0.55, -0.40, 0.25],
        'Market_Potential': [94, 89, 40, 55, 70, 30, 65],
        'Genre': ['Prestige Thriller', 'Action Comedy', 'Horror', 'Drama', 'Sci-Fi', 'Horror', 'Prestige Thriller'],
        'Budget_Tier': ['High', 'Mid', 'Low', 'Mid', 'High', 'Low', 'Mid']
    }
    return pd.DataFrame(data)

# --- 4. SIDEBAR STUDIO CONTROLS ---
df_full = load_placeholder_data()

with st.sidebar:
    st.title("🎬 Strategy Controls")
    st.subheader("📊 Analytical Filters")
    
    all_genres = df_full['Genre'].unique().tolist()
    genre_filter = st.multiselect("Filter by Genre", all_genres, default=all_genres)
    
    st.markdown("---")
    st.markdown("Follow me on:")
    st.markdown(f"LinkedIn → [Ida Akiwumi](https://www.linkedin.com/in/idaa11)")
    
    st.markdown(f"""
        **Developed by Ida Akiwumi**,  
        *Product Architect & Narrative Strategist*
    """)

# --- 5. DATA FILTERING LOGIC ---
df = df_full[df_full['Genre'].isin(genre_filter)]

# --- 6. MAIN INTERFACE ---
st.markdown(f'''
    <div class="compact-header">
        <span>🎬 SCRIPT-SYNC ANALYTICS</span>
        <span>STATUS: <span style="background:#ffd600; color:#000; padding:0 5px; border-radius:3px;">LIVE DATA</span></span>
    </div>
''', unsafe_allow_html=True)

st.markdown("### Translating Narrative Friction into Market ROI")

# --- DYNAMIC METRIC CALCULATIONS ---
if not df.empty:
    raw_sentiment = df['Sentiment_Score'].mean()
    sentiment_pct = (raw_sentiment + 1) / 2 
    sentiment_label = "High" if raw_sentiment > 0.4 else "Moderate" if raw_sentiment > 0 else "Low"
    
    avg_market = int(df['Market_Potential'].mean())
    market_pct = avg_market / 100
    
    count = len(df)
    friction_pct = max(0.1, 1.0 - (count / 10)) 
    saturation = "Low" if count < 3 else "Neutral" if count < 6 else "High"
else:
    sentiment_pct, market_pct, friction_pct = 0, 0, 0
    sentiment_label, avg_market, saturation = "N/A", 0, "N/A"

# --- METRIC CARDS ---
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Avg Sentiment ROI", sentiment_label, delta=f"{len(df)} Active")
    st.progress(sentiment_pct)
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Market Appetite", f"{avg_market}%", delta="Target")
    st.progress(market_pct)
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Genre Friction", saturation, delta="Opportunity")
    st.progress(friction_pct)
    st.markdown('</div>', unsafe_allow_html=True)

# --- 7. VISUALIZATIONS ---

project_palette = px.colors.qualitative.Prism 
genre_color_map = {
    "Prestige Thriller": "#FFD600",
    "Action Comedy": "#FF1493",
    "Horror": "#00F5D4",
    "Drama": "#FF9F1C",
    "Sci-Fi": "#00BBFF"
}

tab1, tab2 = st.tabs(["🎯 Narrative Performance", "📊 Genre Distribution"])

with tab1:
    fig_scatter = px.scatter(
        df, x="Sentiment_Score", y="Market_Potential", 
        size="Market_Potential", color="Project",
        hover_data=["Genre"],
        template="plotly_dark",
        color_discrete_sequence=project_palette 
    )
    fig_scatter.update_traces(marker=dict(opacity=1, line=dict(width=1.5, color='White')))
    fig_scatter.update_layout(
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

with tab2:
    genre_counts = df['Genre'].value_counts().reset_index()
    genre_counts.columns = ['Genre', 'Count']
    
    fig_bar = px.bar(
        genre_counts, x='Genre', y='Count', 
        color='Genre', template="plotly_dark",
        color_discrete_map=genre_color_map,
        title="Portfolio Saturation by Genre"
    )
    fig_bar.update_layout(
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)',
        showlegend=False,
        yaxis=dict(tick0=0, dtick=1)
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with st.expander("📂 Raw Market Intelligence Data"):
    st.dataframe(df, use_container_width=True)

st.markdown("---")
st.caption("Script-Sync Analytics v1.0.0 | Built with Python & Strategic Intent.")