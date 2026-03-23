"""
PROJECT: Script-Sync Analytics
VERSION: 1.1.5
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
__version__ = "1.1.5"
__license__ = "Proprietary"
__status__ = "Production / Portfolio"

import streamlit as st
import pandas as pd
import plotly.express as px
from textblob import TextBlob
import os
import ast 

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

st.markdown("""
    <style>
    [data-testid="stHeader"] { background-color: rgba(0,0,0,0) !important; }
    
    .block-container {
        padding-top: 1.5rem !important;
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

    .metric-card {
        background-color: #1e1e1e;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 5px;
        font-family: 'Courier New', Courier, monospace;
        border: 1px solid #333;
    }

    .stProgress > div > div > div > div {
        background-color: #ffd600;
    }
    
    .stMetric { color: #ffd600 !important; }
    footer {visibility: hidden;}
    </style>
""", unsafe_allow_html=True)

# --- 3. LOGIC: DATA ENGINE ---
@st.cache_data
def load_placeholder_data():
    data = {
        'Project': ['Soul Debt', 'Gone Ghost', 'Market Comp A', 'Market Comp B', 'Market Comp C'],
        'Sentiment_Score': [0.75, 0.45, -0.20, 0.10, 0.30],
        'Market_Potential': [94, 89, 40, 55, 60],
        'Genre': ['Thriller', 'Comedy', 'Horror', 'Drama', 'Action'],
    }
    return pd.DataFrame(data)

@st.cache_data
def load_real_data():
    try:
        base_path = "data" if os.path.exists("data") else "script-sync-analytics/data"
        movies_path = os.path.join(base_path, "movie_database_movies_2026.csv")
        cast_path = os.path.join(base_path, "movie_database_cast_2026.csv")
        
        if os.path.exists(movies_path):
            m_df = pd.read_csv(movies_path, engine='python', on_bad_lines='skip')
            m_df = m_df.rename(columns={
                'title': 'Project',
                'vote_average': 'Sentiment_Score',
                'popularity': 'Market_Potential',
                'genres': 'Genre'
            })

            def clean_genres(g):
                try:
                    if not g or pd.isna(g): return "Unknown"
                    if isinstance(g, str) and "[" in g:
                        items = ast.literal_eval(g)
                        return items[0]['name'] if items else "Other"
                    if isinstance(g, str) and "," in g:
                        return g.split(',')[0].strip()
                    return str(g)
                except:
                    return "Misc"
            
            if 'Genre' in m_df.columns:
                m_df['Genre'] = m_df['Genre'].apply(clean_genres)

            if os.path.exists(cast_path):
                c_df = pd.read_csv(cast_path, engine='python', on_bad_lines='skip')
                if 'id' in m_df.columns and 'id' in c_df.columns:
                    m_df = pd.merge(m_df, c_df[['id', 'name']], on='id', how='left').rename(columns={'name': 'Lead_Talent'})
            
            if 'Sentiment_Score' in m_df.columns and m_df['Sentiment_Score'].max() > 1:
                m_df['Sentiment_Score'] = (m_df['Sentiment_Score'] - 5) / 5
                
            return m_df
    except Exception:
        return None
    return None

# --- 4. SIDEBAR STUDIO CONTROLS ---
real_df = load_real_data()
df_full = real_df if real_df is not None else load_placeholder_data()

with st.sidebar:
    st.title("🎬 Strategy Controls")
    
    all_genres = sorted([str(g) for g in df_full['Genre'].unique() if pd.notna(g)])
    default_selection = [g for g in ["Drama", "Comedy", "Action", "Thriller", "Horror"] if g in all_genres]
    if not default_selection:
        default_selection = all_genres[:5]

    genre_filter = st.multiselect("Filter by Primary Genre", all_genres, default=default_selection)
    
    st.markdown("---")
    st.markdown(f"LinkedIn → [Ida Akiwumi](https://www.linkedin.com/in/idaa11)")
    st.markdown(f"**Designed by Ida Akiwumi** \n*Product Architect*")

# --- 5. DATA FILTERING LOGIC ---
df = df_full[df_full['Genre'].isin(genre_filter)]

# --- 6. MAIN INTERFACE ---
st.markdown(f'''
    <div class="compact-header">
        <span>🎬 SCRIPT-SYNC ANALYTICS</span>
        <span>STATUS: <span style="background:#ffd600; color:#000; padding:0 5px; border-radius:3px;">LIVE MARKET DATA</span></span>
    </div>
''', unsafe_allow_html=True)

# --- DYNAMIC METRIC CALCULATIONS ---
if not df.empty:
    raw_sentiment = df['Sentiment_Score'].mean()
    sentiment_pct = (raw_sentiment + 1) / 2 
    avg_market = int(min(100, df['Market_Potential'].mean()))
    
    # DYNAMIC FRICTION CALC: Ratio of selected data vs total dataset
    saturation_ratio = len(df) / len(df_full)
    friction_pct = max(0.05, min(0.95, 1.0 - saturation_ratio))
    saturation_label = "Low" if saturation_ratio < 0.1 else "Neutral" if saturation_ratio < 0.3 else "High"
else:
    sentiment_pct, avg_market, friction_pct, saturation_label = 0, 0, 0, "N/A"

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Avg Sentiment ROI", "High" if sentiment_pct > 0.6 else "Moderate" if sentiment_pct > 0.4 else "Neutral", delta=f"{len(df)} Projects")
    st.progress(max(0.0, min(1.0, sentiment_pct)))
with col2:
    st.metric("Market Appetite", f"{avg_market}%", delta="Global Target")
    st.progress(avg_market/100 if avg_market > 0 else 0.0)
with col3:
    st.metric("Genre Friction", saturation_label, delta="Market Opportunity")
    st.progress(friction_pct)

# --- 7. VISUALIZATIONS ---
tab1, tab2 = st.tabs(["🎯 Narrative Performance", "📊 Genre Distribution"])

with tab1:
    fig_scatter = px.scatter(
        df.head(600), 
        x="Sentiment_Score", 
        y="Market_Potential", 
        size="Market_Potential", 
        color="Genre", 
        hover_name="Project",
        hover_data=["Lead_Talent", "Genre"] if "Lead_Talent" in df.columns else ["Genre"],
        template="plotly_dark",
        size_max=40, 
        height=700 
    )
    fig_scatter.update_traces(marker=dict(opacity=0.7, line=dict(width=1, color='White')))
    fig_scatter.update_layout(
        showlegend=False, 
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, t=20, b=0)
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

with tab2:
    # REPAIRED: Shows ALL selected genres in the bar chart
    genre_counts = df['Genre'].value_counts().reset_index()
    genre_counts.columns = ['Genre', 'Count']
    
    fig_bar = px.bar(
        genre_counts, x='Genre', y='Count', 
        color='Genre', template="plotly_dark",
        height=500,
        title="Active Market Saturation (Selected Genres)"
    )
    fig_bar.update_layout(showlegend=False, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig_bar, use_container_width=True)

with st.expander("📂 View Full Intelligence Ledger"):
    st.dataframe(df, use_container_width=True)

st.caption(f"Script-Sync Analytics v{__version__} | Strategic Intelligence by Ida Akiwumi.")