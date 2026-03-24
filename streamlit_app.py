"""
PROJECT: Genre Sync Analytics
VERSION: 1.0.0
AUTHOR: Ida Akiwumi
ROLE: Product Architect | Narrative Strategist | Lead Product Designer
TECH STACK: Python, Streamlit, Pandas, Plotly, TextBlob

DESCRIPTION:
A strategic ROI engine for the film and gaming industries. Genre Sync 
translates movie metadata into actionable data 
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
    page_title="Genre Sync Analytics | Designed by Ida Akiwumi", 
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
        'Popularity_Score': [94, 89, 40, 55, 60],
        'Genre': ['Thriller', 'Comedy', 'Horror', 'Drama', 'Action'],
    }
    return pd.DataFrame(data)

@st.cache_data
def load_real_data():
    try:
        base_path = "data" if os.path.exists("data") else "genre-sync-analytics/data"
        movies_path = os.path.join(base_path, "movie_database_movies_2026.csv")
        cast_path = os.path.join(base_path, "movie_database_cast_2026.csv")
        
        if os.path.exists(movies_path):
            m_df = pd.read_csv(movies_path, engine='python', on_bad_lines='skip')
            m_df = m_df.rename(columns={
                'title': 'Project',
                'vote_average': 'Sentiment_Score',
                'popularity': 'Popularity_Score',
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
    
    
    # --- DATA SOURCE INJECTION START ---
    st.markdown("---")
    with st.expander("📚 Data Sources & Intelligence"):
        st.caption("This engine aggregates market metadata from:")
        st.markdown("""
        - [TMDB Movie Metadata](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata)
        - [2023 930k Movie Dataset](https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies)
        - [Comprehensive TMDB Reviews](https://www.kaggle.com/datasets/rishabhkumar2003/the-movie-database-tmdb-comprehensive-dataset)
        
        **Sourced March 2026.**
        """)
    # --- DATA SOURCE INJECTION END ---
    st.info("""
    **Architect's Note:** This dashboard visualizes *Market Volatility* and *Sentiment Trends*. 
    For specific ROI projections, these metrics should be cross-referenced 
    with production budget tiers and IP-attachment status.
    """)
    
    st.markdown("---")
    st.markdown("Follow me on:")
    st.markdown(f"LinkedIn $\\rightarrow$ [Ida Akiwumi](https://www.linkedin.com/in/idaa11)")
    
    st.markdown(f"""
        **Developed by {__author__}**,  
        *Product Architect & Narrative Strategist* Specializing in the intersection of code and story.
    """)

# --- 5. DATA FILTERING LOGIC ---
df = df_full[df_full['Genre'].isin(genre_filter)]

# --- 6. MAIN INTERFACE ---
st.markdown(f'''
    <div class="compact-header">
        <span>🎬 GENRE SYNC ANALYTICS</span>
        <span>STATUS: <span style="background:#ffd600; color:#000; padding:0 5px; border-radius:3px;">SPRING 2026 MARKET DATA</span></span>
    </div>
''', unsafe_allow_html=True)
# --- QUICK START GUIDE ---
with st.expander("ℹ️ STRATEGY GUIDE: How to use Genre Sync"):
    st.markdown("""
    ### **Objective**
    Identify **Blue Ocean** opportunities by finding genres with **High ROI** and **High Market Opportunity**.
    
    ### **User Journey**
    1.  **Filter by Primary Genre:** Use the sidebar to select your target market (e.g., *Thriller* or *Sci-Fi*).
    2.  **Evaluate Market Appetite:** Check the "Heat Index"—is the global audience currently 'hungry' for this content?
    3.  **Assess Market Opportunity:** A high bar here indicates a **Blue Ocean** gap where your narrative can stand out without fighting "Red Ocean" saturation.
    4.  **Analyze Comps:** Hover over the bubbles in the **Narrative Performance** tab to see the specific projects and talent currently defining your selected market's ROI.
    """)

# --- DYNAMIC METRIC CALCULATIONS ---
if not df.empty:
    raw_sentiment = df['Sentiment_Score'].mean()
    sentiment_pct = (raw_sentiment + 1) / 2 
    
    # 1. Appetite Logic: Percentile Ranking (The "Spread" Fix)
    # This ranks the current genre heat against the full dataset distribution
    # providing a beautiful spread from 5% to 95%.
    
    import numpy as np
    
    # Get the 90th percentile of your filtered genre
    genre_90th = df['Popularity_Score'].quantile(0.90)
    
    # Find where that score sits as a percentile of the ENTIRE dataset
    # (e.g., if genre_90th is better than 80% of all movies, the score is 80)
    market_percentile = (df_full['Popularity_Score'] < genre_90th).mean()
    
    # Convert to 0-100 scale and apply a slight boost to keep the "Heat" feeling high
    avg_market = int(market_percentile * 100)
    
    # Ensure a healthy minimum and maximum for UI impact
    avg_market = max(2, min(100, avg_market))
    
    # 2. Genre Market Opportunity Logic
    # We measure 'Market Gap' (The Opportunity)
    saturation_ratio = len(df) / len(df_full)
    opportunity_pct = max(0.05, min(0.95, 1.0 - saturation_ratio))
    
    # Labeling: High Opportunity = Blue Ocean (Green)
    if opportunity_pct > 0.80: 
        opp_label = "High"      # Blue Ocean
        opp_color = "#28a745"   # Green
    elif opportunity_pct > 0.50: 
        opp_label = "Moderate" 
        opp_color = "#ffc107"   # Yellow
    else:                      
        opp_label = "Low"       # Saturated Market
        opp_color = "#dc3545"   # Red
        
   
else:
    # Set default values for empty state to prevent NameErrors
    sentiment_pct, avg_market, opportunity_pct = 0, 0, 0
    opp_label, opp_color = "N/A", "#888"

# --- DYNAMIC COLOR MAPPING (Sentiment & Appetite only) ---
# 1. Sentiment Color
if sentiment_pct > 0.6: 
    sent_color, sent_label = "#28a745", "High"
elif sentiment_pct > 0.4: 
    sent_color, sent_label = "#ffc107", "Moderate"
else: 
    sent_color, sent_label = "#888", "Neutral"

# 2. Appetite Color
if avg_market > 80: 
    app_color = "#28a745"
elif avg_market > 50: 
    app_color = "#ffc107"
else: 
    app_color = "#dc3545"




col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"""
        <div class="metric-card">
            <div style="color: #888; font-size: 0.9rem;">Avg Sentiment ROI</div>
            <div style="color: {sent_color}; font-size: 1.5rem; font-weight: bold; margin: 5px 0;">{sent_label}</div>
            <div style="color: #ffd600; font-size: 0.8rem;">{len(df)} Projects</div>
        </div>
    """, unsafe_allow_html=True)
    st.progress(max(0.0, min(1.0, float(sentiment_pct))))

with col2:
    st.markdown(f"""
        <div class="metric-card">
            <div style="color: #888; font-size: 0.9rem;">Market Appetite</div>
            <div style="color: {app_color}; font-size: 1.5rem; font-weight: bold; margin: 5px 0;">{avg_market}%</div>
            <div style="color: #ffd600; font-size: 0.8rem;">Global Target</div>
        </div>
    """, unsafe_allow_html=True)
    st.progress(max(0.0, min(1.0, avg_market / 100.0)))

with col3:
    st.markdown(f"""
        <div class="metric-card">
            <div style="color: #888; font-size: 0.9rem;">Genre Market Opportunity</div>
            <div style="color: {opp_color}; font-size: 1.5rem; font-weight: bold; margin: 5px 0;">{opp_label}</div>
            <div style="color: #ffd600; font-size: 0.8rem;">Blue Ocean Potential</div>
        </div>
    """, unsafe_allow_html=True)
    st.progress(max(0.0, min(1.0, float(opportunity_pct))))

# --- 7. VISUALIZATIONS ---
tab1, tab2 = st.tabs(["🎯 Narrative Performance", "📊 Genre Distribution"])

with tab1:
    # SHIFT LOGIC: Starts at 5/10 mark. 
    # Added a 'clip' to Popularity to prevent outliers from ruining the scale
    display_df = df.head(600).copy()
    
    # Optional: If your chart looks 'squashed' at the bottom because of one big movie, 
    # uncomment the line below to cap the visual height.
    # display_df['Popularity_Score'] = display_df['Popularity_Score'].clip(upper=display_df['Popularity_Score'].quantile(0.95))

    fig_scatter = px.scatter(
        display_df, 
        x="Sentiment_Score", 
        y="Popularity_Score", 
        size="Popularity_Score", 
        color="Genre", 
        hover_name="Project",
        hover_data={
            "Sentiment_Score": ":.2f",
            "Popularity_Score": ":.1f",
            "Genre": True,
            "Lead_Talent": True if "Lead_Talent" in display_df.columns else False
        },
        range_x=[-0.05, 0.9], # Cleanly shows 5.0 to 9.5 ratings
        template="plotly_dark",
        size_max=35, 
        height=400 
    )
    
    fig_scatter.update_traces(marker=dict(opacity=0.7, line=dict(width=1, color='White')))
    
    fig_scatter.update_layout(
        showlegend=False, 
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, t=20, b=0),
        xaxis_title="Sentiment ROI",
        yaxis_title="Market Appetite (Product Score Volatility Index)"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

with tab2:
    # REPAIRED: Shows ALL selected genres in the bar chart
    genre_counts = df['Genre'].value_counts().reset_index()
    genre_counts.columns = ['Genre', 'Count']
    
    fig_bar = px.bar(
        genre_counts, x='Genre', y='Count', 
        color='Genre', template="plotly_dark",
        height=400,
        title="Active Market Saturation (Selected Genres)"
    )
    fig_bar.update_layout(showlegend=False, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig_bar, use_container_width=True)

with st.expander("📂 View Full Intelligence Ledger"):
    st.dataframe(df, use_container_width=True)

st.caption(f"Genre Sync Analytics v{__version__} | Strategic Intelligence by Ida Akiwumi.")