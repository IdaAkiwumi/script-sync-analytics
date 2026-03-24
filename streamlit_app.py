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
import os
import ast 
import numpy as np  # Ensure this is here for the 90th percentile math

# --- 1. INITIALIZE STATE ---
def init_state():
    if "data_loaded" not in st.session_state:
        st.session_state.data_loaded = False
    if "analysis_results" not in st.session_state:
        st.session_state.analysis_results = None
    # Storage for saved scenarios
    if "saved_filters" not in st.session_state:
        st.session_state.saved_filters = {}

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
    
    /* High-contrast Slate for WCAG compliance on dark background */
    .metric-caption {
        color: #A3AABF; 
        font-size: 0.9rem;
        font-family: 'Courier New', Courier, monospace;
    }

    .compact-header {
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
        border: 1px solid #333;
    }

    @media print {
        [data-testid="stSidebar"], [data-testid="stHeader"] { display: none !important; }
        
        /* Force Metric Card text to black for white paper */
        .metric-caption, .stMetricValue { color: #000000 !important; }
        .metric-card { background-color: #ffffff !important; border: 1px solid #000 !important; }
        
        .main .block-container { max-width: 100% !important; padding: 0 !important; }
    }
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


# --- 4. SIDEBAR STUDIO CONTROLS ---
real_df = load_real_data()
df_full = real_df if real_df is not None else load_placeholder_data()

# Ensure we track the active scenario name
if "active_scenario_name" not in st.session_state:
    st.session_state.active_scenario_name = "Default (Industry Standard)"

with st.sidebar:
    st.title("🎬 Strategy Controls")
    
    # Visual indicator of current state
    st.markdown(f"**Current View:** `{st.session_state.active_scenario_name}`")
    
    all_genres = sorted([str(g) for g in df_full['Genre'].unique() if pd.notna(g)])
    
    # 4a. Load Existing Filter logic
    if st.session_state.saved_filters:
        def load_scenario():
            selection = st.session_state["scenario_loader"]
            if selection != "-- Select --":
                st.session_state["genre_selector"] = st.session_state.saved_filters[selection]
                st.session_state.active_scenario_name = selection
                # Trigger a toast notification for instant feedback
                st.toast(f"✅ Loaded Scenario: {selection}", icon="🎬")
                st.session_state["scenario_loader"] = "-- Select --"

        st.selectbox(
            "📂 Load Saved Scenario", 
            ["-- Select --"] + list(st.session_state.saved_filters.keys()),
            key="scenario_loader",
            on_change=load_scenario
        )

    # 4b. The Main Filter
    if "genre_selector" not in st.session_state:
        st.session_state["genre_selector"] = [g for g in ["Drama", "Comedy", "Action", "Thriller", "Horror"] if g in all_genres]

    def on_filter_change():
        # If user manually changes filters, indicate the view is now custom
        st.session_state.active_scenario_name = "Custom (Modified)"

    genre_filter = st.multiselect(
        "Filter by Primary Genre", 
        all_genres, 
        key="genre_selector",
        on_change=on_filter_change
    )
    
    # 4c. Save Current Filter logic
    scenario_name = st.text_input("💾 Save Filter State (in session)", placeholder="e.g., Q1 Sci-Fi Push")
    if st.button("Confirm Save"):
        if scenario_name and genre_filter:
            st.session_state.saved_filters[scenario_name] = genre_filter
            st.success(f"Saved '{scenario_name}'")
            st.rerun()
    
    st.info("""
    **Architect's Note:** This dashboard visualizes *Market Volatility* and *Sentiment Trends*. 
    For specific ROI projections, these metrics should be cross-referenced 
    with production budget tiers and IP-attachment status.
    """)
    
    # --- DATA SOURCE INJECTION ---
    with st.expander("📚 Data Sources & Intelligence"):
        st.caption("This engine aggregates market metadata from:")
        st.markdown("""
        - [TMDB Movie Metadata](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata)
        - [2023 930k Movie Dataset](https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies)
        - [Comprehensive TMDB Reviews](https://www.kaggle.com/datasets/rishabhkumar2003/the-movie-database-tmdb-comprehensive-dataset)
        
        **Sourced March 2026.**
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
        <span>🎬 GENRE SYNC: {st.session_state.active_scenario_name.upper()}</span>
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
# --- EMPTY STATE GUARD ---
if df.empty:
    st.warning("⚠️ Strategy Engine Standby: Please select at least one genre in the sidebar to activate market analysis.")
    st.stop() # This prevents the rest of the code from running and crashing
    
# --- DYNAMIC METRIC CALCULATIONS ---
if not df.empty:
    raw_sentiment = df['Sentiment_Score'].mean()
    sentiment_pct = (raw_sentiment + 1) / 2 
    
    # 1. Appetite Logic: Percentile Ranking (The "Spread" Fix)
    # This ranks the current genre heat against the full dataset distribution
    # providing a beautiful spread from 5% to 95%.
    
   
    
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
            <div class="metric-caption">Avg Sentiment ROI</div>
            <div style="color: {sent_color}; font-size: 1.5rem; font-weight: bold; margin: 5px 0;">{sent_label}</div>
            <div style="color: #ffd600; font-size: 0.8rem;">{len(df)} Projects</div>
        </div>
    """, unsafe_allow_html=True)
    st.progress(max(0.0, min(1.0, float(sentiment_pct))))

with col2:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-caption">Market Appetite</div>
            <div style="color: {app_color}; font-size: 1.5rem; font-weight: bold; margin: 5px 0;">{avg_market}%</div>
            <div style="color: #ffd600; font-size: 0.8rem;">Global Target</div>
        </div>
    """, unsafe_allow_html=True)
    st.progress(max(0.0, min(1.0, avg_market / 100.0)))

with col3:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-caption">Genre Market Opportunity</div>
            <div style="color: {opp_color}; font-size: 1.5rem; font-weight: bold; margin: 5px 0;">{opp_label}</div>
            <div style="color: #ffd600; font-size: 0.8rem;">Blue Ocean Potential</div>
        </div>
    """, unsafe_allow_html=True)
    st.progress(max(0.0, min(1.0, float(opportunity_pct))))

# --- 7. VISUALIZATIONS ---

# Injecting extra CSS here to specifically target the tab padding 
# and the gap between caption and chart.
st.markdown("""
    <style>
    [data-testid="stTabPanel"] { padding-top: 0rem !important; }
    .stPlotlyChart { margin-top: -10px !important; }
    
    /* Move Toast to the left side near sidebar */
[data-testid="stToastContainer"] {
    left: 40px !important;
    right: auto !important;
    bottom: 0px !important;
}
    </style>
""", unsafe_allow_html=True)

tab1, tab2 = st.tabs(["🎯 Narrative Performance", "📊 Genre Distribution"])

with tab1:
    display_df = df.copy()

    # Create a comma-separated string of the selected genres
    # This ensures that even when the sidebar is hidden (like in print), 
    # the viewer knows exactly what they are looking at.
    active_genres_str = ", ".join(genre_filter) if genre_filter else "All Genres"

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
        range_x=[-0.05, 0.9], 
        template="plotly_dark",
        size_max=35, 
        height=400 
    )
    
    fig_scatter.update_traces(marker=dict(opacity=0.7, line=dict(width=1, color='White')))
    
    fig_scatter.update_layout(
        showlegend=False, 
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, t=5, b=0), 
        xaxis_title="Sentiment ROI",
        yaxis_title="Market Appetite Index"
    )
    
    # --- UPDATED METADATA LINE ---
    # This combines the competitor count and the active genre list
    st.markdown(f"""
        <p style="color:#888; font-size:0.8rem; margin-bottom:-15px; padding-left:2px;">
            Showing <strong>{len(display_df)}</strong> market competitors in: 
            <span style="color:#dc3545;">{active_genres_str}</span>
        </p>
    """, unsafe_allow_html=True)
    
    st.plotly_chart(fig_scatter, use_container_width=True, key="performance_scatter")

with tab2:
    genre_counts = df['Genre'].value_counts().reset_index()
    genre_counts.columns = ['Genre', 'Count']
    
    fig_bar = px.bar(
        genre_counts, x='Genre', y='Count', 
        color='Genre', template="plotly_dark",
        height=400,
        title="Active Market Saturation (Selected Genres)"
    )
    # Reduced top margin from 22 to 10 to tighten space under the tab
    fig_bar.update_layout(margin=dict(l=0, r=0, t=10, b=0), showlegend=False, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig_bar, use_container_width=True, key="genre_distribution_bar")

with st.expander("📂 View Full Intelligence Ledger"):
    # FIXED WIDTH HERE:
    st.dataframe(df, use_container_width=True)

st.caption(f"Genre Sync Analytics v{__version__} | Strategic Intelligence by Ida Akiwumi.")

