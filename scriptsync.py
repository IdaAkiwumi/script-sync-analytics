"""
PROJECT: Script-Sync Analytics
VERSION: 1.1.0
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
__version__ = "1.1.0"
__license__ = "Proprietary"
__status__ = "Production / Portfolio"

import streamlit as st
import pandas as pd
import plotly.express as px
from textblob import TextBlob
import os


import ast # Added for genre parsing

# ... (Keep your existing UI/CSS code exactly as is) ...

@st.cache_data
def load_real_data():
    """Attempts to load and merge data from the /data directory."""
    try:
        # 1. Flexible Pathing for Local vs GitHub
        base_path = "data" if os.path.exists("data") else "script-sync-analytics/data"
        movies_path = os.path.join(base_path, "movie_database_movies_2026.csv")
        cast_path = os.path.join(base_path, "movie_database_cast_2026.csv")
        
        if os.path.exists(movies_path):
            # 2. Robust CSV loading (skips malformed lines)
            m_df = pd.read_csv(movies_path, engine='python', on_bad_lines='skip')
            
            # Bridge column names
            m_df = m_df.rename(columns={
                'title': 'Project',
                'vote_average': 'Sentiment_Score',
                'popularity': 'Market_Potential',
                'genres': 'Genre'
            })

            # 3. Genre Cleaning Logic (Unpacks TMDB format)
            def clean_genres(g):
                try:
                    if isinstance(g, str) and "[" in g:
                        items = ast.literal_eval(g)
                        return items[0]['name'] if items else "Unknown"
                    return str(g)
                except:
                    return "Misc"
            
            if 'Genre' in m_df.columns:
                m_df['Genre'] = m_df['Genre'].apply(clean_genres)

            # Optional: Merge with cast
            if os.path.exists(cast_path):
                c_df = pd.read_csv(cast_path, engine='python', on_bad_lines='skip')
                m_df = pd.merge(m_df, c_df[['id', 'name']], on='id', how='left').rename(columns={'name': 'Lead_Talent'})
            
            # Normalize Sentiment Score
            if m_df['Sentiment_Score'].max() > 1:
                m_df['Sentiment_Score'] = (m_df['Sentiment_Score'] - 5) / 5
                
            return m_df
    except Exception as e:
        # Silently log error or use st.sidebar.error(e) for debugging
        return None
    return None

# ... (Keep the rest of your script unchanged) ...



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

@st.cache_data
def load_real_data():
    """Attempts to load and merge data from the /data directory."""
    try:
        # Paths based on your screenshot
        movies_path = "data/movie_database_movies_2026.csv"
        cast_path = "data/movie_database_cast_2026.csv"
        
        if os.path.exists(movies_path):
            m_df = pd.read_csv(movies_path)
            
            # Bridge column names to match your original UI logic
            m_df = m_df.rename(columns={
                'title': 'Project',
                'vote_average': 'Sentiment_Score',
                'popularity': 'Market_Potential',
                'genres': 'Genre'
            })

            # Optional: Merge with cast if it exists
            if os.path.exists(cast_path):
                c_df = pd.read_csv(cast_path)
                m_df = pd.merge(m_df, c_df[['id', 'name']], on='id', how='left').rename(columns={'name': 'Lead_Talent'})
            
            # Normalize Sentiment Score (assuming 1-10 scale in TMDB data)
            if m_df['Sentiment_Score'].max() > 1:
                m_df['Sentiment_Score'] = (m_df['Sentiment_Score'] - 5) / 5
                
            return m_df
    except Exception as e:
        return None
    return None

# --- 4. SIDEBAR STUDIO CONTROLS ---
# Check if real data exists, otherwise default to placeholders
real_df = load_real_data()
df_full = real_df if real_df is not None else load_placeholder_data()

with st.sidebar:
    st.title("🎬 Strategy Controls")
    
    if real_df is not None:
        st.success("Connected to 2026 Market Intelligence")
    else:
        st.info("Using Proprietary Script Placeholders")

    st.subheader("📊 Analytical Filters")
    
    all_genres = df_full['Genre'].unique().tolist()
    genre_filter = st.multiselect("Filter by Genre", all_genres, default=all_genres[:5])
    
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
    
    # Scale market potential to 0-100 for the progress bar
    avg_market_raw = df['Market_Potential'].mean()
    avg_market = int(min(100, avg_market_raw))
    market_pct = avg_market / 100
    
    count = len(df)
    friction_pct = max(0.1, 1.0 - (count / 100)) # Adjusted for larger datasets
    saturation = "Low" if count < 10 else "Neutral" if count < 50 else "High"
else:
    sentiment_pct, market_pct, friction_pct = 0, 0, 0
    sentiment_label, avg_market, saturation = "N/A", 0, "N/A"

# --- METRIC CARDS ---
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Avg Sentiment ROI", sentiment_label, delta=f"{len(df)} Active")
    st.progress(max(0.0, min(1.0, sentiment_pct)))
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Market Appetite", f"{avg_market}%", delta="Target")
    st.progress(max(0.0, min(1.0, market_pct)))
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Genre Friction", saturation, delta="Opportunity")
    st.progress(max(0.0, min(1.0, friction_pct)))
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
        df.head(200), # Cap for performance on large files
        x="Sentiment_Score", y="Market_Potential", 
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
    genre_counts = df['Genre'].value_counts().head(15).reset_index()
    genre_counts.columns = ['Genre', 'Count']
    
    fig_bar = px.bar(
        genre_counts, x='Genre', y='Count', 
        color='Genre', template="plotly_dark",
        title="Portfolio Saturation by Genre"
    )
    fig_bar.update_layout(
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)',
        showlegend=False
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with st.expander("📂 Raw Market Intelligence Data"):
    st.dataframe(df, use_container_width=True)

st.markdown("---")
st.caption("Script-Sync Analytics v1.1.0 | Built with Python & Strategic Intent.")
