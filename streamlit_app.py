"""
PROJECT: Genre Sync Analytics
VERSION: 1.0.2
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
__version__ = "1.0.2"
__license__ = "Proprietary"
__status__ = "Production / Portfolio"

import streamlit as st
import pandas as pd
import plotly.express as px
import os
import ast 
import numpy as np
import json

# --- 1. INITIALIZE STATE ---
def init_state():
    if "data_loaded" not in st.session_state:
        st.session_state.data_loaded = False
    if "analysis_results" not in st.session_state:
        st.session_state.analysis_results = None
    # Storage for saved scenarios
    if "saved_filters" not in st.session_state:
        st.session_state.saved_filters = {}
    # First visit tracking for welcome message
    if "first_visit" not in st.session_state:
        st.session_state.first_visit = True
    # Track if user has interacted with filters
    if "user_has_interacted" not in st.session_state:
        st.session_state.user_has_interacted = False
    # Default genres for comparison
    if "default_genres" not in st.session_state:
        st.session_state.default_genres = None

init_state()

# --- 2. UI SETUP & BRANDING ---
st.set_page_config(
    page_title="Genre Sync Analytics | Designed by Ida Akiwumi", 
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# SEO-friendly static header that renders early
st.markdown("""
<div style="display:none;" aria-hidden="true">
Genre Sync Analytics: Film industry ROI dashboard for genre strategy, 
market sentiment analysis, and blue ocean opportunity identification. 
A product analytics tool for studio executives and film producers by Ida Akiwumi.
</div>
""", unsafe_allow_html=True)

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
    
    /* Welcome banner styling */
    .welcome-banner {
        background: linear-gradient(135deg, #262730 0%, #1a1a2e 100%);
        border: 1px solid #ffd600;
        border-radius: 8px;
        padding: 20px;
        margin-bottom: 15px;
    }
    
    .welcome-banner h3 {
        color: #ffd600;
        margin: 0 0 10px 0;
    }
    
    .welcome-banner p {
        color: #A3AABF;
        margin: 0;
        line-height: 1.6;
    }
    
    /* Empty state styling */
    .empty-state {
        text-align: center;
        padding: 40px;
        background: #262730;
        border-radius: 8px;
        margin: 20px 0;
        border: 1px dashed #ffd600;
    }
    
    .empty-state h2 {
        color: #ffd600;
        margin-bottom: 10px;
    }
    
    .empty-state p {
        color: #A3AABF;
        margin: 5px 0;
    }

    @media print {
        [data-testid="stSidebar"], [data-testid="stHeader"] { display: none !important; }
        
        /* Force Metric Card text to black for white paper */
        .metric-caption, .stMetricValue { color: #000000 !important; }
        .metric-card { background-color: #ffffff !important; border: 1px solid #000 !important; }
        
        .main .block-container { max-width: 100% !important; padding: 0 !important; }
    }
    
    /* Mobile responsiveness */
    @media (max-width: 768px) {
        .metric-card { font-size: 0.85rem !important; padding: 10px !important; }
        .compact-header { flex-direction: column !important; gap: 8px; font-size: 0.9rem; }
        .welcome-banner { padding: 15px; }
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
        'Lead_Talent': ['John Doe', 'Jane Smith', 'Ensemble', 'Chris Evans', 'Ensemble']
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
            
            # FIX: Remove any duplicate columns immediately after loading
            m_df = m_df.loc[:, ~m_df.columns.duplicated(keep='first')]
            
            # Rename columns if they exist with original names (and target doesn't exist)
            rename_map = {
                'title': 'Project',
                'vote_average': 'Sentiment_Score',
                'popularity': 'Popularity_Score',
                'genres': 'Genre'
            }
            for old_name, new_name in rename_map.items():
                if old_name in m_df.columns and new_name not in m_df.columns:
                    m_df = m_df.rename(columns={old_name: new_name})

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

            # Only merge cast data if Lead_Talent doesn't already exist
            if 'Lead_Talent' not in m_df.columns and os.path.exists(cast_path):
                c_df = pd.read_csv(cast_path, engine='python', on_bad_lines='skip')
                # FIX: Remove duplicate columns from cast data too
                c_df = c_df.loc[:, ~c_df.columns.duplicated(keep='first')]
                if 'id' in m_df.columns and 'id' in c_df.columns:
                    m_df = pd.merge(m_df, c_df[['id', 'name']], on='id', how='left')
                    if 'name' in m_df.columns:
                        m_df = m_df.rename(columns={'name': 'Lead_Talent'})
            
            if 'Sentiment_Score' in m_df.columns:
                if m_df['Sentiment_Score'].max() > 1:
                    m_df['Sentiment_Score'] = (m_df['Sentiment_Score'] - 5) / 5
            
            # FIX: Final safety checks - ensure no duplicate columns
            m_df = m_df.loc[:, ~m_df.columns.duplicated(keep='first')]
            
            # FIX: Ensure Lead_Talent exists
            if 'Lead_Talent' not in m_df.columns:
                m_df['Lead_Talent'] = 'Ensemble'
            
            # Ensure required columns exist with defaults
            required_cols = {
                'Project': 'Unknown',
                'Genre': 'Other',
                'Sentiment_Score': 0.5,
                'Popularity_Score': 50,
                'Lead_Talent': 'Ensemble'
            }
            for col, default in required_cols.items():
                if col not in m_df.columns:
                    m_df[col] = default
                
            return m_df
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None


def truncate_title(title, max_words=6):
    """Truncate title to max_words and add ellipsis if longer"""
    if pd.isna(title):
        return "Unknown"
    words = str(title).split()
    if len(words) > max_words:
        return ' '.join(words[:max_words]) + '...'
    return str(title)


# --- 4. SIDEBAR STUDIO CONTROLS ---

# Branded loading state
with st.spinner('🎬 Loading Market Intelligence...'):
    real_df = load_real_data()
    df_full = real_df if real_df is not None else load_placeholder_data()

# FIX: Ensure df_full has no duplicate columns before any operations
df_full = df_full.loc[:, ~df_full.columns.duplicated(keep='first')]

if "active_scenario_name" not in st.session_state:
    st.session_state.active_scenario_name = "Default"

# Decoupled state for the expander
if "import_expanded" not in st.session_state:
    st.session_state.import_expanded = False

with st.sidebar:
    st.markdown(f"**Current View:** `{st.session_state.active_scenario_name}`")
    
    all_genres = sorted([str(g) for g in df_full['Genre'].unique() if pd.notna(g)])
    
    # Store default genres for interaction detection
    if st.session_state.default_genres is None:
        st.session_state.default_genres = [g for g in ["Drama", "Comedy", "Action", "Thriller", "Horror"] if g in all_genres]
    
    # --- 4a. IMPORT DROPDOWN (AUTO-CLOSING) ---
    def handle_import():
        file = st.session_state.scenario_uploader
        if file is not None:
            try:
                imported_data = json.load(file)
                if isinstance(imported_data, dict):
                    st.session_state.saved_filters.update(imported_data)
                    st.toast(f"Imported {len(imported_data)} scenarios!", icon="📂")
                    st.session_state.import_expanded = False
                    # Mark user as having interacted
                    st.session_state.user_has_interacted = True
                    st.session_state.first_visit = False
                else:
                    st.error("Invalid JSON structure.")
            except Exception as e:
                st.error(f"Upload error: {e}")

    with st.expander("📤 Import Scenarios", expanded=st.session_state.import_expanded):
        st.file_uploader(
            "Upload .json file", 
            type="json", 
            key="scenario_uploader", 
            on_change=handle_import
        )

    # --- 4b. SELECTION & FILTERING ---
    if st.session_state.saved_filters:
        def load_scenario_callback():
            sel = st.session_state["scenario_loader"]
            if sel != "-- Select --":
                st.session_state["genre_selector"] = st.session_state.saved_filters[sel]
                st.session_state.active_scenario_name = sel
                st.toast(f"View Switched: {sel}")
                # Mark user as having interacted
                st.session_state.user_has_interacted = True
                st.session_state.first_visit = False

        st.selectbox(
            "📂 Load Saved Scenario", 
            ["-- Select --"] + list(st.session_state.saved_filters.keys()),
            key="scenario_loader",
            on_change=load_scenario_callback
        )

    # Callback to track user interaction with genre filter
    def on_genre_change():
        st.session_state.active_scenario_name = "Custom (Modified)"
        # Check if user has changed from defaults
        current_selection = st.session_state.get("genre_selector", [])
        if set(current_selection) != set(st.session_state.default_genres):
            st.session_state.user_has_interacted = True
            st.session_state.first_visit = False

    # The Main Multiselect
    if "genre_selector" not in st.session_state:
        st.session_state["genre_selector"] = st.session_state.default_genres.copy()

    genre_filter = st.multiselect(
        "Filter by Primary Genre", 
        all_genres, 
        key="genre_selector",
        on_change=on_genre_change
    )
    
    # --- 4c. SCENARIO MANAGEMENT ---
    st.subheader("💾 Scenario Management")
    
    scenario_name = st.text_input("Scenario Name", placeholder="e.g., Q1 Sci-Fi Push")
    
    # Disable save button when invalid
    save_disabled = not scenario_name or not genre_filter
    
    if st.button("💾 Save Scenario", use_container_width=True, disabled=save_disabled):
        st.session_state.saved_filters[scenario_name] = genre_filter
        st.session_state.active_scenario_name = scenario_name
        st.session_state.user_has_interacted = True
        st.session_state.first_visit = False
        st.success(f"Saved '{scenario_name}'")
        st.rerun()
    

    # Export & Clear
    if st.session_state.saved_filters:
        st.download_button(
            label="📥 Export Scenarios (.json)",
            file_name="genre_sync_scenarios.json",
            mime="application/json",
            data=json.dumps(st.session_state.saved_filters),
            use_container_width=True
        )
        if st.button("🗑️ Clear All Scenarios", use_container_width=True, type="secondary"):
            st.session_state.saved_filters = {}
            st.session_state.active_scenario_name = "Default (Industry Standard)"
            st.rerun()
    else:
        # Empty state hint
        st.info("💡 Save your first scenario to unlock Export features")
        
    # Export options guidance
    with st.expander("📄 Export Options"):
        st.markdown("""
        **Print Report:** Use `Ctrl+P` / `Cmd+P` to print this view  
        **Export Charts:** Hover any chart → press camera icon to 'Download plot as PNG'  
        **Export Data:** Use the 'Full Intelligence Ledger' expander below
        """)    
    
    st.markdown("---")
    
    st.markdown("""
    <div class="sidebar-note" style="margin-bottom: 20px;">
        <strong>Architect's Note:</strong> This dashboard visualizes <em>Market Volatility</em> and <em>Sentiment Trends</em>.
    For specific ROI projections, these metrics should be cross-referenced 
    with production budget tiers and IP status.
   </div>
""", unsafe_allow_html=True)
    
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
df = df_full[df_full['Genre'].isin(genre_filter)].copy()

# FIX: Ensure filtered df also has no duplicate columns
df = df.loc[:, ~df.columns.duplicated(keep='first')]

# --- 6. MAIN INTERFACE ---
st.markdown(f'''
    <div class="compact-header">
        <span>🎬 GENRE SYNC: {st.session_state.active_scenario_name.upper()}</span>
        <span>STATUS: <span style="background:#ffd600; color:#000; padding:0 5px; border-radius:3px;">SPRING 2026 MARKET DATA</span></span>
    </div>
''', unsafe_allow_html=True)

# --- AUTO-DISMISSING WELCOME MESSAGE WITH STRATEGY GUIDE REFERENCE ---
# Only show if first visit AND user hasn't interacted with filters
if st.session_state.first_visit and not st.session_state.user_has_interacted:
    st.info("🎬 **Welcome to Genre Sync Analytics** — Identify Blue Ocean opportunities by analyzing genre sentiment, market appetite, and saturation. Use the sidebar to filter genres and save scenarios. Need help? Expand the **Strategy Guide** below.")

# --- QUICK START GUIDE (Always collapsed, referenced in welcome message) ---
with st.expander("ℹ️ STRATEGY GUIDE: How to use Genre Sync"):
    st.markdown("""
    ### **Objective**
    Identify **Blue Ocean** opportunities by finding genres with **High ROI** and **High Market Opportunity**.
    
    ### **User Journey**
    1.  **Filter by Primary Genre:** Use the sidebar to select your target market (e.g., *Thriller* or *Sci-Fi*).
    2.  **Evaluate Market Appetite:** Check the "Heat Index"—is the global audience currently 'hungry' for this content?
    3.  **Assess Market Opportunity:** A high bar here indicates a **Blue Ocean** gap where your narrative can stand out without fighting "Red Ocean" saturation.
    4.  **Analyze Comps:** Hover over the bubbles in the **Narrative Performance** tab to see the specific projects and talent currently defining your selected market's ROI.
    
    ### **Key Metrics Explained**
    | Metric | Green | Yellow | Red |
    |--------|-------|--------|-----|
    | Sentiment ROI | High audience approval | Mixed reception | Poor reception |
    | Market Appetite | Hot trending genre | Moderate interest | Low demand |
    | Market Opportunity | Blue Ocean (low competition) | Competitive | Red Ocean (saturated) |
    """)

# --- ENHANCED EMPTY STATE ---
if df.empty:
    st.markdown("""
        <div class="empty-state">
            <h2>🎬 Strategy Engine Standby</h2>
            <p>Select at least one genre from the sidebar to activate market analysis.</p>
            <p style="color: #888; font-size: 0.85rem; margin-top: 15px;">
                Available genres include: Drama, Comedy, Action, Thriller, Horror, Sci-Fi, and more.
            </p>
        </div>
    """, unsafe_allow_html=True)
    st.stop()
    
# --- DYNAMIC METRIC CALCULATIONS ---
if not df.empty:
    raw_sentiment = df['Sentiment_Score'].mean()
    sentiment_pct = (raw_sentiment + 1) / 2 
    
    # Get the 90th percentile of your filtered genre
    genre_90th = df['Popularity_Score'].quantile(0.90)
    
    # Find where that score sits as a percentile of the ENTIRE dataset
    market_percentile = (df_full['Popularity_Score'] < genre_90th).mean()
    
    # Convert to 0-100 scale
    avg_market = int(market_percentile * 100)
    avg_market = max(2, min(100, avg_market))
    
    # Genre Market Opportunity Logic
    saturation_ratio = len(df) / len(df_full)
    opportunity_pct = max(0.05, min(0.95, 1.0 - saturation_ratio))
    
    if opportunity_pct > 0.80: 
        opp_label = "High"
        opp_color = "#28a745"
    elif opportunity_pct > 0.50: 
        opp_label = "Moderate" 
        opp_color = "#ffc107"
    else:                      
        opp_label = "Low"
        opp_color = "#dc3545"
else:
    sentiment_pct, avg_market, opportunity_pct = 0, 0, 0
    opp_label, opp_color = "N/A", "#888"

# --- DYNAMIC COLOR MAPPING ---
if sentiment_pct > 0.6: 
    sent_color, sent_label = "#28a745", "High"
elif sentiment_pct > 0.4: 
    sent_color, sent_label = "#ffc107", "Moderate"
else: 
    sent_color, sent_label = "#888", "Neutral"

if avg_market > 80: 
    app_color = "#28a745"
elif avg_market > 50: 
    app_color = "#ffc107"
else: 
    app_color = "#dc3545"

# --- METRIC CARDS WITH ARIA LABELS ---
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"""
        <div class="metric-card" role="region" aria-label="Sentiment ROI Metric">
            <div class="metric-caption">Avg Sentiment ROI</div>
            <div style="color: {sent_color}; font-size: 1.5rem; font-weight: bold; margin: 5px 0;">{sent_label}</div>
            <div style="color: #ffd600; font-size: 0.8rem;">{len(df)} Projects</div>
        </div>
    """, unsafe_allow_html=True)
    st.progress(max(0.0, min(1.0, float(sentiment_pct))))

with col2:
    st.markdown(f"""
        <div class="metric-card" role="region" aria-label="Market Appetite Metric">
            <div class="metric-caption">Market Appetite</div>
            <div style="color: {app_color}; font-size: 1.5rem; font-weight: bold; margin: 5px 0;">{avg_market}%</div>
            <div style="color: #ffd600; font-size: 0.8rem;">Global Target</div>
        </div>
    """, unsafe_allow_html=True)
    st.progress(max(0.0, min(1.0, avg_market / 100.0)))

with col3:
    st.markdown(f"""
        <div class="metric-card" role="region" aria-label="Market Opportunity Metric">
            <div class="metric-caption">Genre Market Opportunity</div>
            <div style="color: {opp_color}; font-size: 1.5rem; font-weight: bold; margin: 5px 0;">{opp_label}</div>
            <div style="color: #ffd600; font-size: 0.8rem;">Blue Ocean Potential</div>
        </div>
    """, unsafe_allow_html=True)
    st.progress(max(0.0, min(1.0, float(opportunity_pct))))

# --- 7. VISUALIZATIONS ---
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
    # Create a clean copy and ensure no duplicate columns for Plotly
    display_df = df.copy()
    display_df = display_df.loc[:, ~display_df.columns.duplicated(keep='first')]
    
    # Truncate long movie titles for cleaner hover tooltips
    display_df['Display_Title'] = display_df['Project'].apply(truncate_title)
    
    active_genres_str = ", ".join(genre_filter) if genre_filter else "All Genres"

    y_max = display_df['Popularity_Score'].max()
    y_upper_limit = y_max * 1.15 if y_max > 0 else 100

    # Build hover_data safely - only include Lead_Talent if it exists
    hover_data_config = {
        "Sentiment_Score": ":.2f",
        "Popularity_Score": ":.1f",
        "Genre": True,
    }
    if "Lead_Talent" in display_df.columns:
        hover_data_config["Lead_Talent"] = True

    # Custom color palette with 30+ unique colors for genres
    GENRE_COLORS = {
        'Comedy': '#FFD700',        # Gold
        'Drama': '#4169E1',         # Royal Blue
        'Action': '#FF4500',        # Orange Red
        'Horror': '#8B0000',        # Dark Red
        'Thriller': '#483D8B',      # Dark Slate Blue
        'Sci-Fi': '#00CED1',        # Dark Turquoise
        'Fantasy': '#9932CC',       # Dark Orchid
        'Romance': '#FF69B4',       # Hot Pink
        'Animation': '#32CD32',     # Lime Green
        'Documentary': '#808080',   # Gray
        'Family': '#FFA07A',        # Light Salmon
        'TV Series': '#6495ED',     # Cornflower Blue
        'Reality': '#00FA9A',       # Medium Spring Green
        'International': '#DB7093', # Pale Violet Red
        'Musical': '#FF1493',       # Deep Pink
        'War': '#556B2F',           # Dark Olive Green
        'Western': '#D2691E',       # Chocolate
        'Crime': '#2F4F4F',         # Dark Slate Gray
        'Mystery': '#4B0082',       # Indigo
        'Supernatural': '#663399',  # Rebecca Purple
        'Indie': '#20B2AA',         # Light Sea Green
        'Classic': '#DAA520',       # Goldenrod
        'Sports': '#228B22',        # Forest Green
        'Short': '#BC8F8F',         # Rosy Brown
        'Adult': '#A52A2A',         # Brown
        'Unknown': '#696969',       # Dim Gray
        'Other': '#778899',         # Light Slate Gray
    }
    
    # Get unique genres in the current data
    unique_genres = display_df['Genre'].unique().tolist()
    
    # Build color sequence for current genres
    color_sequence = []
    for genre in unique_genres:
        if genre in GENRE_COLORS:
            color_sequence.append(GENRE_COLORS[genre])
        else:
            # Generate a color for any new/unmapped genre
            import hashlib
            hash_val = int(hashlib.md5(genre.encode()).hexdigest()[:6], 16)
            r = (hash_val >> 16) & 255
            g = (hash_val >> 8) & 255
            b = hash_val & 255
            color_sequence.append(f'rgb({r},{g},{b})')

    fig_scatter = px.scatter(
        display_df, 
        x="Sentiment_Score", 
        y="Popularity_Score", 
        size="Popularity_Score", 
        color="Genre", 
        hover_name="Display_Title",  # Uses truncated title
        hover_data=hover_data_config,
        range_x=[-0.01, 1.08], 
        range_y=[0, y_upper_limit],
        template="plotly_dark",
        size_max=35, 
        height=500,
        color_discrete_map=GENRE_COLORS
    )
    
    fig_scatter.update_traces(marker=dict(opacity=0.7, line=dict(width=1, color='White')))
    
    # Horizontal legend with breathing room
    fig_scatter.update_layout(
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.15,
            xanchor="center",
            x=0.5,
            font=dict(size=10),
            bgcolor="rgba(0,0,0,0)",
            borderwidth=0
        ),
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, t=10, b=80),
        xaxis_title="Sentiment ROI",
        yaxis_title="Market Appetite Index"
    )
    
    st.markdown(f"""
        <p style="color:#888; font-size:0.8rem; margin-bottom:10px; padding-left:2px;">
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
        height=450,
        title="Active Market Saturation (Selected Genres)",
        color_discrete_map=GENRE_COLORS
    )
    fig_bar.update_layout(
        margin=dict(l=0, r=0, t=50, b=0),
        showlegend=False, 
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)',
        title=dict(
            y=0.95,
            x=0.5,
            xanchor='center',
            yanchor='top',
            font=dict(size=14)
        )
    )
    st.plotly_chart(fig_bar, use_container_width=True, key="genre_distribution_bar")

with st.expander("📂 View Full Intelligence Ledger"):
    # FIX: Ensure no duplicate columns in the displayed dataframe
    ledger_df = df.loc[:, ~df.columns.duplicated(keep='first')]
    st.dataframe(ledger_df, use_container_width=True)

st.caption(f"Genre Sync Analytics v{__version__} | Strategic Intelligence by Ida Akiwumi.")