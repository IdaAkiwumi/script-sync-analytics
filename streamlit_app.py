"""
PROJECT: Genre Sync Analytics
VERSION: 1.1.1
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
__version__ = "1.1.1"
__license__ = "Proprietary"
__status__ = "Production / Portfolio"

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import plotly.express as px
import os
import numpy as np
import json

# --- UI SETUP: MUST COME BEFORE OTHER STREAMLIT COMMANDS ---
st.set_page_config(
    page_title="Genre Sync Analytics | Designed by Ida Akiwumi",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 1. INITIALIZE STATE ---
def init_state():
    if "data_loaded" not in st.session_state:
        st.session_state.data_loaded = False
    if "analysis_results" not in st.session_state:
        st.session_state.analysis_results = None
    if "saved_filters" not in st.session_state:
        st.session_state.saved_filters = {}
    if "first_visit" not in st.session_state:
        st.session_state.first_visit = True
    if "user_has_interacted" not in st.session_state:
        st.session_state.user_has_interacted = False
    if "default_genres" not in st.session_state:
        st.session_state.default_genres = None
    if "selected_project" not in st.session_state:
        st.session_state.selected_project = None
    if "just_selected" not in st.session_state:
        st.session_state.just_selected = False
    if "confirm_clear" not in st.session_state:
        st.session_state.confirm_clear = False
    if "ignore_next_plot_selection" not in st.session_state:
        st.session_state.ignore_next_plot_selection = False
    if "allow_scroll_js" not in st.session_state:
        st.session_state.allow_scroll_js = False

init_state()

# --- 2. UI SETUP & BRANDING ---
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

    .selected-project-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #262730 100%);
        border: 2px solid #ffd600;
        border-radius: 12px;
        padding: 20px;
        margin: 15px 0;
    }

    .selected-project-card h3 {
        color: #ffd600;
        margin: 0 0 15px 0;
        font-size: 1.2rem;
    }

    .selected-project-card .project-title {
        color: #ffffff;
        font-size: 1.4rem;
        font-weight: bold;
        margin-bottom: 10px;
    }

    .selected-project-card .project-meta {
        color: #A3AABF;
        font-size: 0.95rem;
        line-height: 1.8;
    }

    .selected-project-card .project-meta strong {
        color: #ffd600;
    }

    @media print {
        [data-testid="stSidebar"], [data-testid="stHeader"] { display: none !important; }
        .metric-caption, .stMetricValue { color: #000000 !important; }
        .metric-card { background-color: #ffffff !important; border: 1px solid #000 !important; }
        .main .block-container { max-width: 100% !important; padding: 0 !important; }
    }

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
        'Lead_Talent': ['John Doe', 'Jane Smith', 'Ensemble', 'Chris Evans', 'Ensemble'],
        'Content_Type': ['Film', 'Film', 'Film', 'Film', 'Film'],
        'Is_Adult': [False, False, False, False, False]
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
            m_df = m_df.loc[:, ~m_df.columns.duplicated(keep='first')]

            rename_map = {
                'title': 'Project',
                'vote_average': 'Sentiment_Score',
                'popularity': 'Popularity_Score',
                'genres': 'Genre'
            }
            for old_name, new_name in rename_map.items():
                if old_name in m_df.columns and new_name not in m_df.columns:
                    m_df = m_df.rename(columns={old_name: new_name})

            if 'Lead_Talent' not in m_df.columns and os.path.exists(cast_path):
                c_df = pd.read_csv(cast_path, engine='python', on_bad_lines='skip')
                c_df = c_df.loc[:, ~c_df.columns.duplicated(keep='first')]
                if 'id' in m_df.columns and 'id' in c_df.columns and 'name' in c_df.columns:
                    m_df = pd.merge(m_df, c_df[['id', 'name']], on='id', how='left')
                    if 'name' in m_df.columns:
                        m_df = m_df.rename(columns={'name': 'Lead_Talent'})

            if 'Sentiment_Score' in m_df.columns:
                m_df['Sentiment_Score'] = pd.to_numeric(m_df['Sentiment_Score'], errors='coerce').fillna(0.0)
                if m_df['Sentiment_Score'].max() > 1.1:
                    m_df['Sentiment_Score'] = (m_df['Sentiment_Score'] - 5) / 5

            if 'Popularity_Score' in m_df.columns:
                m_df['Popularity_Score'] = pd.to_numeric(m_df['Popularity_Score'], errors='coerce').fillna(50)

            m_df = m_df.loc[:, ~m_df.columns.duplicated(keep='first')]

            if 'Lead_Talent' not in m_df.columns:
                m_df['Lead_Talent'] = 'Ensemble'

            if 'Content_Type' not in m_df.columns:
                m_df['Content_Type'] = 'Film'

            if 'Is_Adult' not in m_df.columns:
                m_df['Is_Adult'] = m_df['Genre'].astype(str).eq('Adult')

            required_cols = {
                'Project': 'Unknown',
                'Genre': 'Unknown',
                'Sentiment_Score': 0.0,
                'Popularity_Score': 50,
                'Lead_Talent': 'Ensemble',
                'Content_Type': 'Film',
                'Is_Adult': False
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

def scroll_js_enabled():
    return st.session_state.allow_scroll_js and st.session_state.user_has_interacted

def scroll_to_element(element_id):
    """Inject JavaScript to scroll to a specific element only after real user interaction."""
    if not scroll_js_enabled():
        return

    components.html(
        f"""
        <script>
            setTimeout(function() {{
                const element = window.parent.document.getElementById('{element_id}');
                if (element) {{
                    element.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
                }}
            }}, 100);
        </script>
        """,
        height=0
    )

def calculate_genre_opportunity(df_full, genre_filter):
    """
    Calculate Genre Market Opportunity using percentile-based saturation.
    """
    genre_counts = df_full['Genre'].value_counts()
    selected_counts = [genre_counts.get(g, 0) for g in genre_filter if g in genre_counts.index]

    if not selected_counts:
        return 0.5, "Moderate", "#ffc107"

    avg_selected_count = np.mean(selected_counts)
    saturation_percentile = (genre_counts <= avg_selected_count).mean()

    opportunity_pct = 1.0 - saturation_percentile
    opportunity_pct = max(0.05, min(0.95, opportunity_pct))

    if opportunity_pct >= 0.85:
        return opportunity_pct, "Very High", "#1a7f37"
    elif opportunity_pct >= 0.70:
        return opportunity_pct, "High", "#28a745"
    elif opportunity_pct >= 0.55:
        return opportunity_pct, "Moderate-High", "#5cb85c"
    elif opportunity_pct >= 0.45:
        return opportunity_pct, "Moderate", "#ffc107"
    elif opportunity_pct >= 0.30:
        return opportunity_pct, "Moderate-Low", "#fd7e14"
    elif opportunity_pct >= 0.15:
        return opportunity_pct, "Low", "#dc3545"
    else:
        return opportunity_pct, "Very Low", "#8b0000"

def get_sentiment_label_and_color(score):
    """Map -1 to 1 sentiment score to label/color."""
    if score >= 0.4:
        return "High", "#28a745"
    elif score >= 0.1:
        return "Moderate", "#ffc107"
    elif score > -0.1:
        return "Neutral", "#888"
    else:
        return "Low", "#dc3545"

# --- 4. SIDEBAR STUDIO CONTROLS ---
with st.spinner('🎬 Loading Market Intelligence...'):
    real_df = load_real_data()
    df_full = real_df if real_df is not None else load_placeholder_data()

df_full = df_full.loc[:, ~df_full.columns.duplicated(keep='first')]

if "active_scenario_name" not in st.session_state:
    st.session_state.active_scenario_name = "Default"

if "import_expanded" not in st.session_state:
    st.session_state.import_expanded = False

with st.sidebar:
    st.markdown(f"**Current View:** `{st.session_state.active_scenario_name}`")

    all_genres = sorted([str(g) for g in df_full['Genre'].unique() if pd.notna(g) and str(g).strip() != ""])

    if st.session_state.default_genres is None:
        st.session_state.default_genres = [
            g for g in ["Drama", "Comedy", "Action", "Thriller", "Horror"] if g in all_genres
        ]

    def handle_import():
        file = st.session_state.scenario_uploader
        if file is not None:
            try:
                imported_data = json.load(file)
                if isinstance(imported_data, dict):
                    st.session_state.saved_filters.update(imported_data)
                    st.toast(f"Imported {len(imported_data)} scenarios!", icon="📂")
                    st.session_state.import_expanded = False
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

    if st.session_state.saved_filters:
        def load_scenario_callback():
            sel = st.session_state["scenario_loader"]
            if sel != "-- Select --":
                st.session_state["genre_selector"] = st.session_state.saved_filters[sel]
                st.session_state.active_scenario_name = sel
                st.toast(f"View Switched: {sel}")
                st.session_state.user_has_interacted = True
                st.session_state.first_visit = False

        st.selectbox(
            "📂 Load Saved Scenario",
            ["-- Select --"] + list(st.session_state.saved_filters.keys()),
            key="scenario_loader",
            on_change=load_scenario_callback
        )

    def on_genre_change():
        st.session_state.active_scenario_name = "Custom (Modified)"
        current_selection = st.session_state.get("genre_selector", [])
        if set(current_selection) != set(st.session_state.default_genres):
            st.session_state.user_has_interacted = True
            st.session_state.first_visit = False

    if "genre_selector" not in st.session_state:
        st.session_state["genre_selector"] = st.session_state.default_genres.copy()

    genre_filter = st.multiselect(
        "Filter by Primary Genre",
        all_genres,
        key="genre_selector",
        on_change=on_genre_change
    )

    st.subheader("💾 Scenario Management")

    scenario_name = st.text_input("Scenario Name", placeholder="e.g., Q1 Sci-Fi Push")

    save_disabled = not scenario_name or not genre_filter

    if st.button("💾 Save Scenario", use_container_width=True, disabled=save_disabled):
        st.session_state.saved_filters[scenario_name] = genre_filter
        st.session_state.active_scenario_name = scenario_name
        st.session_state.user_has_interacted = True
        st.session_state.first_visit = False
        st.success(f"Saved '{scenario_name}'")
        st.rerun()

    if st.session_state.saved_filters:
        st.download_button(
            label="📥 Export Scenarios (.json)",
            file_name="genre_sync_scenarios.json",
            mime="application/json",
            data=json.dumps(st.session_state.saved_filters),
            use_container_width=True
        )

        if not st.session_state.confirm_clear:
            if st.button("🗑️ Clear All Scenarios", use_container_width=True, type="secondary"):
                st.session_state.confirm_clear = True
                st.rerun()
        else:
            st.warning("⚠️ This will delete all saved scenarios.")
            col_yes, col_no = st.columns(2)
            with col_yes:
                if st.button("Yes, Clear All", type="primary", use_container_width=True):
                    st.session_state.saved_filters = {}
                    st.session_state.active_scenario_name = "Default"
                    st.session_state.confirm_clear = False
                    st.rerun()
            with col_no:
                if st.button("Cancel", use_container_width=True):
                    st.session_state.confirm_clear = False
                    st.rerun()
    else:
        st.info("💡 Save your first scenario to unlock Export features")

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

    with st.expander("📚 Data Sources & Intelligence"):
        st.caption("This engine aggregates market metadata from Kaggle datasets across film, TV, streaming, and online video:")

        st.markdown("""
    **Smallest / Highly Curated**
    - [TMDB Top 10,000 Popular Movies Dataset](https://www.kaggle.com/datasets/sankha1998/tmdb-top-10000-popular-movies-dataset)
    - [IMDb Top 5000 Movies](https://www.kaggle.com/datasets/tiagoadrianunes/imdb-top-5000-movies)
    - [Netflix Shows](https://www.kaggle.com/datasets/shivamb/netflix-shows)
    - [Movies](https://www.kaggle.com/datasets/abdallahwagih/movies)
    - [TMDB Movie Metadata](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata)

    **Mid-Size / Comprehensive**
    - [The Movie Database (TMDB) Comprehensive Dataset](https://www.kaggle.com/datasets/rishabhkumar2003/the-movie-database-tmdb-comprehensive-dataset)
    - [TMDB IMDb Merged Movies Dataset](https://www.kaggle.com/datasets/ggtejas/tmdb-imdb-merged-movies-dataset)
    - [Movies](https://www.kaggle.com/datasets/mohammedalsubaie/movies)
    - [IMDb Dataset](https://www.kaggle.com/datasets/ahmedosamamath/imdb-dataset)

    **Large Scale / Global**
    - [TMDB Movies Dataset 2023 (930k Movies)](https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies)
    - [TMDB Movies Daily Updates](https://www.kaggle.com/datasets/alanvourch/tmdb-movies-daily-updates)
    - [Trending YouTube Videos – 113 Countries](https://www.kaggle.com/datasets/asaniczka/trending-youtube-videos-113-countries)

    **Data Snapshot: SPRING 2026**
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
df = df.loc[:, ~df.columns.duplicated(keep='first')]

# --- 6. MAIN INTERFACE ---
st.markdown(f'''
    <div class="compact-header">
        <span>🎬 GENRE SYNC: {st.session_state.active_scenario_name.upper()}</span>
        <span>STATUS: <span style="background:#ffd600; color:#000; padding:0 5px; border-radius:3px;">SPRING 2026 MARKET DATA</span></span>
    </div>
''', unsafe_allow_html=True)

if st.session_state.first_visit and not st.session_state.user_has_interacted:
    st.info("🎬 **Welcome to Genre Sync Analytics** — Identify Blue Ocean opportunities by analyzing genre sentiment, market appetite, and saturation. Use the sidebar to filter genres and save scenarios. Need help? Expand the **Strategy Guide** below.")

with st.expander("ℹ️ STRATEGY GUIDE: How to use Genre Sync"):
    st.markdown("""
    ### **Objective**
    Identify **Blue Ocean** opportunities by finding genres with **High ROI** and **High Market Opportunity**.

    ### **User Journey**
    1. **Filter by Primary Genre:** Use the sidebar to select your target market (e.g., *Thriller* or *Sci-Fi*).
    2. **Evaluate Market Appetite:** Check the "Heat Index"—is the global audience currently 'hungry' for this content?
    3. **Assess Market Opportunity:** A high bar here indicates a **Blue Ocean** gap where your narrative can stand out without fighting "Red Ocean" saturation.
    4. **Analyze Comps:** Hover over the bubbles in the **Narrative Performance** tab to see the specific projects and talent currently defining your selected market's ROI.
    5. **Click any bubble** to view detailed project information below the chart.

    ### **Key Metrics Explained**
    | Metric | Meaning |
    |--------|---------|
    | Sentiment ROI | Audience approval level based on ratings |
    | Market Appetite | How popular/trending this genre is globally |
    | Market Opportunity | Blue Ocean (underserved) vs Red Ocean (saturated) |

    ### **Opportunity Levels**
    | Level | Color | Meaning |
    |-------|-------|---------|
    | Very High | 🟢 Dark Green | Extremely underserved — prime Blue Ocean |
    | High | 🟢 Green | Strong opportunity, low competition |
    | Moderate-High | 🟢 Light Green | Good opportunity with some competition |
    | Moderate | 🟡 Yellow | Balanced market |
    | Moderate-Low | 🟠 Orange | Competitive market |
    | Low | 🔴 Red | Saturated — Red Ocean territory |
    | Very Low | 🔴 Dark Red | Extremely crowded market |
    """)

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

    genre_90th = df['Popularity_Score'].quantile(0.90)
    market_percentile = (df_full['Popularity_Score'] < genre_90th).mean()
    avg_market = int(market_percentile * 100)
    avg_market = max(2, min(100, avg_market))

    opportunity_pct, opp_label, opp_color = calculate_genre_opportunity(df_full, genre_filter)
else:
    raw_sentiment = 0.0
    sentiment_pct, avg_market, opportunity_pct = 0, 0, 0
    opp_label, opp_color = "N/A", "#888"

sent_label, sent_color = get_sentiment_label_and_color(raw_sentiment)

if avg_market > 80:
    app_color = "#28a745"
elif avg_market > 50:
    app_color = "#ffc107"
else:
    app_color = "#dc3545"

# --- METRIC CARDS ---
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
            <div style="color: #ffd600; font-size: 0.8rem;">Blue Ocean Index</div>
        </div>
    """, unsafe_allow_html=True)
    st.progress(max(0.0, min(1.0, float(opportunity_pct))))

# --- 7. VISUALIZATIONS ---
st.markdown("""
    <style>
    [data-testid="stTabPanel"] { padding-top: 0rem !important; }
    .stPlotlyChart { margin-top: -10px !important; }
    [data-testid="stToastContainer"] {
        left: 40px !important;
        right: auto !important;
        bottom: 0px !important;
    }
    </style>
""", unsafe_allow_html=True)

tab1, tab2 = st.tabs(["🎯 Narrative Performance", "📊 Genre Distribution"])

GENRE_COLORS = {
    'Action': '#FF4500',
    'Adventure': '#FF8C00',
    'Animation': '#32CD32',
    'Biography': '#8A2BE2',
    'Comedy': '#FFD700',
    'Crime': '#2F4F4F',
    'Documentary': '#808080',
    'Drama': '#4169E1',
    'Family': '#FFA07A',
    'Fantasy': '#9932CC',
    'History': '#A0522D',
    'Horror': '#8B0000',
    'Musical': '#FF1493',
    'Mystery': '#4B0082',
    'Reality': '#00FA9A',
    'Romance': '#FF69B4',
    'Sci-Fi': '#00CED1',
    'Short': '#BC8F8F',
    'Sports': '#228B22',
    'Supernatural': '#663399',
    'Thriller': '#483D8B',
    'TV Series': '#6495ED',
    'War': '#556B2F',
    'Adult': '#A52A2A',
    'Unknown': '#696969',
    'Other': '#778899',
}

with tab1:
    display_df = df.copy()
    display_df = display_df.loc[:, ~display_df.columns.duplicated(keep='first')]
    display_df['Display_Title'] = display_df['Project'].apply(truncate_title)
    display_df = display_df.reset_index(drop=True)

    active_genres_str = ", ".join(genre_filter) if genre_filter else "All Genres"

    y_max = display_df['Popularity_Score'].max()
    y_upper_limit = y_max * 1.15 if y_max > 0 else 100

    hover_data_config = {
        "Sentiment_Score": ":.2f",
        "Popularity_Score": ":.1f",
        "Genre": True,
        "Content_Type": True,
        "Is_Adult": True,
    }
    if "Lead_Talent" in display_df.columns:
        hover_data_config["Lead_Talent"] = True

    fig_scatter = px.scatter(
        display_df,
        x="Sentiment_Score",
        y="Popularity_Score",
        size="Popularity_Score",
        color="Genre",
        hover_name="Display_Title",
        hover_data=hover_data_config,
        range_x=[-1.05, 1.05],
        range_y=[0, y_upper_limit],
        template="plotly_dark",
        size_max=35,
        height=500,
        color_discrete_map=GENRE_COLORS
    )

    fig_scatter.update_traces(marker=dict(opacity=0.7, line=dict(width=1, color='White')))

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
        margin=dict(l=0, r=0, t=26, b=80),
        xaxis_title="Sentiment ROI (-1 to 1)",
        yaxis_title="Market Appetite Index"
    )

    st.markdown(f"""
        <p style="color:#888; font-size:0.8rem; margin-bottom:5px; padding-left:2px;">
            Showing <strong>{len(display_df)}</strong> market competitors in:
            <span style="color:#dc3545;">{active_genres_str}</span>
            <br><span style="font-style: italic;">💡 Click any bubble to view project details</span>
        </p>
    """, unsafe_allow_html=True)

    selection = st.plotly_chart(
        fig_scatter,
        use_container_width=True,
        key="performance_scatter",
        on_select="rerun",
        selection_mode=["points"]
    )

    if selection and selection.selection and len(selection.selection.points) > 0:
        point_data = selection.selection.points[0]
        point_idx = point_data.get("point_index", None)

        if point_idx is not None and point_idx < len(display_df):
            new_selection = display_df.iloc[point_idx]['Project']
            if new_selection != st.session_state.selected_project:
                st.session_state.selected_project = new_selection
                st.session_state.just_selected = True
                st.session_state.user_has_interacted = True
                st.session_state.first_visit = False
                st.session_state.allow_scroll_js = True
            else:
                st.session_state.just_selected = False

    if st.session_state.selected_project:
        selected_row = df[df['Project'] == st.session_state.selected_project]

        if not selected_row.empty:
            row = selected_row.iloc[0]

            proj_sentiment = row['Sentiment_Score']
            proj_sent_label, proj_sent_color = get_sentiment_label_and_color(proj_sentiment)

            if st.session_state.just_selected:
                st.toast(f"⬇️ Project details loaded below", icon="🎬")
                st.session_state.just_selected = False

            st.markdown('<div id="selected-project-details"></div>', unsafe_allow_html=True)

            header_col1, header_col2 = st.columns([4, 1])
            with header_col1:
                st.markdown("### 🎯 Selected Project Details")
            with header_col2:
                if st.button("✕ Clear", key="clear_selection", type="secondary"):
                    st.session_state.selected_project = None
                    st.session_state.just_selected = False
                    st.session_state.allow_scroll_js = False
                    st.rerun()

            st.markdown(f"""
                <div class="selected-project-card">
                    <div class="project-title">🎬 {row['Project']}</div>
                    <div class="project-meta">
                        <strong>Genre:</strong> {row['Genre']}<br>
                        <strong>Content Type:</strong> {row.get('Content_Type', 'N/A')}<br>
                        <strong>Explicit Adult Flag:</strong> {row.get('Is_Adult', False)}<br>
                        <strong>Lead Talent:</strong> {row.get('Lead_Talent', 'N/A')}<br>
                        <strong>Sentiment ROI:</strong> <span style="color: {proj_sent_color};">{proj_sent_label}</span> ({proj_sentiment:.2f})<br>
                        <strong>Market Appetite Score:</strong> {row['Popularity_Score']:.1f}
                    </div>
                </div>
            """, unsafe_allow_html=True)

            scroll_to_element("selected-project-details")
        else:
            st.session_state.selected_project = None
            st.session_state.just_selected = False
            st.session_state.allow_scroll_js = False

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

with st.expander("📂 View Full Intelligence Ledger", expanded=st.session_state.selected_project is not None):
    ledger_df = df.loc[:, ~df.columns.duplicated(keep='first')]

    if st.session_state.selected_project:
        st.markdown(f"**Filtered to:** {st.session_state.selected_project}")
        filtered_ledger = ledger_df[ledger_df['Project'] == st.session_state.selected_project]
        if not filtered_ledger.empty:
            st.dataframe(filtered_ledger, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("**Full Ledger:**")

    st.dataframe(ledger_df, use_container_width=True, hide_index=True)

st.caption(f"Genre Sync Analytics v{__version__} | Strategic Intelligence by Ida Akiwumi.")