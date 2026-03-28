"""
GENRE SYNC ANALYTICS - ABOUT PAGE
Strategic ROI Intelligence for Film & Gaming Industries

Keywords: film analytics, movie market analysis, genre strategy, 
entertainment data visualization, ROI dashboard, blue ocean strategy,
film production analytics, movie sentiment analysis, genre saturation

Author: Ida Akiwumi
"""

import streamlit as st

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="About Genre Sync Analytics | Film ROI Dashboard by Ida Akiwumi",
    page_icon="🎬",
    layout="wide"
)

# --- CUSTOM CSS (consistent with main app) ---
st.markdown("""
    <style>
    [data-testid="stHeader"] { background-color: rgba(0,0,0,0) !important; }
    
    .about-section {
        background-color: #262730;
        padding: 25px;
        border-radius: 8px;
        margin-bottom: 20px;
        border-left: 4px solid #ffd600;
    }
    
    .about-section h2 {
        color: #ffd600;
        margin-top: 0;
    }
    
    .keyword-tag {
        display: inline-block;
        background: #1e1e1e;
        color: #A3AABF;
        padding: 4px 12px;
        border-radius: 15px;
        margin: 3px;
        font-size: 0.85rem;
        border: 1px solid #333;
    }
    
    .cta-box {
        background: linear-gradient(135deg, #262730 0%, #1a1a2e 100%);
        border: 2px solid #ffd600;
        border-radius: 8px;
        padding: 30px;
        text-align: center;
        margin: 30px 0;
    }
    
    .cta-box h2 {
        color: #ffd600;
        margin-bottom: 15px;
    }
    </style>
""", unsafe_allow_html=True)

# --- SIDEBAR CONTENT (so sidebar always shows something) ---
with st.sidebar:
    # st.title("🎬 Navigation")
    st.info("Use the page selector above to switch between the Dashboard and About page.")
    st.markdown("---")
    st.markdown("**Quick Links:**")
    st.markdown("[LinkedIn → Ida Akiwumi](https://www.linkedin.com/in/idaa11)")

# --- HEADER ---
st.title("🎬 About Genre Sync Analytics")
st.markdown("### Strategic ROI Intelligence for the Entertainment Industry")

# --- MAIN DESCRIPTION (SEO-rich content) ---
st.markdown("""
<div class="about-section">
<h2>What Is Genre Sync Analytics?</h2>

**Genre Sync Analytics** is a strategic decision-support dashboard designed for 
**film producers**, **studio executives**, **greenlight committees**, and 
**entertainment data analysts**. 

This tool transforms raw movie metadata into actionable market intelligence, 
helping creative teams identify **Blue Ocean opportunities**—genres and narrative 
spaces with high audience demand but low market saturation.

Unlike traditional box office trackers, Genre Sync focuses on **predictive 
sentiment analysis** and **market appetite modeling** to answer the question 
every producer asks: *"Is this the right story for right now?"*

</div>
""", unsafe_allow_html=True)

# --- KEY FEATURES ---
st.markdown("""
<div class="about-section">
<h2>Key Features</h2>
</div>
""", unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **📊 Market Sentiment Analysis**
    - Aggregate audience sentiment by genre
    - Track emotional ROI across market segments
    - Identify underserved audience needs
    
    **🎯 Blue Ocean Opportunity Mapping**
    - Visualize genre saturation levels
    - Spot gaps in the competitive landscape
    - Data-driven greenlight support
    """)

with col2:
    st.markdown("""
    **💾 Scenario Planning**
    - Save and compare filter configurations
    - Export strategies as JSON for team sharing
    - Track market shifts over time
    
    **📈 Interactive Visualizations**
    - Sentiment vs. Popularity scatter analysis
    - Genre distribution breakdowns
    - Exportable charts for presentations
    """)

# --- USE CASES (keyword-rich) ---
st.markdown("""
<div class="about-section">
<h2>Who Uses This Tool?</h2>

| Role | Use Case |
|------|----------|
| **Studio Executives** | Evaluate genre portfolio balance and identify acquisition targets |
| **Greenlight Committees** | Data-backed evidence for project approval decisions |
| **Independent Producers** | Find underserved niches to maximize limited budgets |
| **Creative Strategists** | Align narrative development with market demand |
| **Entertainment Analysts** | Research market trends and competitive positioning |
| **Film Students & Researchers** | Study genre economics and audience behavior |

</div>
""", unsafe_allow_html=True)

# --- METHODOLOGY ---
st.markdown("""
<div class="about-section">
<h2>Data & Methodology</h2>

Genre Sync Analytics aggregates metadata from established entertainment databases 
including **TMDB (The Movie Database)** to provide market-representative insights.

**Key Metrics Explained:**

- **Sentiment ROI**: Normalized audience approval scores (-1 to +1 scale), 
  indicating how well content in a genre resonates with viewers
  
- **Market Appetite**: Popularity percentile ranking against the full database, 
  showing current audience demand levels
  
- **Genre Market Opportunity**: Inverse saturation metric—higher scores indicate 
  less competition (Blue Ocean), lower scores indicate crowded markets (Red Ocean)

*Data refreshed quarterly. Current dataset: Spring 2026 market snapshot.*

</div>
""", unsafe_allow_html=True)

# --- KEYWORDS/TAGS (helps crawlers understand content) ---
st.markdown("### 🏷️ Related Topics")
keywords = [
    "Film Analytics", "Movie ROI", "Entertainment Data", "Genre Strategy",
    "Blue Ocean Strategy", "Market Analysis", "Sentiment Analysis",
    "Film Production", "Studio Analytics", "Greenlight Decision",
    "Box Office Prediction", "Audience Insights", "Content Strategy",
    "Data Visualization", "Streamlit Dashboard", "Python Analytics"
]

keyword_html = " ".join([f'<span class="keyword-tag">{kw}</span>' for kw in keywords])
st.markdown(f'<div style="margin: 15px 0;">{keyword_html}</div>', unsafe_allow_html=True)

# --- ABOUT THE CREATOR ---
st.markdown("""
<div class="about-section">
<h2>About the Creator</h2>

**Ida Akiwumi** is a **Creative Technologist** and **Narrative Strategist** specializing 
in the intersection of technology and storytelling. With expertise in data 
visualization, user experience design, and entertainment industry workflows, 
Ida builds tools that help creative professionals make informed decisions.

**Technical Stack:** Python, Streamlit, Pandas, Plotly, TextBlob

**Specializations:**
- Product Design & Architecture
- Data Storytelling & Visualization  
- Entertainment Industry Analytics
- Strategic Decision Support Systems

</div>
""", unsafe_allow_html=True)

# --- CALL TO ACTION ---
st.markdown("""
<div class="cta-box">
<h2>🚀 Ready to Explore?</h2>
<p style="color: #A3AABF; font-size: 1.1rem;">
Select <strong>streamlit_app</strong> from the sidebar to launch the main dashboard.
</p>
</div>
""", unsafe_allow_html=True)

# --- CONTACT & LINKS ---
st.markdown("---")
st.markdown("### 📬 Connect")

link_col1, link_col2, link_col3 = st.columns(3)

with link_col1:
    st.markdown("**LinkedIn**")
    st.markdown("[→ Ida Akiwumi](https://www.linkedin.com/in/idaa11)")

with link_col2:
     st.markdown("**Website**")
     st.markdown("[→ Ida Akiwumi](https://idaakiwumi.com)")

with link_col3:
    st.markdown("**GitHub**")
    st.markdown("[→ Source Code](https://github.com/idaakiwumi/genre-sync-analytics)")

# --- FOOTER ---
st.markdown("---")
st.caption("Genre Sync Analytics v1.0.2 | © 2024 Ida Akiwumi | Built with Streamlit")

# --- HIDDEN SEO TEXT (visible to crawlers) ---
st.markdown("""
<div style="position: absolute; left: -9999px; opacity: 0;" aria-hidden="true">
Genre Sync Analytics is a film industry analytics dashboard for movie market 
analysis, entertainment ROI tracking, and genre strategy planning. Built by 
Ida Akiwumi, Creative Technologist specializing in data visualization for creative 
industries. Features include sentiment analysis, market appetite scoring, 
blue ocean opportunity identification, and scenario planning for film production 
decisions. Ideal for studio executives, independent producers, greenlight 
committees, and entertainment data analysts seeking data-driven insights into 
genre performance and audience demand.
</div>
""", unsafe_allow_html=True)