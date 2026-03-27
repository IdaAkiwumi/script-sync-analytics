# 🎬 Genre Sync Analytics
**Bridging the Gap Between Creative Intuition and Market Data.**

[![GitHub Sponsor](https://img.shields.io/badge/Sponsor-GitHub-EA4AAA?style=for-the-badge&logo=github-sponsors)](https://github.com/sponsors/IdaAkiwumi)
[![PayPal](https://img.shields.io/badge/Donate-PayPal-00457C?style=for-the-badge&logo=paypal)](https://www.paypal.com/paypalme/iakiwumi)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://genre-sync-analytics.streamlit.app/)
![License: Proprietary](https://img.shields.io/badge/License-Proprietary-gold.svg)

**Genre Sync Analytics** is a strategic content intelligence dashboard designed for Studio Executives, Creative Producers, and Narrative Strategists. It transforms large-scale entertainment metadata into decision-ready visual signals that help users evaluate genre performance, audience appetite, and market saturation.

Rather than replacing creative instinct, Genre Sync helps **validate it**.

---

## 📌 What the Product Does

Genre Sync helps users explore:
- **Sentiment trends** across content categories
- **Market appetite** using popularity-based signals
- **Genre saturation vs opportunity**
- **Comparable titles** through interactive visualization
- **Scenario-based planning** for development and slate strategy

The platform aggregates and normalizes entertainment metadata across:
- Film
- TV series
- Streaming catalogs
- Documentary/reality content
- Short-form online video
- Explicit adult content detection

---

## 📈 Intelligence Metrics Defined

To provide a high-level strategic view, Genre Sync processes a large pipeline of global entertainment metadata, then narrows it into a lightweight intelligence dataset of **10,000 top-performing and high-visibility projects** for fast comparative analysis.

The dashboard surfaces three core decision signals:

### **Avg Sentiment ROI**
A normalized audience-response signal derived from rating-based fields across the dataset.  
A **High** score suggests stronger historical audience approval within the selected category mix.

### **Market Appetite**
A popularity-based demand indicator showing how visible and competitive the selected content space is relative to the wider dataset.

### **Genre Market Opportunity**
A "Blue Ocean vs Red Ocean" signal based on category saturation:
- **High Opportunity** = fewer comparable projects, more whitespace
- **Low Opportunity** = denser competitive space, heavier saturation

---

## ✨ Why Genre Sync Analytics?

In creative development, instinct matters — but instinct alone is hard to defend in a greenlight room.

Genre Sync provides a structured way to:
- Test genre hypotheses
- Compare narrative spaces
- Identify oversaturated vs underserved categories
- Inspect project-level comps quickly
- Save strategic scenarios for repeat review

### Key Strengths
- **Controlled taxonomy:** Normalizes messy source labels into a usable 24-bucket content taxonomy
- **Mixed-media intelligence:** Handles film, TV, shorts, streaming, and broader entertainment metadata
- **Executive-friendly UX:** Built for quick scanning, comparative review, and visual storytelling
- **Scenario workflow:** Supports saving, loading, exporting, and importing strategic genre combinations

---

## 🛠️ Tech Stack

| Layer | Technologies |
|-------|--------------|
| **Frontend / App** | Streamlit |
| **Data Processing** | Python, Pandas, NumPy |
| **Visualization** | Plotly |
| **Data Acquisition** | Kaggle API |
| **ETL Pipeline** | Custom Python scripts for extraction, transformation, and loading |

---

## 🔄 ETL Pipeline & Data Engineering

Genre Sync is powered by a custom-built **ETL (Extract, Transform, Load)** pipeline that consolidates fragmented entertainment datasets into a unified, analysis-ready intelligence ledger.

### Pipeline Overview

    ┌─────────────────────────────────────────────────────────────────────────┐
    │                           EXTRACT                                        │
    │  12 Kaggle datasets → Kaggle API authentication → Raw CSV ingestion     │
    │  (Ranging from 1MB curated sets to 930K+ record global catalogs)        │
    └─────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                          TRANSFORM                                       │
    │  • Schema routing (movies, cast, crew, genres, reviews)                 │
    │  • JSON parsing for TMDB-style nested credit fields                     │
    │  • Column normalization across inconsistent source schemas              │
    │  • Sentiment score standardization (-1 to 1 scale)                      │
    │  • Popularity score normalization                                       │
    │  • Genre taxonomy mapping (raw labels → 24 controlled buckets)          │
    │  • Duplicate detection via title normalization                          │
    │  • Adult content flagging using keyword detection                       │
    │  • Content type inference (Film, TV Series, Short, etc.)                │
    │  • Quality scoring for deduplication priority                           │
    └─────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                            LOAD                                          │
    │  • Sort by popularity (highest visibility first)                        │
    │  • Select top 10,000 projects for optimized app performance             │
    │  • Export to curated CSV files for Streamlit consumption                │
    │  • Cleanup of intermediate raw files                                    │
    └─────────────────────────────────────────────────────────────────────────┘

### Key Transformation Steps

| Step | Description |
|------|-------------|
| **Schema Detection** | Auto-routes incoming CSVs to appropriate tables (movies, cast, crew, reviews) based on column signatures |
| **JSON Credit Parsing** | Extracts structured cast/crew data from TMDB-style JSON strings embedded in CSV cells |
| **Title Normalization** | Strips articles ("The", "A"), removes year suffixes, handles encoding variations for accurate deduplication |
| **Genre Mapping** | Converts 200+ raw genre labels into 24 standardized content buckets via a curated lookup dictionary |
| **Sentiment Normalization** | Converts 0-10 rating scales to -1 to 1 range for consistent cross-source comparison |
| **Quality-Based Deduplication** | Prioritizes records with known genres, named talent, and valid IDs when merging duplicates |
| **Unknown Genre Rescue** | Secondary inference pass using title keywords and metadata fields to reduce "Unknown" classifications |

### Data Reduction Summary

| Stage | Record Count |
|-------|--------------|
| Raw ingestion (all sources) | ~2,000,000+ records |
| After schema routing & parsing | ~1,500,000 movie records |
| After deduplication | ~500,000 unique projects |
| **Final curated output** | **10,000 top projects** |

The final 10K dataset is optimized for:
- Fast browser-based visualization
- Meaningful comparative analysis
- Representative coverage across all 24 genre buckets

---

## 🧠 Data Architecture

Genre Sync's data layer is designed for **maintainability and extensibility**:

### Output Files

    data/
    ├── movie_database_movies_2026.csv    # Primary intelligence ledger (10K projects)
    ├── movie_database_cast_2026.csv      # Parsed cast/talent data
    ├── movie_database_crew_2026.csv      # Parsed crew/department data
    ├── movie_database_genres_2026.csv    # Genre lookup tables
    └── movie_database_reviews_2026.csv   # Review/rating data (where available)

### Primary Content Buckets (24 Categories)

    Action, Adventure, Animation, Biography, Comedy, Crime, Documentary, Drama,
    Family, Fantasy, History, Horror, Musical, Mystery, Reality, Romance,
    Sci-Fi, Short, Sports, Supernatural, Thriller, TV Series, War, Adult

### Data Freshness
- Pipeline supports automatic staleness detection (7-day threshold)
- Manual refresh available via `--refresh` flag
- Designed for scheduled runs via cron or GitHub Actions

---

## 📚 Data Sources

All source datasets are pulled from **Kaggle** via authenticated API and combined into a unified intelligence pipeline.

### Smallest / Highly Curated (1MB - 10MB)
- [TMDB Top 10,000 Popular Movies Dataset](https://www.kaggle.com/datasets/sankha1998/tmdb-top-10000-popular-movies-dataset)
- [IMDb Top 5000 Movies](https://www.kaggle.com/datasets/tiagoadrianunes/imdb-top-5000-movies)
- [Netflix Shows](https://www.kaggle.com/datasets/shivamb/netflix-shows)
- [Movies](https://www.kaggle.com/datasets/abdallahwagih/movies)
- [TMDB Movie Metadata](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata)

### Mid-Size / Comprehensive (15MB - 150MB)
- [The Movie Database (TMDB) Comprehensive Dataset](https://www.kaggle.com/datasets/rishabhkumar2003/the-movie-database-tmdb-comprehensive-dataset)
- [TMDB IMDb Merged Movies Dataset](https://www.kaggle.com/datasets/ggtejas/tmdb-imdb-merged-movies-dataset)
- [Movies](https://www.kaggle.com/datasets/mohammedalsubaie/movies)
- [IMDb Dataset](https://www.kaggle.com/datasets/ahmedosamamath/imdb-dataset)

### Large Scale / Global (250MB+)
- [TMDB Movies Dataset 2023 (930k Movies)](https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies)
- [TMDB Movies Daily Updates](https://www.kaggle.com/datasets/alanvourch/tmdb-movies-daily-updates)
- [Trending YouTube Videos – 113 Countries](https://www.kaggle.com/datasets/asaniczka/trending-youtube-videos-113-countries)

---

## 🔍 Industry Use Cases

### **Studio Executives**
Use market appetite and saturation signals to support greenlight conversations.

### **Creative Producers**
Compare category mixes, inspect comps, and identify whitespace for original IP.

### **Narrative Strategists**
Translate broad story instincts into structured market positioning signals.

### **Creative Operations / Research**
Build reusable scenarios and evaluate shifts in category opportunity.

---

## 🚀 Live App

[Launch Genre Sync Analytics](https://genre-sync-analytics.streamlit.app/)

---

## 📂 Repository Structure

    genre-sync-analytics/
    ├── streamlit_app.py          # Main Streamlit application
    ├── prep_data.py              # ETL pipeline script
    ├── requirements.txt          # Python dependencies
    ├── data/                     # Processed data outputs
    │   ├── movie_database_movies_2026.csv
    │   ├── movie_database_cast_2026.csv
    │   ├── movie_database_crew_2026.csv
    │   ├── movie_database_genres_2026.csv
    │   └── movie_database_reviews_2026.csv
    └── README.md

---

## 🧪 Running the Pipeline Locally

### Prerequisites
- Python 3.8+
- Kaggle API credentials (`~/.kaggle/kaggle.json`)

### Setup

    # Clone the repository
    git clone https://github.com/IdaAkiwumi/genre-sync-analytics.git
    cd genre-sync-analytics

    # Install dependencies
    pip install -r requirements.txt

    # Run the ETL pipeline
    python prep_data.py           # Normal run (skips if data is fresh)
    python prep_data.py --refresh # Force re-download from Kaggle

    # Launch the Streamlit app
    streamlit run streamlit_app.py

---

## 📖 Learning & Development Notes

This project reflects my ongoing journey into **data engineering**, combining product design instincts with hands-on technical implementation. The ETL pipeline was built while deepening my skills in:

- **Python** for data transformation and automation
- **SQL** for data querying and relational thinking
- **Pandas** for dataframe manipulation at scale
- **API integration** for programmatic data acquisition

Currently expanding these foundations through structured coursework on **DataCamp**, with a focus on building robust, maintainable data pipelines.

---

## ☕ Support the Narrative

If this tool helped you refine a slate, shape a creative strategy, or think more clearly about genre opportunity, consider supporting continued development:

- [Sponsor on GitHub](https://github.com/sponsors/IdaAkiwumi)
- [Donate via PayPal](https://www.paypal.com/paypalme/iakiwumi)

---

## 👩🏽‍💻 Developed by Ida Akiwumi

**Lead Product Designer | Product Architect | Narrative Strategist**

*Translating user friction into product opportunities.*

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin)](https://www.linkedin.com/in/idaa11)
