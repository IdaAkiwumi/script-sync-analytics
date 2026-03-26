# 🎬 Genre Sync Analytics
**Bridging the Gap Between Creative Intuition and Market Data.**

[![GitHub Sponsor](https://img.shields.io/badge/Sponsor-GitHub-EA4AAA?style=for-the-badge&logo=github-sponsors)](https://github.com/sponsors/IdaAkiwumi)
[![PayPal](https://img.shields.io/badge/Donate-PayPal-00457C?style=for-the-badge&logo=paypal)](https://www.paypal.com/paypalme/iakiwumi)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://genre-sync-analytics.streamlit.app/)
![License: Proprietary](https://img.shields.io/badge/License-Proprietary-gold.svg)

**Genre Sync Analytics** is a strategic content intelligence dashboard designed for Studio Executives, Creative Producers, and Narrative Strategists. It transforms large-scale entertainment metadata into decision-ready visual signals that help users evaluate genre performance, audience appetite, and market saturation.

Rather than replacing creative instinct, Genre Sync helps **validate it**.

---

## 📌 What the product does

Genre Sync helps users explore:
- **Sentiment trends** across content categories
- **Market appetite** using popularity-based signals
- **Genre saturation vs opportunity**
- **Comparable titles** through interactive visualization
- **Scenario-based planning** for development and slate strategy

The platform aggregates and normalizes entertainment metadata across:
- film
- TV series
- streaming catalogs
- documentary/reality content
- short-form online video
- explicit adult content detection

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
A “Blue Ocean vs Red Ocean” signal based on category saturation:
- **High Opportunity** = fewer comparable projects, more whitespace
- **Low Opportunity** = denser competitive space, heavier saturation

---

## ✨ Why Genre Sync Analytics?

In creative development, instinct matters — but instinct alone is hard to defend in a greenlight room.

Genre Sync provides a structured way to:
- test genre hypotheses
- compare narrative spaces
- identify oversaturated vs underserved categories
- inspect project-level comps quickly
- save strategic scenarios for repeat review

### Key strengths
- **Controlled taxonomy:** Normalizes messy source labels into a usable 24-bucket content taxonomy
- **Mixed-media intelligence:** Handles film, TV, shorts, streaming, and broader entertainment metadata
- **Executive-friendly UX:** Built for quick scanning, comparative review, and visual storytelling
- **Scenario workflow:** Supports saving, loading, exporting, and importing strategic genre combinations

---

## 🛠️ Tech Stack

- **Frontend / App Layer:** Streamlit
- **Data Processing:** Python, Pandas, NumPy
- **Visualization:** Plotly
- **Data Acquisition:** Kaggle API
- **Parsing / Normalization:** Custom Python pipeline for schema routing, deduplication, taxonomy mapping, and content-type inference

---

## 🧠 Data Architecture

Genre Sync is powered by a custom data pipeline that:

- ingests multiple Kaggle entertainment datasets
- routes movie, cast, crew, genre, and review tables separately
- parses TMDB-style JSON credit fields
- normalizes duplicate titles
- standardizes sentiment and popularity fields
- maps noisy source genres into **24 primary content buckets**
- flags explicit adult content
- infers broad content type
- outputs a curated top-10,000 project intelligence ledger for app performance

### Primary content buckets include:
`Action, Adventure, Animation, Biography, Comedy, Crime, Documentary, Drama, Family, Fantasy, History, Horror, Musical, Mystery, Reality, Romance, Sci-Fi, Short, Sports, Supernatural, Thriller, TV Series, War, Adult`

---

## 📚 Data Sources

All source datasets are pulled from **Kaggle** and combined into a unified intelligence pipeline.

### Smallest / Highly Curated
- [TMDB Top 10,000 Popular Movies Dataset](https://www.kaggle.com/datasets/sankha1998/tmdb-top-10000-popular-movies-dataset)
- [IMDb Top 5000 Movies](https://www.kaggle.com/datasets/tiagoadrianunes/imdb-top-5000-movies)
- [Netflix Shows](https://www.kaggle.com/datasets/shivamb/netflix-shows)
- [Movies](https://www.kaggle.com/datasets/abdallahwagih/movies)
- [TMDB Movie Metadata](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata)

### Mid-Size / Comprehensive
- [The Movie Database (TMDB) Comprehensive Dataset](https://www.kaggle.com/datasets/rishabhkumar2003/the-movie-database-tmdb-comprehensive-dataset)
- [TMDB IMDb Merged Movies Dataset](https://www.kaggle.com/datasets/ggtejas/tmdb-imdb-merged-movies-dataset)
- [Movies](https://www.kaggle.com/datasets/mohammedalsubaie/movies)
- [IMDb Dataset](https://www.kaggle.com/datasets/ahmedosamamath/imdb-dataset)

### Large Scale / Global
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

## ☕ Support the Narrative
If this tool helped you refine a slate, shape a creative strategy, or think more clearly about genre opportunity, consider supporting continued development:

- [Sponsor on GitHub](https://github.com/sponsors/IdaAkiwumi)
- [Donate via PayPal](https://www.paypal.com/paypalme/iakiwumi)

---

## 👩🏽‍💻 Developed by Ida Akiwumi
**Lead Product Designer | Product Architect | Narrative Strategist**

*Translating user friction into product opportunities.*
