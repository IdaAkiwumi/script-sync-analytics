import pandas as pd
import os
import kaggle
import glob
import time
import sys

# --- CONFIGURATION ---
DATA_DIR = "data"
DATASETS = [
    "asaniczka/tmdb-movies-dataset-2023-930k-movies",
    "shivamb/netflix-shows"
   # "asaniczka/trending-youtube-videos-113-countries"
]

TARGET_MOVIES = os.path.join(DATA_DIR, "movie_database_movies_2026.csv")

def setup_env():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    try:
        kaggle.api.authenticate()
        print("🔑 Kaggle Authenticated. - prep_data.py:23")
    except Exception as e:
        print(f"❌ Auth Error: Check C:/Users/kemia/.kaggle/kaggle.json - prep_data.py:25")

def fetch_data(force_refresh=False):
    """Fetches from Kaggle if data is missing, stale (>7 days), or forced."""
    seven_days_in_seconds = 7 * 24 * 60 * 60
    current_time = time.time()
    
    if force_refresh:
        print("🔄 Force Refresh active. Bypassing freshness checks... - prep_data.py:33")
    else:
        if os.path.exists(TARGET_MOVIES):
            file_age = current_time - os.path.getmtime(TARGET_MOVIES)
            if file_age < seven_days_in_seconds:
                print(f"⏭️  Intelligence is fresh (updated < 7 days ago). Skipping Kaggle sync. - prep_data.py:38")
                return

    tmdb_raw = glob.glob(os.path.join(DATA_DIR, "*TMDB*.csv"))
    nf_raw = os.path.exists(os.path.join(DATA_DIR, "netflix_titles.csv"))
    
    if force_refresh or not tmdb_raw or not nf_raw:
        print("📡 Fetching fresh streams from Kaggle... - prep_data.py:45")
        for slug in DATASETS:
            try:
                kaggle.api.dataset_download_files(slug, path=DATA_DIR, unzip=True)
                print(f"✅ Synced {slug} - prep_data.py:49")
            except Exception as e:
                print(f"⚠️ Skip/Error: {e} - prep_data.py:51")
    else:
        print("⏭️  Raw Intelligence detected locally. Proceeding to merge. - prep_data.py:53")

def cleanup_raw_data():
    """Removes heavy raw Kaggle CSVs and ZIPs to keep the workspace lean."""
    print("🧹 Cleaning up raw intelligence files... - prep_data.py:57")
    patterns = ["*TMDB*.csv", "netflix_titles.csv", "*videos*.csv", "*.zip"]
    
    for pattern in patterns:
        files = glob.glob(os.path.join(DATA_DIR, pattern))
        for f in files:
            try:
                os.remove(f)
                print(f"🗑️ Deleted: {os.path.basename(f)} - prep_data.py:65")
            except Exception as e:
                print(f"⚠️ Could not delete {f}: {e} - prep_data.py:67")

def process_and_merge():
    all_frames = []

    # --- 1. DEFINE GENRE MAPPING (To fix your screenshot issue) ---
    GENRE_MAP = {
        'Comedies': 'Comedy',
        'Dramas': 'Drama',
        'Horror Movies': 'Horror',
        'Sci-Fi & Fantasy': 'Science Fiction',
        'TV Sci-Fi & Fantasy': 'Science Fiction',
        'Romantic Movies': 'Romance',
        'Romantic TV Shows': 'Romance',
        'Thrillers': 'Thriller',
        'TV Thrillers': 'Thriller',
        'Action & Adventure': 'Action',
        'TV Action & Adventure': 'Action',
        'Anime Features': 'Animation',
        'Anime Series': 'Animation',
        'Entertainment': 'Streaming/YouTube',
        'People & Blogs': 'Streaming/YouTube',
        'Gaming': 'Streaming/YouTube',
        'Music': 'Streaming/YouTube',
        'Film & Animation': 'Streaming/YouTube'
    }

    ADULT_KEYWORDS = ['porn', 'erotica', 'hentai', 'adult film', 'hardcore']

    def clean_genre_logic(g):
        if not g or pd.isna(g): return "Other"
        first_g = str(g).split(',')[0].strip().split('&')[0].strip()
        return GENRE_MAP.get(first_g, first_g)

    def filter_adult(row):
        title = str(row.get('Project', '')).lower()
        if any(word in title for word in ADULT_KEYWORDS):
            return "Adult"
        return row.get('Genre', 'Other')

    # --- NEW: HARMONIZED INGESTION ---
    # We use glob to find ALL csv files downloaded from the slugs above
    all_csvs = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    for csv_file in all_csvs:
        filename = os.path.basename(csv_file)
        if filename == os.path.basename(TARGET_MOVIES): continue # Don't ingest the output
        
        try:
            temp_df = pd.read_csv(csv_file, low_memory=False)
            
            # 1. Harmonize Project Name
            name_cols = ['title', 'Series_Title', 'movie_title', 'original_title', 'track_name']
            for c in name_cols:
                if c in temp_df.columns:
                    temp_df = temp_df.rename(columns={c: 'Project'})
                    break
            
            # 2. Harmonize Genres
            genre_cols = ['genres', 'listed_in', 'genre', 'Genre']
            for c in genre_cols:
                if c in temp_df.columns:
                    temp_df = temp_df.rename(columns={c: 'Genre'})
                    break

            # 3. Harmonize Lead Talent (Starring)
            talent_cols = ['actors', 'stars', 'cast', 'Star1', 'Lead_Talent']
            for c in talent_cols:
                if c in temp_df.columns:
                    temp_df = temp_df.rename(columns={c: 'Lead_Talent'})
                    break

            # 4. Harmonize Scores
            score_cols = ['vote_average', 'IMDB_Rating', 'rating', 'score']
            for c in score_cols:
                if c in temp_df.columns:
                    temp_df = temp_df.rename(columns={c: 'Sentiment_Score'})
                    break

            pop_cols = ['popularity', 'view_count', 'No_of_Votes']
            for c in pop_cols:
                if c in temp_df.columns:
                    temp_df = temp_df.rename(columns={c: 'Popularity_Score'})
                    break

            # Ensure minimal requirements exist
            if 'Project' in temp_df.columns:
                # NEW: Source-specific Genre Tagging
                if "video" in filename.lower() or "youtube" in filename.lower():
                    temp_df['Genre'] = 'Streaming/YouTube'
                
                # Add placeholders if missing
                if 'Sentiment_Score' not in temp_df.columns: temp_df['Sentiment_Score'] = 0.5
                if 'Popularity_Score' not in temp_df.columns: temp_df['Popularity_Score'] = 50
                if 'Genre' not in temp_df.columns: temp_df['Genre'] = 'Unknown'
                if 'Lead_Talent' not in temp_df.columns: temp_df['Lead_Talent'] = 'Ensemble'
                if 'id' not in temp_df.columns: temp_df['id'] = range(len(temp_df))

                all_frames.append(temp_df)
                print(f"📊 Harmonized {filename} - prep_data.py:166")
        except Exception as e:
            print(f"⚠️ Could not process {filename}: {e} - prep_data.py:168")

    if not all_frames:
        print("❌ No data found to merge. - prep_data.py:171")
        return

    # --- 5. FINAL MERGE & REFINEMENT ---
    final_df = pd.concat(all_frames, ignore_index=True, sort=False)
    
    # FIX: Force scores to be numeric and handle the 'float vs str' error
    final_df['Sentiment_Score'] = pd.to_numeric(final_df['Sentiment_Score'], errors='coerce').fillna(0.5)
    final_df['Popularity_Score'] = pd.to_numeric(final_df['Popularity_Score'], errors='coerce').fillna(50)

    # Clean up any messy strings that could break the UI
    final_df['Lead_Talent'] = final_df['Lead_Talent'].astype(str).apply(lambda x: x.split(',')[0].replace('[', '').replace("'", "").strip())
    
    # Cleaning
    final_df['Genre'] = final_df['Genre'].apply(clean_genre_logic)
    final_df['Genre'] = final_df.apply(filter_adult, axis=1)
    
    # Now this math will work safely because the column is guaranteed to be numeric
    if final_df['Sentiment_Score'].max() > 1.1:
        final_df['Sentiment_Score'] = final_df['Sentiment_Score'] / 10.0

    # Sort and Drop Duplicates
    final_df = final_df.sort_values('Popularity_Score', ascending=False)
    final_df = final_df.drop_duplicates(subset=['Project'], keep='first')

    # Keep all columns but limit to top 10k for performance
    final_df = final_df.head(10000)



    final_df.to_csv(TARGET_MOVIES, index=False)
    print(f"🚀 MISSION COMPLETE: {len(final_df)} unique projects synced. - prep_data.py:202")

if __name__ == "__main__":
    refresh_flag = "--refresh" in sys.argv
    setup_env()
    fetch_data(force_refresh=refresh_flag)
    process_and_merge()
    cleanup_raw_data() # Trigger cleanup after everything is finished