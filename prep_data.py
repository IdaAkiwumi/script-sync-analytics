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
    "shivamb/netflix-shows",
    "asaniczka/trending-youtube-videos-113-countries"
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
        'Anime Series': 'Animation'
    }

    ADULT_KEYWORDS = ['porn', 'erotica', 'hentai', 'adult film', 'hardcore']

    def clean_genre_logic(g):
        if not g or pd.isna(g): return "Other"
        # Take the first genre in a list
        first_g = str(g).split(',')[0].strip().split('&')[0].strip()
        # Apply the mapping
        return GENRE_MAP.get(first_g, first_g)

    def filter_adult(row):
        title = str(row['Project']).lower()
        if any(word in title for word in ADULT_KEYWORDS):
            return "Adult"
        return row['Genre']

    # --- 2. TMDB ---
    tmdb_files = glob.glob(os.path.join(DATA_DIR, "*TMDB*.csv"))
    if tmdb_files:
        df = pd.read_csv(tmdb_files[0], low_memory=False)
        df = df.rename(columns={
            'title': 'Project',
            'vote_average': 'Sentiment_Score',
            'popularity': 'Popularity_Score',
            'genres': 'Genre'
        })
        # Basic TMDB cleaning
        if 'vote_count' in df.columns:
            df = df[df['vote_count'] > 50] # Lowered slightly to ensure 10k volume
        all_frames.append(df)

    # --- 3. NETFLIX ---
    nf_files = glob.glob(os.path.join(DATA_DIR, "netflix_titles.csv"))
    if nf_files:
        df_nf = pd.read_csv(nf_files[0])
        df_nf = df_nf.rename(columns={'title': 'Project', 'listed_in': 'Genre'})
        df_nf['Sentiment_Score'] = 0.65 
        df_nf['Popularity_Score'] = 65
        df_nf['id'] = range(900000, 900000 + len(df_nf))
        all_frames.append(df_nf)

    # --- 4. YOUTUBE ---
    yt_files = glob.glob(os.path.join(DATA_DIR, "*videos*.csv"))
    if yt_files:
        df_yt = pd.read_csv(yt_files[0], low_memory=False).head(10000)
        df_yt = df_yt.rename(columns={'title': 'Project'})
        df_yt['Genre'] = 'YouTube/Streaming'
        df_yt['Sentiment_Score'] = 0.70
        if 'view_count' in df_yt.columns:
            max_views = df_yt['view_count'].max()
            df_yt['Popularity_Score'] = (df_yt['view_count'] / max_views) * 100 if max_views > 0 else 0
        df_yt['id'] = range(1000000, 1000000 + len(df_yt))
        all_frames.append(df_yt)

    if not all_frames:
        print("❌ No data found. - prep_data.py:143")
        return

    # --- 5. FINAL MERGE & REFINEMENT ---
    final_df = pd.concat(all_frames, ignore_index=True, sort=False)
    
    # Apply Genre Cleaning
    final_df['Genre'] = final_df['Genre'].apply(clean_genre_logic)
    
    # Apply Adult Content Filter
    final_df['Genre'] = final_df.apply(filter_adult, axis=1)
    
    # Drop duplicates based on Project Name (keeps the highest popularity version)
    final_df = final_df.sort_values('Popularity_Score', ascending=False)
    final_df = final_df.drop_duplicates(subset=['Project'], keep='first')

    # Ensure we keep all columns you need for the UI
    final_df = final_df.head(10000) # Only keep the row limit, not the column limit

    final_df.to_csv(TARGET_MOVIES, index=False)
    print(f"🚀 MISSION COMPLETE: {len(final_df)} unique projects synced. - prep_data.py:163")

if __name__ == "__main__":
    refresh_flag = "--refresh" in sys.argv
    setup_env()
    fetch_data(force_refresh=refresh_flag)
    process_and_merge()
    cleanup_raw_data() # Trigger cleanup after everything is finished