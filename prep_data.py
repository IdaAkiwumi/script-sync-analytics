import pandas as pd
import os

# Configuration
DATA_DIR = "data"
LARGE_FILE = os.path.join(DATA_DIR, "TMDB_movie_dataset_v11_2024.csv")
TARGET_MOVIES = os.path.join(DATA_DIR, "movie_database_movies_2026.csv")
TARGET_CAST = os.path.join(DATA_DIR, "movie_database_cast_2026.csv")

def downsample_ecosystem():
    if not os.path.exists(LARGE_FILE):
        print(f"Error: {LARGE_FILE} not found! - prep_data.py:12")
        return

    print("Starting Narrative Data Sync - prep_data.py:15")
    
    # 1. Process Movies (The Anchor)
    # We use low_memory=False because TMDB files have mixed types
    df_movies = pd.read_csv(LARGE_FILE, low_memory=False)
    
    # Filter for quality: Only movies with a vote count > 100 to ensure meaningful analytics
    # and keep the top 10,000 by popularity.
    df_movies = df_movies[df_movies['vote_count'] > 100]
    df_slim_movies = df_movies.sort_values('popularity', ascending=False).head(10000)
    
    # Save the Anchor
    df_slim_movies.to_csv(TARGET_MOVIES, index=False)
    print(f"✅ Created Slim Movies: {TARGET_MOVIES} (~5MB) - prep_data.py:28")

    # 2. Sync Cast (Filtering cast to match only our 10,000 movies)
    # This prevents the 'Lead_Talent' merge from failing or being bloated
    cast_path = os.path.join(DATA_DIR, "movie_database_cast_2026.csv") # Existing cast file
    if os.path.exists(cast_path):
        df_cast = pd.read_csv(cast_path, low_memory=False)
        # Only keep cast members associated with our top 10k movies
        valid_ids = df_slim_movies['id'].unique()
        df_slim_cast = df_cast[df_cast['id'].isin(valid_ids)]
        
        df_slim_cast.to_csv(TARGET_CAST, index=False)
        print(f"✅ Synced & Slimmed Cast: {TARGET_CAST} - prep_data.py:40")

    print("Sync Complete. You can now delete the 600MB file. - prep_data.py:42")

if __name__ == "__main__":
    downsample_ecosystem()