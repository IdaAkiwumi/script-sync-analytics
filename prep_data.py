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
    "tmdb/tmdb-movie-metadata",
    "rishabhkumar2003/the-movie-database-tmdb-comprehensive-dataset"
]

# --- TARGET OUTPUT FILES (Your existing structure) ---
TARGET_MOVIES = os.path.join(DATA_DIR, "movie_database_movies_2026.csv")
TARGET_CAST = os.path.join(DATA_DIR, "movie_database_cast_2026.csv")
TARGET_CREW = os.path.join(DATA_DIR, "movie_database_crew_2026.csv")
TARGET_GENRES = os.path.join(DATA_DIR, "movie_database_genres_2026.csv")
TARGET_REVIEWS = os.path.join(DATA_DIR, "movie_database_reviews_2026.csv")

# Files to preserve (don't delete these during cleanup)
PRESERVE_FILES = [
    "movie_database_movies_2026.csv",
    "movie_database_cast_2026.csv",
    "movie_database_crew_2026.csv",
    "movie_database_genres_2026.csv",
    "movie_database_reviews_2026.csv"
]

def setup_env():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    try:
        kaggle.api.authenticate()
        print("🔑 Kaggle Authenticated. - prep_data.py:38")
    except Exception as e:
        print(f"❌ Auth Error: Check your kaggle.json file - prep_data.py:40")

def fetch_data(force_refresh=False):
    """Fetches from Kaggle if data is missing, stale (>7 days), or forced."""
    seven_days_in_seconds = 7 * 24 * 60 * 60
    current_time = time.time()
    
    if force_refresh:
        print("🔄 Force Refresh active. Bypassing freshness checks... - prep_data.py:48")
    else:
        if os.path.exists(TARGET_MOVIES):
            file_age = current_time - os.path.getmtime(TARGET_MOVIES)
            if file_age < seven_days_in_seconds:
                print(f"⏭️  Intelligence is fresh (updated < 7 days ago). Skipping Kaggle sync. - prep_data.py:53")
                return

    print("📡 Fetching fresh streams from Kaggle... - prep_data.py:56")
    for slug in DATASETS:
        try:
            kaggle.api.dataset_download_files(slug, path=DATA_DIR, unzip=True)
            print(f"✅ Synced {slug} - prep_data.py:60")
        except Exception as e:
            print(f"⚠️ Skip/Error: {e} - prep_data.py:62")

def cleanup_raw_data():
    """Removes heavy raw Kaggle CSVs and ZIPs, preserving your target files."""
    print("🧹 Cleaning up raw intelligence files... - prep_data.py:66")
    
    all_files = glob.glob(os.path.join(DATA_DIR, "*"))
    
    for f in all_files:
        filename = os.path.basename(f)
        
        # Skip our preserved output files
        if filename in PRESERVE_FILES:
            print(f"✅ Preserved: {filename} - prep_data.py:75")
            continue
        
        # Delete everything else (raw downloads, zips, etc.)
        try:
            if os.path.isfile(f):
                os.remove(f)
                print(f"🗑️ Deleted: {filename} - prep_data.py:82")
        except Exception as e:
            print(f"⚠️ Could not delete {filename}: {e} - prep_data.py:84")

def process_and_merge():
    all_frames = []
    cast_frames = []
    crew_frames = []
    genre_frames = []
    review_frames = []

    # --- 1. COMPREHENSIVE GENRE MAPPING ---
    GENRE_MAP = {
        # COMEDY variants
        'Comedies': 'Comedy',
        'Stand-Up Comedy': 'Comedy',
        'Stand-Up Comedy & Talk Shows': 'Comedy',
        'Talk Show': 'Comedy',
        'Late Night': 'Comedy',
        'Sitcom': 'Comedy',
        'Sketch Comedy': 'Comedy',
        'Parody': 'Comedy',
        'Satire': 'Comedy',
        'Dark Comedy': 'Comedy',
        'Romantic Comedies': 'Comedy',
        'Comedy-Drama': 'Comedy',
        
        # DRAMA variants
        'Dramas': 'Drama',
        'TV Dramas': 'Drama',
        'Crime Dramas': 'Drama',
        'Legal Drama': 'Drama',
        'Medical Drama': 'Drama',
        'Political Drama': 'Drama',
        'Period Drama': 'Drama',
        'Costume Drama': 'Drama',
        'Biographical Drama': 'Drama',
        'Social Issue Drama': 'Drama',
        'Teen Drama': 'Drama',
        'Soap Opera': 'Drama',
        'Melodrama': 'Drama',
        
        # HORROR variants
        'Horror Movies': 'Horror',
        'TV Horror': 'Horror',
        'Slasher': 'Horror',
        'Supernatural Horror': 'Horror',
        'Psychological Horror': 'Horror',
        'Body Horror': 'Horror',
        'Found Footage': 'Horror',
        'Zombie': 'Horror',
        'Monster Movies': 'Horror',
        'Creature Feature': 'Horror',
        'Gothic Horror': 'Horror',
        
        # SCIENCE FICTION variants
        'Sci-Fi': 'Science Fiction',
        'SciFi': 'Science Fiction',
        'SF': 'Science Fiction',
        'Sci-Fi & Fantasy': 'Science Fiction',
        'TV Sci-Fi & Fantasy': 'Science Fiction',
        'Space Opera': 'Science Fiction',
        'Cyberpunk': 'Science Fiction',
        'Dystopian': 'Science Fiction',
        'Post-Apocalyptic': 'Science Fiction',
        'Time Travel': 'Science Fiction',
        'Alien': 'Science Fiction',
        'Robot & AI': 'Science Fiction',
        
        # FANTASY variants
        'Fantasy': 'Fantasy',
        'Dark Fantasy': 'Fantasy',
        'Urban Fantasy': 'Fantasy',
        'High Fantasy': 'Fantasy',
        'Epic Fantasy': 'Fantasy',
        'Sword & Sorcery': 'Fantasy',
        'Fairy Tale': 'Fantasy',
        'Mythology': 'Fantasy',
        'Supernatural': 'Fantasy',
        
        # ROMANCE variants
        'Romantic Movies': 'Romance',
        'Romantic TV Shows': 'Romance',
        'Romance Movies': 'Romance',
        'Romantic Dramas': 'Romance',
        'Rom-Com': 'Romance',
        'Love Story': 'Romance',
        'Chick Flick': 'Romance',
        
        # THRILLER variants
        'Thrillers': 'Thriller',
        'TV Thrillers': 'Thriller',
        'Psychological Thriller': 'Thriller',
        'Crime Thriller': 'Thriller',
        'Spy Thriller': 'Thriller',
        'Political Thriller': 'Thriller',
        'Legal Thriller': 'Thriller',
        'Techno-Thriller': 'Thriller',
        'Erotic Thriller': 'Thriller',
        'Conspiracy': 'Thriller',
        'Suspense': 'Thriller',
        'Mystery': 'Thriller',
        'Whodunit': 'Thriller',
        'Neo-Noir': 'Thriller',
        'Film Noir': 'Thriller',
        
        # ACTION variants
        'Action & Adventure': 'Action',
        'TV Action & Adventure': 'Action',
        'Action-Adventure': 'Action',
        'Action Movies': 'Action',
        'Martial Arts': 'Action',
        'Kung Fu': 'Action',
        'Superhero': 'Action',
        'Comic Book': 'Action',
        'Disaster': 'Action',
        'War': 'Action',
        'War Movies': 'Action',
        'Military': 'Action',
        'Espionage': 'Action',
        'Heist': 'Action',
        'Caper': 'Action',
        'Chase': 'Action',
        'Swashbuckler': 'Action',
        'Western': 'Action',
        'Spaghetti Western': 'Action',
        
        # ANIMATION variants
        'Anime Features': 'Animation',
        'Anime Series': 'Animation',
        'Anime': 'Animation',
        'Animated': 'Animation',
        'Cartoon': 'Animation',
        'CGI': 'Animation',
        'Stop Motion': 'Animation',
        'Claymation': 'Animation',
        'Adult Animation': 'Animation',
        
        # DOCUMENTARY variants
        'Documentaries': 'Documentary',
        'Docuseries': 'Documentary',
        'Docudrama': 'Documentary',
        'True Crime': 'Documentary',
        'Nature Documentary': 'Documentary',
        'Historical Documentary': 'Documentary',
        'Music Documentary': 'Documentary',
        'Sports Documentary': 'Documentary',
        'Political Documentary': 'Documentary',
        'Biographical': 'Documentary',
        'Biography': 'Documentary',
        
        # TV SHOWS
        'TV Shows': 'TV Series',
        'TV Series': 'TV Series',
        'British TV Shows': 'TV Series',
        'Korean TV Shows': 'TV Series',
        'Spanish-Language TV Shows': 'TV Series',
        'International TV Shows': 'TV Series',
        'Kids TV': 'TV Series',
        "Children's TV": 'TV Series',
        'Reality TV': 'Reality',
        'Reality': 'Reality',
        'Competition Reality': 'Reality',
        'Dating Show': 'Reality',
        'Game Show': 'Reality',
        'Talent Show': 'Reality',
        'Makeover Show': 'Reality',
        'Cooking Show': 'Reality',
        'Travel Show': 'Reality',
        
        # FAMILY/KIDS variants
        'Family Movies': 'Family',
        'Kids': 'Family',
        "Children's": 'Family',
        'Family': 'Family',
        'Teen': 'Family',
        'Tween': 'Family',
        'Young Adult': 'Family',
        
        # MUSIC/PERFORMANCE variants
        'Music': 'Music & Musical',
        'Musical': 'Music & Musical',
        'Musicals': 'Music & Musical',
        'Concert Film': 'Music & Musical',
        'Music Video': 'Music & Musical',
        'Opera': 'Music & Musical',
        'Dance': 'Music & Musical',
        
        # SPORTS variants
        'Sports': 'Sports',
        'Sports Movies': 'Sports',
        'Sports Drama': 'Sports',
        'Boxing': 'Sports',
        'Football': 'Sports',
        'Baseball': 'Sports',
        'Basketball': 'Sports',
        'Racing': 'Sports',
        
        # STREAMING/DIGITAL variants
        'Entertainment': 'Streaming/YouTube',
        'People & Blogs': 'Streaming/YouTube',
        'Gaming': 'Streaming/YouTube',
        'Film & Animation': 'Streaming/YouTube',
        'Howto & Style': 'Streaming/YouTube',
        'How-To': 'Streaming/YouTube',
        'Vlog': 'Streaming/YouTube',
        'Podcast': 'Streaming/YouTube',
        'Web Series': 'Streaming/YouTube',
        'Short Film': 'Streaming/YouTube',
        'Shorts': 'Streaming/YouTube',
        
        # CRIME variants
        'Crime': 'Crime',
        'Crime Movies': 'Crime',
        'TV Crime': 'Crime',
        'Gangster': 'Crime',
        'Mob': 'Crime',
        'Mafia': 'Crime',
        'Prison': 'Crime',
        'Courtroom': 'Crime',
        
        # INTERNATIONAL/REGIONAL
        'Bollywood': 'International',
        'Korean Movies': 'International',
        'K-Drama': 'International',
        'Chinese Movies': 'International',
        'Japanese Movies': 'International',
        'French Movies': 'International',
        'Spanish Movies': 'International',
        'Latin American': 'International',
        'European': 'International',
        'African Movies': 'International',
        'Middle Eastern': 'International',
        'Indian Movies': 'International',
        
        # HOLIDAY/SEASONAL
        'Christmas': 'Holiday',
        'Halloween': 'Holiday',
        'Holiday Movies': 'Holiday',
        
        # FAITH/SPIRITUAL
        'Faith & Spirituality': 'Faith',
        'Christian': 'Faith',
        'Religious': 'Faith',
        'Inspirational': 'Faith',
        
        # LGBTQ+
        'LGBTQ Movies': 'LGBTQ+',
        'LGBTQ+': 'LGBTQ+',
        'Queer Cinema': 'LGBTQ+',
        
        # CLASSIC/CULT
        'Classic Movies': 'Classic',
        'Classics': 'Classic',
        'Cult Movies': 'Cult',
        'Cult Classics': 'Cult',
        'Cult': 'Cult',
        'B-Movie': 'Cult',
        'Exploitation': 'Cult',
        'Grindhouse': 'Cult',
        'Midnight Movie': 'Cult',
        'Camp': 'Cult',
        
        # ADULT (Keep separate)
        'Adult': 'Adult',
        'NC-17': 'Adult',
        'X-Rated': 'Adult',
        'Erotic': 'Adult',
        'Erotica': 'Adult',
        'Softcore': 'Adult',
        'Mature': 'Adult',
    }

    # Expanded adult detection keywords
    ADULT_KEYWORDS = [
        'porn', 'porno', 'pornographic', 
        'xxx', 'x-rated', 'xrated',
        'erotica', 'erotic film',
        'hentai', 'ecchi',
        'adult film', 'adult video', 'adult movie',
        'hardcore', 'softcore',
        'sexploitation', 'nudie',
        'blue film', 'stag film'
    ]

    # Genres that should NEVER be remapped to Adult
    PROTECTED_GENRES = [
        'Animation', 'Family', 'Documentary', 'Faith', 
        'Holiday', 'Sports', 'Reality', 'TV Series'
    ]

    def clean_genre_logic(g):
        """Map verbose genres to consolidated categories."""
        if not g or pd.isna(g): 
            return "Other"
        
        genre_str = str(g).strip()
        
        # Try exact match first
        if genre_str in GENRE_MAP:
            return GENRE_MAP[genre_str]
        
        # Try first genre if comma-separated
        first_g = genre_str.split(',')[0].strip()
        if first_g in GENRE_MAP:
            return GENRE_MAP[first_g]
        
        # Try partial match for future-proofing
        genre_lower = genre_str.lower()
        for key, value in GENRE_MAP.items():
            if key.lower() in genre_lower or genre_lower in key.lower():
                return value
        
        return first_g if first_g else "Other"

    def filter_adult(row):
        """Safely categorize adult content without false positives."""
        title = str(row.get('Project', '')).lower()
        current_genre = row.get('Genre', 'Other')
        
        if current_genre in PROTECTED_GENRES:
            return current_genre
        
        if any(word in title for word in ADULT_KEYWORDS):
            return "Adult"
        
        return current_genre

    def safe_read_csv(filepath):
        """Try multiple methods to read potentially malformed CSV files."""
        filename = os.path.basename(filepath)
        
        # Method 1: Standard read
        try:
            return pd.read_csv(filepath, low_memory=False)
        except Exception as e1:
            print(f"⚠️ Standard read failed for {filename}, trying with error handling... - prep_data.py:418")
        
        # Method 2: Skip bad lines
        try:
            return pd.read_csv(filepath, low_memory=False, on_bad_lines='skip')
        except Exception as e2:
            print(f"⚠️ Skip bad lines failed for {filename}, trying Python engine... - prep_data.py:424")
        
        # Method 3: Python engine (slower but more forgiving)
        try:
            return pd.read_csv(filepath, low_memory=False, engine='python', on_bad_lines='skip')
        except Exception as e3:
            print(f"⚠️ Python engine failed for {filename}, trying chunked read... - prep_data.py:430")
        
        # Method 4: Chunked read for very large files
        try:
            chunks = []
            for chunk in pd.read_csv(filepath, chunksize=50000, low_memory=False, on_bad_lines='skip'):
                chunks.append(chunk)
                if len(chunks) >= 20:  # Limit to ~1M rows
                    break
            if chunks:
                return pd.concat(chunks, ignore_index=True)
        except Exception as e4:
            print(f"❌ All read methods failed for {filename}: {e4} - prep_data.py:442")
        
        return None

    # --- FIND ALL RAW CSV FILES (excluding our output files) ---
    all_csvs = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    raw_csvs = [f for f in all_csvs if os.path.basename(f) not in PRESERVE_FILES]
    
    print(f"📂 Found {len(raw_csvs)} raw CSV files to process. - prep_data.py:450")
    
    for csv_file in raw_csvs:
        filename = os.path.basename(csv_file).lower()
        
        # Use safe read method
        temp_df = safe_read_csv(csv_file)
        
        if temp_df is None:
            print(f"⏭️ Skipping {filename} (could not read) - prep_data.py:459")
            continue
            
        try:
            # --- ROUTE TO APPROPRIATE OUTPUT FILE ---
            
            # CAST data
            if 'cast' in filename or ('character' in temp_df.columns if hasattr(temp_df, 'columns') else False):
                if 'name' in temp_df.columns or 'cast' in temp_df.columns:
                    cast_frames.append(temp_df)
                    print(f"🎭 Routed to CAST: {filename} - prep_data.py:469")
                    continue
            
            # CREW data
            if 'crew' in filename or 'director' in filename:
                if 'job' in temp_df.columns or 'department' in temp_df.columns:
                    crew_frames.append(temp_df)
                    print(f"🎬 Routed to CREW: {filename} - prep_data.py:476")
                    continue
            
            # REVIEWS data
            if 'review' in filename:
                review_frames.append(temp_df)
                print(f"📝 Routed to REVIEWS: {filename} - prep_data.py:482")
                continue
            
            # GENRES lookup data (small reference tables)
            if 'genre' in filename and len(temp_df) < 100:
                genre_frames.append(temp_df)
                print(f"🏷️ Routed to GENRES: {filename} - prep_data.py:488")
                continue
            
            # --- MAIN MOVIES DATA ---
            # Harmonize Project Name
            name_cols = ['title', 'Series_Title', 'movie_title', 'original_title', 'track_name', 'name']
            project_found = False
            for c in name_cols:
                if c in temp_df.columns:
                    temp_df = temp_df.rename(columns={c: 'Project'})
                    project_found = True
                    break
            
            # Harmonize Genres
            genre_cols = ['genres', 'listed_in', 'genre', 'Genre']
            for c in genre_cols:
                if c in temp_df.columns and c != 'Genre':
                    temp_df = temp_df.rename(columns={c: 'Genre'})
                    break

            # Harmonize Lead Talent - FIX: Only rename if Lead_Talent doesn't already exist
            talent_cols = ['actors', 'stars', 'cast', 'Star1']
            if 'Lead_Talent' not in temp_df.columns:
                for c in talent_cols:
                    if c in temp_df.columns:
                        temp_df = temp_df.rename(columns={c: 'Lead_Talent'})
                        break

            # Harmonize Scores
            score_cols = ['vote_average', 'IMDB_Rating', 'rating', 'score']
            for c in score_cols:
                if c in temp_df.columns and c != 'Sentiment_Score':
                    temp_df = temp_df.rename(columns={c: 'Sentiment_Score'})
                    break

            pop_cols = ['popularity', 'view_count', 'No_of_Votes']
            for c in pop_cols:
                if c in temp_df.columns and c != 'Popularity_Score':
                    temp_df = temp_df.rename(columns={c: 'Popularity_Score'})
                    break

            if 'Project' in temp_df.columns:
                # Source-specific Genre Tagging
                if "video" in filename or "youtube" in filename:
                    temp_df['Genre'] = 'Streaming/YouTube'
                
                # Add placeholders if missing
                if 'Sentiment_Score' not in temp_df.columns: 
                    temp_df['Sentiment_Score'] = 0.5
                if 'Popularity_Score' not in temp_df.columns: 
                    temp_df['Popularity_Score'] = 50
                if 'Genre' not in temp_df.columns: 
                    temp_df['Genre'] = 'Unknown'
                if 'Lead_Talent' not in temp_df.columns: 
                    temp_df['Lead_Talent'] = 'Ensemble'
                if 'id' not in temp_df.columns: 
                    temp_df['id'] = range(len(temp_df))

                all_frames.append(temp_df)
                print(f"🎬 Routed to MOVIES: {filename} ({len(temp_df)} rows) - prep_data.py:547")
                
        except Exception as e:
            print(f"⚠️ Could not process {filename}: {e} - prep_data.py:550")

    # --- PROCESS MAIN MOVIES DATA ---
    if all_frames:
        print(f"\n📊 Merging {len(all_frames)} movie datasets... - prep_data.py:554")
        final_df = pd.concat(all_frames, ignore_index=True, sort=False)
        
        # FIX: Remove duplicate columns BEFORE any processing
        # This handles the case where multiple source files have similar column names
        final_df = final_df.loc[:, ~final_df.columns.duplicated(keep='first')]
        print(f"📋 Removed duplicate columns. Remaining: {len(final_df.columns)} columns - prep_data.py:560")
        
        # Force scores to be numeric
        final_df['Sentiment_Score'] = pd.to_numeric(final_df['Sentiment_Score'], errors='coerce').fillna(0.5)
        final_df['Popularity_Score'] = pd.to_numeric(final_df['Popularity_Score'], errors='coerce').fillna(50)

        # Clean Lead Talent - ensure it exists and is clean
        if 'Lead_Talent' in final_df.columns:
            final_df['Lead_Talent'] = final_df['Lead_Talent'].astype(str).apply(
                lambda x: x.split(',')[0].replace('[', '').replace("'", "").replace('"', '').replace('{', '').strip()[:100]
            )
        else:
            final_df['Lead_Talent'] = 'Ensemble'
        
        # Apply genre cleaning and adult filtering
        final_df['Genre'] = final_df['Genre'].apply(clean_genre_logic)
        final_df['Genre'] = final_df.apply(filter_adult, axis=1)
        
        # Normalize sentiment scores
        if final_df['Sentiment_Score'].max() > 1.1:
            final_df['Sentiment_Score'] = final_df['Sentiment_Score'] / 10.0

        # Sort and deduplicate
        final_df = final_df.sort_values('Popularity_Score', ascending=False)
        final_df = final_df.drop_duplicates(subset=['Project'], keep='first')

        # Limit to top 10k for performance
        final_df = final_df.head(10000)

        # --- REORDER COLUMNS: Filled columns first, sparse columns last ---
        def count_non_empty(col):
            """Count non-null, non-empty values in a column."""
            non_null = final_df[col].notna().sum()
            non_empty = (final_df[col].astype(str).str.strip() != '').sum()
            return min(non_null, non_empty)

        sorted_columns = sorted(final_df.columns, key=count_non_empty, reverse=True)
        final_df = final_df[sorted_columns]
        print(f"📋 Columns reordered by data density. - prep_data.py:598")
        
        # FIX: Final verification - ensure no duplicate columns
        if final_df.columns.duplicated().any():
            print("⚠️ Warning: Still found duplicate columns, removing... - prep_data.py:602")
            final_df = final_df.loc[:, ~final_df.columns.duplicated(keep='first')]

        # Save to target file
        final_df.to_csv(TARGET_MOVIES, index=False)
        print(f"✅ MOVIES: {len(final_df)} unique projects saved to {TARGET_MOVIES} - prep_data.py:607")
        print(f"Columns: {list(final_df.columns[:10])}... ({len(final_df.columns)} total) - prep_data.py:608")
    else:
        print("⚠️ No movie data found to merge. - prep_data.py:610")

    # --- PROCESS CAST DATA ---
    if cast_frames:
        cast_df = pd.concat(cast_frames, ignore_index=True, sort=False)
        cast_df = cast_df.loc[:, ~cast_df.columns.duplicated(keep='first')]  # Remove duplicate columns
        cast_df = cast_df.drop_duplicates()
        
        # Reorder columns by density
        def count_non_empty_cast(col):
            non_null = cast_df[col].notna().sum()
            non_empty = (cast_df[col].astype(str).str.strip() != '').sum()
            return min(non_null, non_empty)
        
        sorted_cols = sorted(cast_df.columns, key=count_non_empty_cast, reverse=True)
        cast_df = cast_df[sorted_cols]
        
        cast_df.to_csv(TARGET_CAST, index=False)
        print(f"✅ CAST: {len(cast_df)} records saved to {TARGET_CAST} - prep_data.py:628")

    # --- PROCESS CREW DATA ---
    if crew_frames:
        crew_df = pd.concat(crew_frames, ignore_index=True, sort=False)
        crew_df = crew_df.loc[:, ~crew_df.columns.duplicated(keep='first')]  # Remove duplicate columns
        crew_df = crew_df.drop_duplicates()
        
        def count_non_empty_crew(col):
            non_null = crew_df[col].notna().sum()
            non_empty = (crew_df[col].astype(str).str.strip() != '').sum()
            return min(non_null, non_empty)
        
        sorted_cols = sorted(crew_df.columns, key=count_non_empty_crew, reverse=True)
        crew_df = crew_df[sorted_cols]
        
        crew_df.to_csv(TARGET_CREW, index=False)
        print(f"✅ CREW: {len(crew_df)} records saved to {TARGET_CREW} - prep_data.py:645")

    # --- PROCESS GENRES LOOKUP DATA ---
    if genre_frames:
        genre_df = pd.concat(genre_frames, ignore_index=True, sort=False)
        genre_df = genre_df.loc[:, ~genre_df.columns.duplicated(keep='first')]  # Remove duplicate columns
        genre_df = genre_df.drop_duplicates()
        genre_df.to_csv(TARGET_GENRES, index=False)
        print(f"✅ GENRES: {len(genre_df)} records saved to {TARGET_GENRES} - prep_data.py:653")

    # --- PROCESS REVIEWS DATA ---
    if review_frames:
        review_df = pd.concat(review_frames, ignore_index=True, sort=False)
        review_df = review_df.loc[:, ~review_df.columns.duplicated(keep='first')]  # Remove duplicate columns
        review_df = review_df.drop_duplicates()
        
        def count_non_empty_review(col):
            non_null = review_df[col].notna().sum()
            non_empty = (review_df[col].astype(str).str.strip() != '').sum()
            return min(non_null, non_empty)
        
        sorted_cols = sorted(review_df.columns, key=count_non_empty_review, reverse=True)
        review_df = review_df[sorted_cols]
        
        review_df.to_csv(TARGET_REVIEWS, index=False)
        print(f"✅ REVIEWS: {len(review_df)} records saved to {TARGET_REVIEWS} - prep_data.py:670")

    print(f"\n🚀 MISSION COMPLETE - prep_data.py:672")

if __name__ == "__main__":
    refresh_flag = "--refresh" in sys.argv
    setup_env()
    fetch_data(force_refresh=refresh_flag)
    process_and_merge()
    cleanup_raw_data()