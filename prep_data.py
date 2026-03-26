import pandas as pd
import os
import kaggle
import glob
import time
import sys
import re

# --- CONFIGURATION ---
DATA_DIR = "data"
DATASETS = [
    "asaniczka/tmdb-movies-dataset-2023-930k-movies",
    "shivamb/netflix-shows",
    "tmdb/tmdb-movie-metadata",
    "rishabhkumar2003/the-movie-database-tmdb-comprehensive-dataset"
]

# --- TARGET OUTPUT FILES ---
TARGET_MOVIES = os.path.join(DATA_DIR, "movie_database_movies_2026.csv")
TARGET_CAST = os.path.join(DATA_DIR, "movie_database_cast_2026.csv")
TARGET_CREW = os.path.join(DATA_DIR, "movie_database_crew_2026.csv")
TARGET_GENRES = os.path.join(DATA_DIR, "movie_database_genres_2026.csv")
TARGET_REVIEWS = os.path.join(DATA_DIR, "movie_database_reviews_2026.csv")

# Files to preserve (don't delete during cleanup)
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
        print("🔑 Kaggle Authenticated. - prep_data.py:39")
    except Exception as e:
        print(f"❌ Auth Error: Check your kaggle.json file - prep_data.py:41")

def fetch_data(force_refresh=False):
    """Fetches from Kaggle if data is missing, stale (>7 days), or forced."""
    seven_days_in_seconds = 7 * 24 * 60 * 60
    current_time = time.time()
    
    if force_refresh:
        print("🔄 Force Refresh active. Bypassing freshness checks... - prep_data.py:49")
    else:
        if os.path.exists(TARGET_MOVIES):
            file_age = current_time - os.path.getmtime(TARGET_MOVIES)
            if file_age < seven_days_in_seconds:
                print(f"⏭️  Intelligence is fresh (updated < 7 days ago). Skipping Kaggle sync. - prep_data.py:54")
                return

    print("📡 Fetching fresh streams from Kaggle... - prep_data.py:57")
    for slug in DATASETS:
        try:
            kaggle.api.dataset_download_files(slug, path=DATA_DIR, unzip=True)
            print(f"✅ Synced {slug} - prep_data.py:61")
        except Exception as e:
            print(f"⚠️ Skip/Error: {e} - prep_data.py:63")

def cleanup_raw_data():
    """Removes raw Kaggle CSVs and ZIPs, preserving target files."""
    print("🧹 Cleaning up raw intelligence files... - prep_data.py:67")
    
    all_files = glob.glob(os.path.join(DATA_DIR, "*"))
    
    for f in all_files:
        filename = os.path.basename(f)
        
        if filename in PRESERVE_FILES:
            print(f"✅ Preserved: {filename} - prep_data.py:75")
            continue
        
        try:
            if os.path.isfile(f):
                os.remove(f)
                print(f"🗑️ Deleted: {filename} - prep_data.py:81")
        except Exception as e:
            print(f"⚠️ Could not delete {filename}: {e} - prep_data.py:83")

def normalize_title(title):
    """Normalize title for duplicate detection."""
    if not title or pd.isna(title):
        return ""
    t = str(title).lower().strip()
    t = t.replace('(film)', '').replace('(movie)', '').replace('(tv series)', '').replace('(tv)', '')
    t = re.sub(r'\s*[\($$]\d{4}[\)$$]', '', t)
    t = re.sub(r'\s*\d{4}$', '', t)
    if ', the' in t:
        t = 'the ' + t.replace(', the', '')
    if ', a ' in t:
        t = 'a ' + t.replace(', a', '')
    if t.startswith('the '):
        t = t[4:]
    t = re.sub(r'[^\w\s]', '', t)
    t = ' '.join(t.split())
    return t.strip()

def process_and_merge():
    all_frames = []
    cast_frames = []
    crew_frames = []
    genre_frames = []
    review_frames = []

    # --- STREAMLIT-OPTIMIZED GENRE MAPPING (~20 Core Genres) ---
    GENRE_MAP = {
        # ============ COMEDY ============
        'Comedy': 'Comedy',
        'Comedies': 'Comedy',
        'Comedy Movies': 'Comedy',
        'Stand-Up Comedy': 'Comedy',
        'Stand-Up Comedy & Talk Shows': 'Comedy',
        'Stand Up': 'Comedy',
        'Standup': 'Comedy',
        'Talk Show': 'Comedy',
        'Talk Shows': 'Comedy',
        'Late Night': 'Comedy',
        'Sitcom': 'Comedy',
        'Sketch Comedy': 'Comedy',
        'Parody': 'Comedy',
        'Satire': 'Comedy',
        'Dark Comedy': 'Comedy',
        'Black Comedy': 'Comedy',
        'Romantic Comedies': 'Comedy',
        'Romantic Comedy': 'Comedy',
        'Rom-Com': 'Comedy',
        'Comedy-Drama': 'Comedy',
        'Dramedy': 'Comedy',
        'Slapstick': 'Comedy',
        'Screwball Comedy': 'Comedy',
        'Mockumentary': 'Comedy',
        
        # ============ DRAMA ============
        'Drama': 'Drama',
        'Dramas': 'Drama',
        'Drama Movies': 'Drama',
        'TV Dramas': 'Drama',
        'Legal Drama': 'Drama',
        'Medical Drama': 'Drama',
        'Political Drama': 'Drama',
        'Period Drama': 'Drama',
        'Costume Drama': 'Drama',
        'Historical Drama': 'Drama',
        'Historical': 'Drama',
        'History': 'Drama',
        'Biographical Drama': 'Drama',
        'Biography': 'Drama',
        'Biographical': 'Drama',
        'Biopic': 'Drama',
        'Bio-Pic': 'Drama',
        'Social Issue Drama': 'Drama',
        'Teen Drama': 'Drama',
        'Coming of Age': 'Drama',
        'Soap Opera': 'Drama',
        'Melodrama': 'Drama',
        'Family Drama': 'Drama',
        'Psychological Drama': 'Drama',
        'Courtroom': 'Drama',
        'Legal': 'Drama',
        'Medical': 'Drama',
        'Political': 'Drama',
        'Period Piece': 'Drama',
        'Epic': 'Drama',
        'Tragedy': 'Drama',
        'Faith & Spirituality': 'Drama',
        'Faith': 'Drama',
        'Christian': 'Drama',
        'Christian Movies': 'Drama',
        'Religious': 'Drama',
        'Inspirational': 'Drama',
        'Spiritual': 'Drama',
        'LGBTQ Movies': 'Drama',
        'LGBTQ+': 'Drama',
        'LGBTQ': 'Drama',
        'LGBT': 'Drama',
        'Queer Cinema': 'Drama',
        'Gay': 'Drama',
        'Lesbian': 'Drama',
        
        # ============ ACTION ============
        'Action': 'Action',
        'Action & Adventure': 'Action',
        'TV Action & Adventure': 'Action',
        'Action-Adventure': 'Action',
        'Action Movies': 'Action',
        'Action Film': 'Action',
        'Adventure': 'Action',
        'Adventure Movies': 'Action',
        'Adventures': 'Action',
        'Martial Arts': 'Action',
        'Martial Arts Film': 'Action',
        'Kung Fu': 'Action',
        'Superhero': 'Action',
        'Superhero Movies': 'Action',
        'Comic Book': 'Action',
        'Disaster': 'Action',
        'Disaster Film': 'Action',
        'Spy': 'Action',
        'Spy Movies': 'Action',
        'Spy Film': 'Action',
        'Espionage': 'Action',
        'Secret Agent': 'Action',
        'Heist': 'Action',
        'Heist Film': 'Action',
        'Caper': 'Action',
        'Swashbuckler': 'Action',
        'Samurai': 'Action',
        'Stunt': 'Action',
        
        # ============ WAR (Separate) ============
        'War': 'War',
        'War Movies': 'War',
        'War Film': 'War',
        'Military': 'War',
        'Anti-War': 'War',
        'Combat': 'War',
        'World War': 'War',
        'Vietnam': 'War',
        'WWII': 'War',
        'WWI': 'War',
        
        # ============ WESTERN (Separate) ============
        'Western': 'Western',
        'Westerns': 'Western',
        'Western Movies': 'Western',
        'Spaghetti Western': 'Western',
        'Neo-Western': 'Western',
        'Cowboy': 'Western',
        'Frontier': 'Western',
        
        # ============ CRIME (Separate) ============
        'Crime': 'Crime',
        'Crime Movies': 'Crime',
        'Crime Films': 'Crime',
        'TV Crime': 'Crime',
        'Gangster': 'Crime',
        'Gangster Film': 'Crime',
        'Mob': 'Crime',
        'Mafia': 'Crime',
        'Prison': 'Crime',
        'Prison Film': 'Crime',
        'Heist': 'Crime',
        'True Crime': 'Crime',
        'Organized Crime': 'Crime',
        'Drug': 'Crime',
        'Cartel': 'Crime',
        
        # ============ MYSTERY (Separate) ============
        'Mystery': 'Mystery',
        'Mysteries': 'Mystery',
        'Mystery Movies': 'Mystery',
        'Whodunit': 'Mystery',
        'Detective': 'Mystery',
        'Cozy Mystery': 'Mystery',
        'Murder Mystery': 'Mystery',
        'Amateur Sleuth': 'Mystery',
        
        # ============ THRILLER ============
        'Thriller': 'Thriller',
        'Thrillers': 'Thriller',
        'Thriller Movies': 'Thriller',
        'TV Thrillers': 'Thriller',
        'Psychological Thriller': 'Thriller',
        'Psychological': 'Thriller',
        'Crime Thriller': 'Thriller',
        'Spy Thriller': 'Thriller',
        'Political Thriller': 'Thriller',
        'Legal Thriller': 'Thriller',
        'Techno-Thriller': 'Thriller',
        'Conspiracy': 'Thriller',
        'Suspense': 'Thriller',
        'Suspenseful': 'Thriller',
        'Neo-Noir': 'Thriller',
        'Film Noir': 'Thriller',
        'Noir': 'Thriller',
        
        # ============ HORROR ============
        'Horror': 'Horror',
        'Horror Movies': 'Horror',
        'Horrors': 'Horror',
        'TV Horror': 'Horror',
        'Slasher': 'Horror',
        'Slasher Film': 'Horror',
        'Psychological Horror': 'Horror',
        'Body Horror': 'Horror',
        'Found Footage': 'Horror',
        'Zombie': 'Horror',
        'Zombies': 'Horror',
        'Zombie Film': 'Horror',
        'Monster Movies': 'Horror',
        'Monster': 'Horror',
        'Creature Feature': 'Horror',
        'Gothic Horror': 'Horror',
        'Gothic': 'Horror',
        'Splatter': 'Horror',
        'Survival Horror': 'Horror',
        'Vampire': 'Horror',
        'Werewolf': 'Horror',
        'Demonic': 'Horror',
        'Occult': 'Horror',
        'Halloween': 'Horror',
        
        # ============ SUPERNATURAL (Separate) ============
        'Supernatural': 'Supernatural',
        'Supernatural Horror': 'Supernatural',
        'Paranormal': 'Supernatural',
        'Ghost': 'Supernatural',
        'Ghosts': 'Supernatural',
        'Haunted': 'Supernatural',
        'Haunting': 'Supernatural',
        'Poltergeist': 'Supernatural',
        'Possession': 'Supernatural',
        'Exorcism': 'Supernatural',
        'Afterlife': 'Supernatural',
        'Angels': 'Supernatural',
        'Demons': 'Supernatural',
        
        # ============ SCI-FI ============
        'Science Fiction': 'Sci-Fi',
        'Sci-Fi': 'Sci-Fi',
        'SciFi': 'Sci-Fi',
        'SF': 'Sci-Fi',
        'Sci-Fi & Fantasy': 'Sci-Fi',
        'TV Sci-Fi & Fantasy': 'Sci-Fi',
        'Science Fiction Movies': 'Sci-Fi',
        'Sci-Fi Movies': 'Sci-Fi',
        'Space Opera': 'Sci-Fi',
        'Space': 'Sci-Fi',
        'Cyberpunk': 'Sci-Fi',
        'Dystopian': 'Sci-Fi',
        'Dystopia': 'Sci-Fi',
        'Post-Apocalyptic': 'Sci-Fi',
        'Apocalyptic': 'Sci-Fi',
        'Time Travel': 'Sci-Fi',
        'Alien': 'Sci-Fi',
        'Aliens': 'Sci-Fi',
        'Robot & AI': 'Sci-Fi',
        'Robots': 'Sci-Fi',
        'Futuristic': 'Sci-Fi',
        'Tech': 'Sci-Fi',
        'Virtual Reality': 'Sci-Fi',
        'Steampunk': 'Sci-Fi',
        
        # ============ FANTASY ============
        'Fantasy': 'Fantasy',
        'Fantasy Movies': 'Fantasy',
        'Dark Fantasy': 'Fantasy',
        'Urban Fantasy': 'Fantasy',
        'High Fantasy': 'Fantasy',
        'Epic Fantasy': 'Fantasy',
        'Sword & Sorcery': 'Fantasy',
        'Sword and Sorcery': 'Fantasy',
        'Fairy Tale': 'Fantasy',
        'Fairy Tales': 'Fantasy',
        'Mythology': 'Fantasy',
        'Myth': 'Fantasy',
        'Magic': 'Fantasy',
        'Magical Realism': 'Fantasy',
        'Wizards': 'Fantasy',
        'Dragons': 'Fantasy',
        'Medieval': 'Fantasy',
        
        # ============ ROMANCE ============
        'Romance': 'Romance',
        'Romantic Movies': 'Romance',
        'Romantic TV Shows': 'Romance',
        'Romance Movies': 'Romance',
        'Romance Film': 'Romance',
        'Romantic Dramas': 'Romance',
        'Love Story': 'Romance',
        'Chick Flick': 'Romance',
        'Love': 'Romance',
        'Wedding': 'Romance',
        
        # ============ ANIMATION ============
        'Animation': 'Animation',
        'Anime Features': 'Animation',
        'Anime Series': 'Animation',
        'Anime': 'Animation',
        'Animated': 'Animation',
        'Animated Movies': 'Animation',
        'Cartoon': 'Animation',
        'Cartoons': 'Animation',
        'CGI': 'Animation',
        'Computer Animation': 'Animation',
        'Stop Motion': 'Animation',
        'Claymation': 'Animation',
        'Adult Animation': 'Animation',
        'Pixar': 'Animation',
        'Disney': 'Animation',
        'Dreamworks': 'Animation',
        'Studio Ghibli': 'Animation',
        'Shonen': 'Animation',
        'Shojo': 'Animation',
        'Isekai': 'Animation',
        'Mecha': 'Animation',
        
        # ============ DOCUMENTARY ============
        'Documentary': 'Documentary',
        'Documentaries': 'Documentary',
        'Documentary Movies': 'Documentary',
        'Docuseries': 'Documentary',
        'Docudrama': 'Documentary',
        'Nature Documentary': 'Documentary',
        'Nature': 'Documentary',
        'Wildlife': 'Documentary',
        'Historical Documentary': 'Documentary',
        'Music Documentary': 'Documentary',
        'Sports Documentary': 'Documentary',
        'Political Documentary': 'Documentary',
        'Science': 'Documentary',
        'Educational': 'Documentary',
        'Instructional': 'Documentary',
        'News': 'Documentary',
        'News & Politics': 'Documentary',
        
        # ============ FAMILY ============
        'Family': 'Family',
        'Family Movies': 'Family',
        'Family Films': 'Family',
        'Kids': 'Family',
        'Kids Movies': 'Family',
        "Children's": 'Family',
        "Children's Movies": 'Family',
        'Children': 'Family',
        'Teen': 'Family',
        'Teen Movies': 'Family',
        'Tween': 'Family',
        'Young Adult': 'Family',
        'Preschool': 'Family',
        'Kids TV': 'Family',
        "Children's TV": 'Family',
        'Holiday': 'Family',
        'Christmas': 'Family',
        'Christmas Movies': 'Family',
        
        # ============ TV SERIES ============
        'TV Shows': 'TV Series',
        'TV Series': 'TV Series',
        'Television': 'TV Series',
        'TV': 'TV Series',
        'British TV Shows': 'TV Series',
        'Korean TV Shows': 'TV Series',
        'Spanish-Language TV Shows': 'TV Series',
        'International TV Shows': 'TV Series',
        'Miniseries': 'TV Series',
        'Mini-Series': 'TV Series',
        'Limited Series': 'TV Series',
        'Anthology': 'TV Series',
        'Web Series': 'TV Series',
        'Streaming': 'TV Series',
        
        # ============ REALITY ============
        'Reality TV': 'Reality',
        'Reality': 'Reality',
        'Reality Shows': 'Reality',
        'Competition Reality': 'Reality',
        'Competition': 'Reality',
        'Dating Show': 'Reality',
        'Dating': 'Reality',
        'Game Show': 'Reality',
        'Game Shows': 'Reality',
        'Talent Show': 'Reality',
        'Makeover Show': 'Reality',
        'Makeover': 'Reality',
        'Cooking Show': 'Reality',
        'Cooking': 'Reality',
        'Food': 'Reality',
        'Travel Show': 'Reality',
        'Travel': 'Reality',
        'Lifestyle': 'Reality',
        'Home Improvement': 'Reality',
        'Renovation': 'Reality',
        'Entertainment': 'Reality',
        'People & Blogs': 'Reality',
        'Vlog': 'Reality',
        'Podcast': 'Reality',
        'ASMR': 'Reality',
        'Commentary': 'Reality',
        'Reaction': 'Reality',
        
        # ============ INTERNATIONAL ============
        'International': 'International',
        'International Movies': 'International',
        'Foreign': 'International',
        'Foreign Film': 'International',
        'Foreign Language': 'International',
        'World Cinema': 'International',
        'Bollywood': 'International',
        'Korean Movies': 'International',
        'Korean': 'International',
        'K-Drama': 'International',
        'Chinese Movies': 'International',
        'Chinese': 'International',
        'Japanese Movies': 'International',
        'Japanese': 'International',
        'J-Drama': 'International',
        'French Movies': 'International',
        'French': 'International',
        'Spanish Movies': 'International',
        'Spanish': 'International',
        'Latin American': 'International',
        'Telenovela': 'International',
        'European': 'International',
        'European Movies': 'International',
        'African Movies': 'International',
        'African': 'International',
        'Middle Eastern': 'International',
        'Indian Movies': 'International',
        'Indian': 'International',
        'British': 'International',
        'British Movies': 'International',
        'German': 'International',
        'Italian': 'International',
        'Scandinavian': 'International',
        'Australian': 'International',
        'Thai': 'International',
        'Turkish': 'International',
        
        # ============ MUSICAL ============
        'Music': 'Musical',
        'Musical': 'Musical',
        'Musicals': 'Musical',
        'Music Movies': 'Musical',
        'Concert Film': 'Musical',
        'Concert': 'Musical',
        'Music Video': 'Musical',
        'Opera': 'Musical',
        'Dance': 'Musical',
        'Dance Film': 'Musical',
        'Jukebox Musical': 'Musical',
        
        # ============ INDIE (Separate) ============
        'Independent': 'Indie',
        'Indie': 'Indie',
        'Independent Movies': 'Indie',
        'Indie Movies': 'Indie',
        'Indie Film': 'Indie',
        'Arthouse': 'Indie',
        'Art House': 'Indie',
        'Art Film': 'Indie',
        'Experimental': 'Indie',
        'Avant-Garde': 'Indie',
        'Low Budget': 'Indie',
        'Microbudget': 'Indie',
        
        # ============ CLASSIC (Separate) ============
        'Classic': 'Classic',
        'Classic Movies': 'Classic',
        'Classics': 'Classic',
        'Golden Age': 'Classic',
        'Vintage': 'Classic',
        'Old Hollywood': 'Classic',
        'Cult Movies': 'Classic',
        'Cult Classics': 'Classic',
        'Cult Classic': 'Classic',
        'Cult': 'Classic',
        'B-Movie': 'Classic',
        'B-Movies': 'Classic',
        'B Movie': 'Classic',
        'Exploitation': 'Classic',
        'Grindhouse': 'Classic',
        'Midnight Movie': 'Classic',
        'Camp': 'Classic',
        'Campy': 'Classic',
        'Retro': 'Classic',
        
        # ============ SPORTS ============
        'Sports': 'Sports',
        'Sports Movies': 'Sports',
        'Sports Film': 'Sports',
        'Sports Drama': 'Sports',
        'Boxing': 'Sports',
        'Football': 'Sports',
        'Baseball': 'Sports',
        'Basketball': 'Sports',
        'Racing': 'Sports',
        'Soccer': 'Sports',
        'Wrestling': 'Sports',
        'MMA': 'Sports',
        'Esports': 'Sports',
        'Olympics': 'Sports',
        'Gaming': 'Sports',
        
        # ============ SHORT ============
        'Short Film': 'Short',
        'Short Films': 'Short',
        'Shorts': 'Short',
        'Short': 'Short',
        
        # ============ ADULT ============
        'Adult': 'Adult',
        'NC-17': 'Adult',
        'X-Rated': 'Adult',
        'Erotic': 'Adult',
        'Erotica': 'Adult',
        'Softcore': 'Adult',
        'Mature': 'Adult',
        'Erotic Thriller': 'Adult',
    }

    # Adult detection keywords
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

    # Protected genres (never remap to Adult)
    PROTECTED_GENRES = [
        'Animation', 'Family', 'Documentary', 'Sports',
        'Reality', 'TV Series', 'Musical'
    ]
    
    # Known major titles that shouldn't be Unknown
    KNOWN_MAJOR_TITLES = [
        'the shawshank redemption', 'the godfather', 'the dark knight',
        'pulp fiction', 'forrest gump', 'inception', 'the matrix',
        'goodfellas', 'se7en', 'fight club', 'the silence of the lambs',
        'interstellar', 'the green mile', 'the departed', 'gladiator',
        'the prestige', 'memento', 'apocalypse now', 'alien', 'aliens',
        'the thing', 'blade runner', 'terminator', 'die hard', 'jaws',
        'jurassic park', 'star wars', 'indiana jones', 'back to the future',
        'the lord of the rings', 'harry potter', 'pirates of the caribbean',
        'avatar', 'titanic', 'the avengers', 'iron man', 'spider-man',
        'batman', 'superman', 'x-men', 'transformers', 'fast & furious',
        'mission impossible', 'james bond', 'john wick', 'the bourne',
        'breaking bad', 'game of thrones', 'stranger things', 'the office',
        'friends', 'the simpsons', 'south park', 'rick and morty',
        'the walking dead', 'lost', 'prison break', 'house of cards',
        'narcos', 'peaky blinders', 'vikings', 'the crown', 'ozark',
        'better call saul', 'westworld', 'black mirror', 'the mandalorian',
        'the witcher', 'squid game', 'money heist', 'wednesday',
        'schindler', 'saving private ryan', 'braveheart', 'the pianist',
        'a beautiful mind', 'the wolf of wall street', 'catch me if you can',
        'django', 'inglourious basterds', 'kill bill', 'reservoir dogs',
        'the big lebowski', 'no country for old men', 'fargo', 'sicario',
        'arrival', 'dune', 'mad max', 'gravity', 'the martian', 'dunkirk',
        'oppenheimer', 'barbie', 'everything everywhere all at once'
    ]

    def clean_genre_logic(g, title=''):
        """Map verbose genres to consolidated categories with smart fallback."""
        if not g or pd.isna(g): 
            title_lower = str(title).lower().strip()
            for known_title in KNOWN_MAJOR_TITLES:
                if known_title in title_lower:
                    return "Drama"
            return "Unknown"
        
        genre_str = str(g).strip()
        genre_clean = genre_str.replace(' Movies', '').replace(' Films', '').replace(' Film', '').strip()
        
        # Try exact match
        if genre_str in GENRE_MAP:
            return GENRE_MAP[genre_str]
        
        if genre_clean in GENRE_MAP:
            return GENRE_MAP[genre_clean]
        
        # Try first genre if comma-separated
        first_g = genre_str.split(',')[0].strip()
        if first_g in GENRE_MAP:
            return GENRE_MAP[first_g]
        
        first_g_clean = first_g.replace(' Movies', '').replace(' Films', '').replace(' Film', '').strip()
        if first_g_clean in GENRE_MAP:
            return GENRE_MAP[first_g_clean]
        
        # Partial match
        genre_lower = genre_str.lower()
        for key, value in GENRE_MAP.items():
            key_lower = key.lower()
            if key_lower in genre_lower or genre_lower in key_lower:
                return value
        
        # Keyword fallback
        genre_lower = genre_clean.lower()
        
        if any(word in genre_lower for word in ['comedy', 'funny', 'humor', 'laugh']):
            return 'Comedy'
        if any(word in genre_lower for word in ['drama', 'dramatic']):
            return 'Drama'
        if any(word in genre_lower for word in ['horror', 'scary', 'terror', 'fear']):
            return 'Horror'
        if any(word in genre_lower for word in ['action', 'adventure', 'stunt']):
            return 'Action'
        if any(word in genre_lower for word in ['thriller', 'suspense', 'tense']):
            return 'Thriller'
        if any(word in genre_lower for word in ['mystery', 'detective', 'whodunit']):
            return 'Mystery'
        if any(word in genre_lower for word in ['crime', 'criminal', 'gangster', 'mafia']):
            return 'Crime'
        if any(word in genre_lower for word in ['war', 'military', 'combat', 'battle']):
            return 'War'
        if any(word in genre_lower for word in ['western', 'cowboy', 'frontier']):
            return 'Western'
        if any(word in genre_lower for word in ['supernatural', 'paranormal', 'ghost', 'haunted']):
            return 'Supernatural'
        if any(word in genre_lower for word in ['romance', 'romantic', 'love']):
            return 'Romance'
        if any(word in genre_lower for word in ['sci-fi', 'scifi', 'science', 'space', 'future']):
            return 'Sci-Fi'
        if any(word in genre_lower for word in ['fantasy', 'magic', 'mythic', 'dragon']):
            return 'Fantasy'
        if any(word in genre_lower for word in ['document', 'real', 'true']):
            return 'Documentary'
        if any(word in genre_lower for word in ['anim', 'cartoon', 'anime']):
            return 'Animation'
        if any(word in genre_lower for word in ['music', 'musical', 'song', 'band', 'concert']):
            return 'Musical'
        if any(word in genre_lower for word in ['sport', 'athletic', 'boxing', 'football', 'racing']):
            return 'Sports'
        if any(word in genre_lower for word in ['family', 'kid', 'child']):
            return 'Family'
        if any(word in genre_lower for word in ['indie', 'independent', 'arthouse']):
            return 'Indie'
        if any(word in genre_lower for word in ['classic', 'vintage', 'cult', 'retro']):
            return 'Classic'
        
        # Check known titles
        title_lower = str(title).lower().strip()
        for known_title in KNOWN_MAJOR_TITLES:
            if known_title in title_lower:
                return "Drama"
        
        # Return cleaned first genre if it looks valid
        if first_g_clean and len(first_g_clean) > 2 and first_g_clean.lower() not in ['nan', 'none', 'null', 'n/a', '']:
            if first_g_clean[0].isupper() and not any(char.isdigit() for char in first_g_clean):
                return first_g_clean
        
        return "Drama"

    def filter_adult(row):
        """Safely categorize adult content."""
        title = str(row.get('Project', '')).lower()
        current_genre = row.get('Genre', 'Other')
        
        if current_genre in PROTECTED_GENRES:
            return current_genre
        
        if any(word in title for word in ADULT_KEYWORDS):
            return "Adult"
        
        return current_genre

    def safe_read_csv(filepath):
        """Try multiple methods to read CSV files."""
        filename = os.path.basename(filepath)
        
        try:
            return pd.read_csv(filepath, low_memory=False)
        except:
            pass
        
        try:
            return pd.read_csv(filepath, low_memory=False, on_bad_lines='skip')
        except:
            pass
        
        try:
            return pd.read_csv(filepath, low_memory=False, engine='python', on_bad_lines='skip')
        except:
            pass
        
        try:
            chunks = []
            for chunk in pd.read_csv(filepath, chunksize=50000, low_memory=False, on_bad_lines='skip'):
                chunks.append(chunk)
                if len(chunks) >= 20:
                    break
            if chunks:
                return pd.concat(chunks, ignore_index=True)
        except Exception as e:
            print(f"❌ All read methods failed for {filename}: {e} - prep_data.py:785")
        
        return None

    # --- PROCESS FILES ---
    all_csvs = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    raw_csvs = [f for f in all_csvs if os.path.basename(f) not in PRESERVE_FILES]
    
    print(f"📂 Found {len(raw_csvs)} raw CSV files to process. - prep_data.py:793")
    
    for csv_file in raw_csvs:
        filename = os.path.basename(csv_file).lower()
        
        temp_df = safe_read_csv(csv_file)
        
        if temp_df is None:
            print(f"⏭️ Skipping {filename} (could not read) - prep_data.py:801")
            continue
            
        try:
            # Route to appropriate output
            if 'cast' in filename or ('character' in temp_df.columns if hasattr(temp_df, 'columns') else False):
                if 'name' in temp_df.columns or 'cast' in temp_df.columns:
                    cast_frames.append(temp_df)
                    print(f"🎭 Routed to CAST: {filename} - prep_data.py:809")
                    continue
            
            if 'crew' in filename or 'director' in filename:
                if 'job' in temp_df.columns or 'department' in temp_df.columns:
                    crew_frames.append(temp_df)
                    print(f"🎬 Routed to CREW: {filename} - prep_data.py:815")
                    continue
            
            if 'review' in filename:
                review_frames.append(temp_df)
                print(f"📝 Routed to REVIEWS: {filename} - prep_data.py:820")
                continue
            
            if 'genre' in filename and len(temp_df) < 100:
                genre_frames.append(temp_df)
                print(f"🏷️ Routed to GENRES: {filename} - prep_data.py:825")
                continue
            
            # Main movies data
            name_cols = ['title', 'Series_Title', 'movie_title', 'original_title', 'track_name', 'name']
            for c in name_cols:
                if c in temp_df.columns:
                    temp_df = temp_df.rename(columns={c: 'Project'})
                    break
            
            genre_cols = ['genres', 'listed_in', 'genre', 'Genre']
            for c in genre_cols:
                if c in temp_df.columns and c != 'Genre':
                    temp_df = temp_df.rename(columns={c: 'Genre'})
                    break

            talent_cols = ['actors', 'stars', 'cast', 'Star1']
            if 'Lead_Talent' not in temp_df.columns:
                for c in talent_cols:
                    if c in temp_df.columns:
                        temp_df = temp_df.rename(columns={c: 'Lead_Talent'})
                        break

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
                if "video" in filename or "youtube" in filename:
                    temp_df['Genre'] = 'Reality'
                
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
                print(f"🎬 Routed to MOVIES: {filename} ({len(temp_df)} rows) - prep_data.py:876")
                
        except Exception as e:
            print(f"⚠️ Could not process {filename}: {e} - prep_data.py:879")

    # --- PROCESS MOVIES ---
    if all_frames:
        print(f"\n📊 Merging {len(all_frames)} movie datasets... - prep_data.py:883")
        final_df = pd.concat(all_frames, ignore_index=True, sort=False)
        
        final_df = final_df.loc[:, ~final_df.columns.duplicated(keep='first')]
        print(f"📋 Removed duplicate columns. Remaining: {len(final_df.columns)} columns - prep_data.py:887")
        
        final_df['Sentiment_Score'] = pd.to_numeric(final_df['Sentiment_Score'], errors='coerce').fillna(0.5)
        final_df['Popularity_Score'] = pd.to_numeric(final_df['Popularity_Score'], errors='coerce').fillna(50)

        if 'Lead_Talent' in final_df.columns:
            final_df['Lead_Talent'] = final_df['Lead_Talent'].astype(str).apply(
                lambda x: x.split(',')[0].replace('[', '').replace("'", "").replace('"', '').replace('{', '').strip()[:100]
            )
        else:
            final_df['Lead_Talent'] = 'Ensemble'
        
        print(f"🏷️ Cleaning and categorizing genres... - prep_data.py:899")
        final_df['Genre'] = final_df.apply(
            lambda row: clean_genre_logic(row.get('Genre', ''), row.get('Project', '')), 
            axis=1
        )
        final_df['Genre'] = final_df.apply(filter_adult, axis=1)
        
        if final_df['Sentiment_Score'].max() > 1.1:
            final_df['Sentiment_Score'] = final_df['Sentiment_Score'] / 10.0

        # Duplicate detection
        print(f"🔍 Checking for duplicates... - prep_data.py:910")
        final_df['_normalized_title'] = final_df['Project'].apply(normalize_title)
        final_df = final_df.sort_values('Popularity_Score', ascending=False)
        
        exact_dupes = final_df.duplicated(subset=['Project'], keep=False).sum()
        normalized_dupes = final_df.duplicated(subset=['_normalized_title'], keep=False).sum()
        
        print(f"Exact title duplicates found: {exact_dupes} - prep_data.py:917")
        print(f"Normalized title duplicates found: {normalized_dupes} - prep_data.py:918")
        
        before_count = len(final_df)
        final_df = final_df.drop_duplicates(subset=['Project'], keep='first')
        removed_exact = before_count - len(final_df)
        print(f"Removed {removed_exact} exact duplicates - prep_data.py:923")
        
        before_count = len(final_df)
        final_df = final_df.drop_duplicates(subset=['_normalized_title'], keep='first')
        removed_normalized = before_count - len(final_df)
        print(f"Removed {removed_normalized} normalized duplicates - prep_data.py:928")
        
        final_df = final_df.drop(columns=['_normalized_title'])
        print(f"After deduplication: {len(final_df)} unique projects - prep_data.py:931")

        final_df = final_df.head(10000)

        # Reorder columns
        def count_non_empty(col):
            non_null = final_df[col].notna().sum()
            non_empty = (final_df[col].astype(str).str.strip() != '').sum()
            return min(non_null, non_empty)

        sorted_columns = sorted(final_df.columns, key=count_non_empty, reverse=True)
        final_df = final_df[sorted_columns]
        print(f"📋 Columns reordered by data density. - prep_data.py:943")
        
        if final_df.columns.duplicated().any():
            final_df = final_df.loc[:, ~final_df.columns.duplicated(keep='first')]

        # Report genre distribution
        genre_counts = final_df['Genre'].value_counts()
        print(f"\n📊 Genre Distribution: - prep_data.py:950")
        for genre, count in genre_counts.head(25).items():
            print(f"{genre}: {count} - prep_data.py:952")
        
        unknown_count = genre_counts.get('Unknown', 0) + genre_counts.get('Other', 0) + genre_counts.get('Misc', 0)
        if unknown_count > 0:
            print(f"⚠️ (Unknown/Other/Misc combined: {unknown_count}) - prep_data.py:956")

        final_df.to_csv(TARGET_MOVIES, index=False)
        print(f"\n✅ MOVIES: {len(final_df)} unique projects saved to {TARGET_MOVIES} - prep_data.py:959")
        print(f"Columns: {list(final_df.columns[:10])}... ({len(final_df.columns)} total) - prep_data.py:960")
    else:
        print("⚠️ No movie data found to merge. - prep_data.py:962")

    # Process other data frames
    if cast_frames:
        cast_df = pd.concat(cast_frames, ignore_index=True, sort=False)
        cast_df = cast_df.loc[:, ~cast_df.columns.duplicated(keep='first')]
        cast_df = cast_df.drop_duplicates()
        
        def count_non_empty_cast(col):
            return min(cast_df[col].notna().sum(), (cast_df[col].astype(str).str.strip() != '').sum())
        
        sorted_cols = sorted(cast_df.columns, key=count_non_empty_cast, reverse=True)
        cast_df = cast_df[sorted_cols]
        cast_df.to_csv(TARGET_CAST, index=False)
        print(f"✅ CAST: {len(cast_df)} records saved to {TARGET_CAST} - prep_data.py:976")

    if crew_frames:
        crew_df = pd.concat(crew_frames, ignore_index=True, sort=False)
        crew_df = crew_df.loc[:, ~crew_df.columns.duplicated(keep='first')]
        crew_df = crew_df.drop_duplicates()
        
        def count_non_empty_crew(col):
            return min(crew_df[col].notna().sum(), (crew_df[col].astype(str).str.strip() != '').sum())
        
        sorted_cols = sorted(crew_df.columns, key=count_non_empty_crew, reverse=True)
        crew_df = crew_df[sorted_cols]
        crew_df.to_csv(TARGET_CREW, index=False)
        print(f"✅ CREW: {len(crew_df)} records saved to {TARGET_CREW} - prep_data.py:989")

    if genre_frames:
        genre_df = pd.concat(genre_frames, ignore_index=True, sort=False)
        genre_df = genre_df.loc[:, ~genre_df.columns.duplicated(keep='first')]
        genre_df = genre_df.drop_duplicates()
        genre_df.to_csv(TARGET_GENRES, index=False)
        print(f"✅ GENRES: {len(genre_df)} records saved to {TARGET_GENRES} - prep_data.py:996")

    if review_frames:
        review_df = pd.concat(review_frames, ignore_index=True, sort=False)
        review_df = review_df.loc[:, ~review_df.columns.duplicated(keep='first')]
        review_df = review_df.drop_duplicates()
        
        def count_non_empty_review(col):
            return min(review_df[col].notna().sum(), (review_df[col].astype(str).str.strip() != '').sum())
        
        sorted_cols = sorted(review_df.columns, key=count_non_empty_review, reverse=True)
        review_df = review_df[sorted_cols]
        review_df.to_csv(TARGET_REVIEWS, index=False)
        print(f"✅ REVIEWS: {len(review_df)} records saved to {TARGET_REVIEWS} - prep_data.py:1009")

    print(f"\n🚀 MISSION COMPLETE - prep_data.py:1011")

if __name__ == "__main__":
    refresh_flag = "--refresh" in sys.argv
    setup_env()
    fetch_data(force_refresh=refresh_flag)
    process_and_merge()
    cleanup_raw_data()