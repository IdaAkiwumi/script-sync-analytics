"""
PREP_DATA.PY - Genre Sync Analytics Data Pipeline
VERSION: 1.0.0
AUTHOR: Ida Akiwumi
LAST UPDATED: March 2026

DESCRIPTION:
Fetches, cleans, normalizes, and consolidates movie metadata from multiple
Kaggle datasets into a unified intelligence database for Genre Sync Analytics.

OUTPUT:
- movie_database_movies_2026.csv (Top 10,000 movies)
- movie_database_cast_2026.csv
- movie_database_crew_2026.csv
- movie_database_genres_2026.csv
- movie_database_reviews_2026.csv

USAGE:
    python prep_data.py           # Normal run (skips if data is fresh)
    python prep_data.py --refresh # Force re-download from Kaggle
"""

import pandas as pd
import numpy as np
import os
import kaggle
import glob
import time
import sys
import re
import ast
import hashlib
from datetime import datetime

# --- CONFIGURATION ---
DATA_DIR = "data"
TARGET_MOVIE_COUNT = 10000  # Final output size

DATASETS = [
    "sankha1998/tmdb-top-10000-popular-movies-dataset",
    "tiagoadrianunes/imdb-top-5000-movies",
    "shivamb/netflix-shows",
    "abdallahwagih/movies",
    "tmdb/tmdb-movie-metadata",
    "rishabhkumar2003/the-movie-database-tmdb-comprehensive-dataset",
    "ggtejas/tmdb-imdb-merged-movies-dataset"
]

# --- TARGET OUTPUT FILES ---
TARGET_MOVIES = os.path.join(DATA_DIR, "movie_database_movies_2026.csv")
TARGET_CAST = os.path.join(DATA_DIR, "movie_database_cast_2026.csv")
TARGET_CREW = os.path.join(DATA_DIR, "movie_database_crew_2026.csv")
TARGET_GENRES = os.path.join(DATA_DIR, "movie_database_genres_2026.csv")
TARGET_REVIEWS = os.path.join(DATA_DIR, "movie_database_reviews_2026.csv")

PRESERVE_FILES = [
    "movie_database_movies_2026.csv",
    "movie_database_cast_2026.csv",
    "movie_database_crew_2026.csv",
    "movie_database_genres_2026.csv",
    "movie_database_reviews_2026.csv"
]

# --- CORE COLUMN ORDER (must match streamlit_app.py expectations) ---
CORE_COLUMNS = [
    'Project', 'Genre', 'Sentiment_Score',
    'Popularity_Score', 'Lead_Talent', 'id'
]


def log(message, level="INFO"):
    """Consistent logging with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    icons = {
        "INFO": "ℹ️",
        "SUCCESS": "✅",
        "WARNING": "⚠️",
        "ERROR": "❌",
        "PROCESS": "🔄",
        "DATA": "📊",
        "FILE": "📁"
    }
    icon = icons.get(level, "•")
    print(f"[{timestamp}] {icon} {message} - prep_data.py:84")


def setup_env():
    """Initialize environment and authenticate Kaggle."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        log(f"Created data directory: {DATA_DIR}", "SUCCESS")

    try:
        kaggle.api.authenticate()
        log("Kaggle API authenticated successfully", "SUCCESS")
        return True
    except Exception as e:
        log(f"Kaggle authentication failed: {e}", "ERROR")
        log("Ensure kaggle.json is in ~/.kaggle/ or %USERPROFILE%\\.kaggle\\", "WARNING")
        return False


def fetch_data(force_refresh=False):
    """Fetch datasets from Kaggle if needed."""
    seven_days_seconds = 7 * 24 * 60 * 60
    current_time = time.time()

    if not force_refresh and os.path.exists(TARGET_MOVIES):
        file_age = current_time - os.path.getmtime(TARGET_MOVIES)
        if file_age < seven_days_seconds:
            days_old = file_age / (24 * 60 * 60)
            log(f"Data is fresh ({days_old:.1f} days old). Skipping Kaggle sync.", "INFO")
            log("Use --refresh flag to force re-download", "INFO")
            return

    if force_refresh:
        log("Force refresh enabled - downloading all datasets", "PROCESS")
    else:
        log("Data is stale or missing - downloading from Kaggle", "PROCESS")

    successful = 0
    failed = 0

    for slug in DATASETS:
        try:
            log(f"Downloading: {slug}", "PROCESS")
            kaggle.api.dataset_download_files(slug, path=DATA_DIR, unzip=True)
            successful += 1
            log(f"Downloaded: {slug}", "SUCCESS")
        except Exception as e:
            failed += 1
            log(f"Failed to download {slug}: {e}", "WARNING")

    log(f"Download complete: {successful} succeeded, {failed} failed", "DATA")


def cleanup_raw_data():
    """Remove raw Kaggle files, preserving only final outputs."""
    log("Cleaning up raw data files...", "PROCESS")

    all_files = glob.glob(os.path.join(DATA_DIR, "*"))
    deleted = 0
    preserved = 0

    for f in all_files:
        filename = os.path.basename(f)

        if filename in PRESERVE_FILES:
            preserved += 1
            continue

        try:
            if os.path.isfile(f):
                os.remove(f)
                deleted += 1
        except Exception as e:
            log(f"Could not delete {filename}: {e}", "WARNING")

    log(f"Cleanup complete: {deleted} files deleted, {preserved} preserved", "SUCCESS")


GENRE_MAP = {
    'Comedy': 'Comedy', 'Comedies': 'Comedy', 'Comedy Movies': 'Comedy',
    'Stand-Up Comedy': 'Comedy', 'Stand-Up Comedy & Talk Shows': 'Comedy',
    'Stand Up': 'Comedy', 'Standup': 'Comedy', 'Talk Show': 'Comedy',
    'Talk Shows': 'Comedy', 'Late Night': 'Comedy', 'Sitcom': 'Comedy',
    'Sketch Comedy': 'Comedy', 'Parody': 'Comedy', 'Satire': 'Comedy',
    'Dark Comedy': 'Comedy', 'Black Comedy': 'Comedy', 'Romantic Comedies': 'Comedy',
    'Romantic Comedy': 'Comedy', 'Rom-Com': 'Comedy', 'Comedy-Drama': 'Comedy',
    'Dramedy': 'Comedy', 'Slapstick': 'Comedy', 'Screwball Comedy': 'Comedy',
    'Mockumentary': 'Comedy',

    'Drama': 'Drama', 'Dramas': 'Drama', 'Drama Movies': 'Drama',
    'TV Dramas': 'Drama', 'Legal Drama': 'Drama', 'Medical Drama': 'Drama',
    'Political Drama': 'Drama', 'Period Drama': 'Drama', 'Costume Drama': 'Drama',
    'Historical Drama': 'Drama', 'Historical': 'Drama', 'History': 'Drama',
    'Biographical Drama': 'Drama', 'Biography': 'Drama', 'Biographical': 'Drama',
    'Biopic': 'Drama', 'Bio-Pic': 'Drama', 'Social Issue Drama': 'Drama',
    'Teen Drama': 'Drama', 'Coming of Age': 'Drama', 'Soap Opera': 'Drama',
    'Melodrama': 'Drama', 'Family Drama': 'Drama', 'Psychological Drama': 'Drama',
    'Courtroom': 'Drama', 'Legal': 'Drama', 'Medical': 'Drama',
    'Political': 'Drama', 'Period Piece': 'Drama', 'Epic': 'Drama',
    'Tragedy': 'Drama', 'Faith & Spirituality': 'Drama', 'Faith': 'Drama',
    'Christian': 'Drama', 'Christian Movies': 'Drama', 'Religious': 'Drama',
    'Inspirational': 'Drama', 'Spiritual': 'Drama', 'LGBTQ Movies': 'Drama',
    'LGBTQ+': 'Drama', 'LGBTQ': 'Drama', 'LGBT': 'Drama', 'Queer Cinema': 'Drama',
    'Gay': 'Drama', 'Lesbian': 'Drama',

    'Action': 'Action', 'Action & Adventure': 'Action', 'TV Action & Adventure': 'Action',
    'Action-Adventure': 'Action', 'Action Movies': 'Action', 'Action Film': 'Action',
    'Adventure': 'Action', 'Adventure Movies': 'Action', 'Adventures': 'Action',
    'Martial Arts': 'Action', 'Martial Arts Film': 'Action', 'Kung Fu': 'Action',
    'Superhero': 'Action', 'Superhero Movies': 'Action', 'Comic Book': 'Action',
    'Disaster': 'Action', 'Disaster Film': 'Action', 'Spy': 'Action',
    'Spy Movies': 'Action', 'Spy Film': 'Action', 'Espionage': 'Action',
    'Secret Agent': 'Action', 'Swashbuckler': 'Action', 'Samurai': 'Action',
    'Stunt': 'Action',

    'War': 'War', 'War Movies': 'War', 'War Film': 'War', 'Military': 'War',
    'Anti-War': 'War', 'Combat': 'War', 'World War': 'War', 'Vietnam': 'War',
    'WWII': 'War', 'WWI': 'War',

    'Western': 'Western', 'Westerns': 'Western', 'Western Movies': 'Western',
    'Spaghetti Western': 'Western', 'Neo-Western': 'Western', 'Cowboy': 'Western',
    'Frontier': 'Western',

    'Crime': 'Crime', 'Crime Movies': 'Crime', 'Crime Films': 'Crime',
    'TV Crime': 'Crime', 'Gangster': 'Crime', 'Gangster Film': 'Crime',
    'Mob': 'Crime', 'Mafia': 'Crime', 'Prison': 'Crime', 'Prison Film': 'Crime',
    'Heist': 'Crime', 'Heist Film': 'Crime', 'Caper': 'Crime',
    'True Crime': 'Crime', 'Organized Crime': 'Crime', 'Drug': 'Crime',
    'Cartel': 'Crime',

    'Mystery': 'Mystery', 'Mysteries': 'Mystery', 'Mystery Movies': 'Mystery',
    'Whodunit': 'Mystery', 'Detective': 'Mystery', 'Cozy Mystery': 'Mystery',
    'Murder Mystery': 'Mystery', 'Amateur Sleuth': 'Mystery',

    'Thriller': 'Thriller', 'Thrillers': 'Thriller', 'Thriller Movies': 'Thriller',
    'TV Thrillers': 'Thriller', 'Psychological Thriller': 'Thriller',
    'Psychological': 'Thriller', 'Crime Thriller': 'Thriller', 'Spy Thriller': 'Thriller',
    'Political Thriller': 'Thriller', 'Legal Thriller': 'Thriller',
    'Techno-Thriller': 'Thriller', 'Conspiracy': 'Thriller', 'Suspense': 'Thriller',
    'Suspenseful': 'Thriller', 'Neo-Noir': 'Thriller', 'Film Noir': 'Thriller',
    'Noir': 'Thriller',

    'Horror': 'Horror', 'Horror Movies': 'Horror', 'Horrors': 'Horror',
    'TV Horror': 'Horror', 'Slasher': 'Horror', 'Slasher Film': 'Horror',
    'Psychological Horror': 'Horror', 'Body Horror': 'Horror', 'Found Footage': 'Horror',
    'Zombie': 'Horror', 'Zombies': 'Horror', 'Zombie Film': 'Horror',
    'Monster Movies': 'Horror', 'Monster': 'Horror', 'Creature Feature': 'Horror',
    'Gothic Horror': 'Horror', 'Gothic': 'Horror', 'Splatter': 'Horror',
    'Survival Horror': 'Horror', 'Vampire': 'Horror', 'Werewolf': 'Horror',
    'Demonic': 'Horror', 'Occult': 'Horror', 'Halloween': 'Horror',

    'Supernatural': 'Supernatural', 'Supernatural Horror': 'Supernatural',
    'Paranormal': 'Supernatural', 'Ghost': 'Supernatural', 'Ghosts': 'Supernatural',
    'Haunted': 'Supernatural', 'Haunting': 'Supernatural', 'Poltergeist': 'Supernatural',
    'Possession': 'Supernatural', 'Exorcism': 'Supernatural', 'Afterlife': 'Supernatural',
    'Angels': 'Supernatural', 'Demons': 'Supernatural',

    'Science Fiction': 'Sci-Fi', 'Sci-Fi': 'Sci-Fi', 'SciFi': 'Sci-Fi', 'SF': 'Sci-Fi',
    'Sci-Fi & Fantasy': 'Sci-Fi', 'TV Sci-Fi & Fantasy': 'Sci-Fi',
    'Science Fiction Movies': 'Sci-Fi', 'Sci-Fi Movies': 'Sci-Fi',
    'Space Opera': 'Sci-Fi', 'Space': 'Sci-Fi', 'Cyberpunk': 'Sci-Fi',
    'Dystopian': 'Sci-Fi', 'Dystopia': 'Sci-Fi', 'Post-Apocalyptic': 'Sci-Fi',
    'Apocalyptic': 'Sci-Fi', 'Time Travel': 'Sci-Fi', 'Alien': 'Sci-Fi',
    'Aliens': 'Sci-Fi', 'Robot & AI': 'Sci-Fi', 'Robots': 'Sci-Fi',
    'Futuristic': 'Sci-Fi', 'Tech': 'Sci-Fi', 'Virtual Reality': 'Sci-Fi',
    'Steampunk': 'Sci-Fi',

    'Fantasy': 'Fantasy', 'Fantasy Movies': 'Fantasy', 'Dark Fantasy': 'Fantasy',
    'Urban Fantasy': 'Fantasy', 'High Fantasy': 'Fantasy', 'Epic Fantasy': 'Fantasy',
    'Sword & Sorcery': 'Fantasy', 'Sword and Sorcery': 'Fantasy', 'Fairy Tale': 'Fantasy',
    'Fairy Tales': 'Fantasy', 'Mythology': 'Fantasy', 'Myth': 'Fantasy',
    'Magic': 'Fantasy', 'Magical Realism': 'Fantasy', 'Wizards': 'Fantasy',
    'Dragons': 'Fantasy', 'Medieval': 'Fantasy',

    'Romance': 'Romance', 'Romantic Movies': 'Romance', 'Romantic TV Shows': 'Romance',
    'Romance Movies': 'Romance', 'Romance Film': 'Romance', 'Romantic Dramas': 'Romance',
    'Love Story': 'Romance', 'Chick Flick': 'Romance', 'Love': 'Romance',
    'Wedding': 'Romance',

    'Animation': 'Animation', 'Anime Features': 'Animation', 'Anime Series': 'Animation',
    'Anime': 'Animation', 'Animated': 'Animation', 'Animated Movies': 'Animation',
    'Cartoon': 'Animation', 'Cartoons': 'Animation', 'CGI': 'Animation',
    'Computer Animation': 'Animation', 'Stop Motion': 'Animation', 'Claymation': 'Animation',
    'Adult Animation': 'Animation', 'Pixar': 'Animation', 'Disney': 'Animation',
    'Dreamworks': 'Animation', 'Studio Ghibli': 'Animation', 'Shonen': 'Animation',
    'Shojo': 'Animation', 'Isekai': 'Animation', 'Mecha': 'Animation',

    'Documentary': 'Documentary', 'Documentaries': 'Documentary',
    'Documentary Movies': 'Documentary', 'Docuseries': 'Documentary',
    'Docudrama': 'Documentary', 'Nature Documentary': 'Documentary', 'Nature': 'Documentary',
    'Wildlife': 'Documentary', 'Historical Documentary': 'Documentary',
    'Music Documentary': 'Documentary', 'Sports Documentary': 'Documentary',
    'Political Documentary': 'Documentary', 'Science': 'Documentary',
    'Educational': 'Documentary', 'Instructional': 'Documentary', 'News': 'Documentary',
    'News & Politics': 'Documentary',

    'Family': 'Family', 'Family Movies': 'Family', 'Family Films': 'Family',
    'Kids': 'Family', 'Kids Movies': 'Family', "Children's": 'Family',
    "Children's Movies": 'Family', 'Children': 'Family', 'Teen': 'Family',
    'Teen Movies': 'Family', 'Tween': 'Family', 'Young Adult': 'Family',
    'Preschool': 'Family', 'Kids TV': 'Family', "Children's TV": 'Family',
    'Holiday': 'Family', 'Christmas': 'Family', 'Christmas Movies': 'Family',

    'TV Shows': 'TV Series', 'TV Series': 'TV Series', 'Television': 'TV Series',
    'TV': 'TV Series', 'British TV Shows': 'TV Series', 'Korean TV Shows': 'TV Series',
    'Spanish-Language TV Shows': 'TV Series', 'International TV Shows': 'TV Series',
    'Miniseries': 'TV Series', 'Mini-Series': 'TV Series', 'Limited Series': 'TV Series',
    'Anthology': 'TV Series', 'Web Series': 'TV Series', 'Streaming': 'TV Series',

    'Reality TV': 'Reality', 'Reality': 'Reality', 'Reality Shows': 'Reality',
    'Competition Reality': 'Reality', 'Competition': 'Reality', 'Dating Show': 'Reality',
    'Dating': 'Reality', 'Game Show': 'Reality', 'Game Shows': 'Reality',
    'Talent Show': 'Reality', 'Makeover Show': 'Reality', 'Makeover': 'Reality',
    'Cooking Show': 'Reality', 'Cooking': 'Reality', 'Food': 'Reality',
    'Travel Show': 'Reality', 'Travel': 'Reality', 'Lifestyle': 'Reality',
    'Home Improvement': 'Reality', 'Renovation': 'Reality', 'Entertainment': 'Reality',
    'People & Blogs': 'Reality', 'Vlog': 'Reality', 'Podcast': 'Reality',
    'ASMR': 'Reality', 'Commentary': 'Reality', 'Reaction': 'Reality',

    'International': 'International', 'International Movies': 'International',
    'Foreign': 'International', 'Foreign Film': 'International',
    'Foreign Language': 'International', 'World Cinema': 'International',
    'Bollywood': 'International', 'Korean Movies': 'International', 'Korean': 'International',
    'K-Drama': 'International', 'Chinese Movies': 'International', 'Chinese': 'International',
    'Japanese Movies': 'International', 'Japanese': 'International', 'J-Drama': 'International',
    'French Movies': 'International', 'French': 'International', 'Spanish Movies': 'International',
    'Spanish': 'International', 'Latin American': 'International', 'Telenovela': 'International',
    'European': 'International', 'European Movies': 'International',
    'African Movies': 'International', 'African': 'International', 'Middle Eastern': 'International',
    'Indian Movies': 'International', 'Indian': 'International', 'British': 'International',
    'British Movies': 'International', 'German': 'International', 'Italian': 'International',
    'Scandinavian': 'International', 'Australian': 'International', 'Thai': 'International',
    'Turkish': 'International',

    'Music': 'Musical', 'Musical': 'Musical', 'Musicals': 'Musical',
    'Music Movies': 'Musical', 'Concert Film': 'Musical', 'Concert': 'Musical',
    'Music Video': 'Musical', 'Opera': 'Musical', 'Dance': 'Musical',
    'Dance Film': 'Musical', 'Jukebox Musical': 'Musical',

    'Independent': 'Indie', 'Indie': 'Indie', 'Independent Movies': 'Indie',
    'Indie Movies': 'Indie', 'Indie Film': 'Indie', 'Arthouse': 'Indie',
    'Art House': 'Indie', 'Art Film': 'Indie', 'Experimental': 'Indie',
    'Avant-Garde': 'Indie', 'Low Budget': 'Indie', 'Microbudget': 'Indie',

    'Classic': 'Classic', 'Classic Movies': 'Classic', 'Classics': 'Classic',
    'Golden Age': 'Classic', 'Vintage': 'Classic', 'Old Hollywood': 'Classic',
    'Cult Movies': 'Classic', 'Cult Classics': 'Classic', 'Cult Classic': 'Classic',
    'Cult': 'Classic', 'B-Movie': 'Classic', 'B-Movies': 'Classic',
    'B Movie': 'Classic', 'Exploitation': 'Classic', 'Grindhouse': 'Classic',
    'Midnight Movie': 'Classic', 'Camp': 'Classic', 'Campy': 'Classic',
    'Retro': 'Classic',

    'Sports': 'Sports', 'Sports Movies': 'Sports', 'Sports Film': 'Sports',
    'Sports Drama': 'Sports', 'Boxing': 'Sports', 'Football': 'Sports',
    'Baseball': 'Sports', 'Basketball': 'Sports', 'Racing': 'Sports',
    'Soccer': 'Sports', 'Wrestling': 'Sports', 'MMA': 'Sports',
    'Esports': 'Sports', 'Olympics': 'Sports', 'Gaming': 'Sports',

    'Short Film': 'Short', 'Short Films': 'Short', 'Shorts': 'Short', 'Short': 'Short',

    'Adult': 'Adult', 'NC-17': 'Adult', 'X-Rated': 'Adult', 'Erotic': 'Adult',
    'Erotica': 'Adult', 'Softcore': 'Adult', 'Mature': 'Adult',
    'Erotic Thriller': 'Adult',
}

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

PROTECTED_GENRES = [
    'Animation', 'Family', 'Documentary', 'Sports',
    'Reality', 'TV Series', 'Musical'
]

KNOWN_TITLES_WITH_GENRES = {
    'the shawshank redemption': 'Drama',
    'the godfather': 'Crime',
    'the dark knight': 'Action',
    'pulp fiction': 'Crime',
    'forrest gump': 'Drama',
    'inception': 'Sci-Fi',
    'the matrix': 'Sci-Fi',
    'goodfellas': 'Crime',
    'se7en': 'Thriller',
    'seven': 'Thriller',
    'fight club': 'Drama',
    'the silence of the lambs': 'Thriller',
    'interstellar': 'Sci-Fi',
    'the green mile': 'Drama',
    'the departed': 'Crime',
    'gladiator': 'Action',
    'the prestige': 'Thriller',
    'memento': 'Thriller',
    'apocalypse now': 'War',
    'alien': 'Sci-Fi',
    'aliens': 'Sci-Fi',
    'the thing': 'Horror',
    'blade runner': 'Sci-Fi',
    'terminator': 'Sci-Fi',
    'the terminator': 'Sci-Fi',
    'die hard': 'Action',
    'jaws': 'Thriller',
    'jurassic park': 'Sci-Fi',
    'star wars': 'Sci-Fi',
    'indiana jones': 'Action',
    'back to the future': 'Sci-Fi',
    'the lord of the rings': 'Fantasy',
    'harry potter': 'Fantasy',
    'pirates of the caribbean': 'Action',
    'avatar': 'Sci-Fi',
    'titanic': 'Romance',
    'the avengers': 'Action',
    'iron man': 'Action',
    'spider-man': 'Action',
    'spiderman': 'Action',
    'batman': 'Action',
    'superman': 'Action',
    'x-men': 'Action',
    'transformers': 'Action',
    'fast & furious': 'Action',
    'fast and furious': 'Action',
    'mission impossible': 'Action',
    'mission: impossible': 'Action',
    'james bond': 'Action',
    'john wick': 'Action',
    'the bourne': 'Action',
    'breaking bad': 'Drama',
    'game of thrones': 'Fantasy',
    'stranger things': 'Sci-Fi',
    'the office': 'Comedy',
    'friends': 'Comedy',
    'the simpsons': 'Animation',
    'south park': 'Animation',
    'rick and morty': 'Animation',
    'the walking dead': 'Horror',
    'lost': 'Drama',
    'prison break': 'Drama',
    'house of cards': 'Drama',
    'narcos': 'Crime',
    'peaky blinders': 'Crime',
    'vikings': 'Drama',
    'the crown': 'Drama',
    'ozark': 'Crime',
    'better call saul': 'Drama',
    'westworld': 'Sci-Fi',
    'black mirror': 'Sci-Fi',
    'the mandalorian': 'Sci-Fi',
    'the witcher': 'Fantasy',
    'squid game': 'Thriller',
    'money heist': 'Crime',
    'wednesday': 'Comedy',
    "schindler's list": 'Drama',
    'saving private ryan': 'War',
    'braveheart': 'Action',
    'the pianist': 'Drama',
    'a beautiful mind': 'Drama',
    'the wolf of wall street': 'Comedy',
    'catch me if you can': 'Crime',
    'django unchained': 'Western',
    'inglourious basterds': 'War',
    'kill bill': 'Action',
    'reservoir dogs': 'Crime',
    'the big lebowski': 'Comedy',
    'no country for old men': 'Thriller',
    'fargo': 'Crime',
    'sicario': 'Thriller',
    'arrival': 'Sci-Fi',
    'dune': 'Sci-Fi',
    'mad max': 'Action',
    'gravity': 'Sci-Fi',
    'the martian': 'Sci-Fi',
    'dunkirk': 'War',
    'oppenheimer': 'Drama',
    'barbie': 'Comedy',
    'everything everywhere all at once': 'Sci-Fi',
    'the exorcist': 'Horror',
    'get out': 'Horror',
    'hereditary': 'Horror',
    'midsommar': 'Horror',
    'it': 'Horror',
    'the conjuring': 'Horror',
    'a quiet place': 'Horror',
    'us': 'Horror',
    'parasite': 'Thriller',
    'joker': 'Drama',
    'knives out': 'Mystery',
    'once upon a time in hollywood': 'Comedy',
    'la la land': 'Musical',
    'whiplash': 'Drama',
    'moonlight': 'Drama',
    'spotlight': 'Drama',
    'the shape of water': 'Fantasy',
    'coco': 'Animation',
    'soul': 'Animation',
    'inside out': 'Animation',
    'toy story': 'Animation',
    'finding nemo': 'Animation',
    'the lion king': 'Animation',
    'frozen': 'Animation',
    'spirited away': 'Animation',
    'your name': 'Animation',
    'howls moving castle': 'Animation',
    "howl's moving castle": 'Animation',
    'princess mononoke': 'Animation',
    'my neighbor totoro': 'Animation',
    'akira': 'Animation',
    'ghost in the shell': 'Animation',
}


def normalize_title(title):
    """Normalize title for duplicate detection."""
    if not title or pd.isna(title):
        return ""

    t = str(title).lower().strip()
    t = t.replace('(film)', '').replace('(movie)', '')
    t = t.replace('(tv series)', '').replace('(tv)', '')
    t = t.replace('(miniseries)', '').replace('(documentary)', '')

    t = re.sub(r'\s*[\($$]?\d{4}[\)$$]?', '', t)
    t = re.sub(r'\s*\d{4}$', '', t)

    if ', the' in t:
        t = 'the ' + t.replace(', the', '')
    if ', a' in t:
        t = 'a ' + t.replace(', a', '')

    if t.startswith('the '):
        t = t[4:]
    if t.startswith('a '):
        t = t[2:]
    if t.startswith('an '):
        t = t[3:]

    t = re.sub(r'[^\w\s]', '', t)
    t = ' '.join(t.split())

    return t.strip()


def parse_json_genre(genre_str):
    """Parse JSON-formatted genre strings from TMDB datasets."""
    if not genre_str or pd.isna(genre_str):
        return None

    genre_str = str(genre_str).strip()

    if not genre_str.startswith('['):
        return genre_str

    try:
        items = ast.literal_eval(genre_str)
        if items and isinstance(items, list) and len(items) > 0:
            first_item = items[0]
            if isinstance(first_item, dict) and 'name' in first_item:
                return first_item['name']
            elif isinstance(first_item, str):
                return first_item
    except Exception:
        pass

    return genre_str


def matches_known_title(title_lower, known_title):
    """Check if title matches a known title."""
    title_lower = title_lower.strip()
    known_title = known_title.strip()

    if title_lower == known_title:
        return True
    if title_lower.startswith(known_title + ' '):
        return True
    if title_lower.startswith(known_title + ':'):
        return True
    if title_lower.startswith(known_title + ' -'):
        return True

    return False


def clean_genre(genre_value, title=''):
    """
    Map raw genre to consolidated category.
    """
    title_str = str(title).strip()
    title_lower = title_str.lower()

    if pd.isna(genre_value):
        genre_str = ""
    else:
        genre_str = str(genre_value).strip()

    if genre_str.lower() in ['', 'nan', 'none', 'null', 'n/a', 'unknown']:
        genre_str = ""

    # Known-title override first
    for known_title, known_genre in KNOWN_TITLES_WITH_GENRES.items():
        if matches_known_title(title_lower, known_title):
            return known_genre

    if genre_str:
        parsed = parse_json_genre(genre_str)
        if parsed:
            genre_str = str(parsed).strip()

    if genre_str:
        genre_clean = genre_str.replace(' Movies', '').replace(' Films', '').replace(' Film', '').strip()

        if genre_str in GENRE_MAP:
            return GENRE_MAP[genre_str]
        if genre_clean in GENRE_MAP:
            return GENRE_MAP[genre_clean]

        genre_parts = re.split(r'[,\|/;]', genre_str)
        for part in genre_parts:
            part = part.strip()
            part_clean = part.replace(' Movies', '').replace(' Films', '').replace(' Film', '').strip()
            if part in GENRE_MAP:
                return GENRE_MAP[part]
            if part_clean in GENRE_MAP:
                return GENRE_MAP[part_clean]

        genre_lower = genre_str.lower()
        for key, value in GENRE_MAP.items():
            key_lower = key.lower()
            if key_lower in genre_lower or genre_lower in key_lower:
                return value

        keyword_mapping = [
            (['comedy', 'funny', 'humor', 'laugh', 'sitcom'], 'Comedy'),
            (['drama', 'dramatic'], 'Drama'),
            (['horror', 'scary', 'terror', 'fear', 'slasher'], 'Horror'),
            (['action', 'adventure', 'stunt', 'martial'], 'Action'),
            (['thriller', 'suspense', 'tense', 'noir'], 'Thriller'),
            (['mystery', 'detective', 'whodunit'], 'Mystery'),
            (['crime', 'criminal', 'gangster', 'mafia', 'heist'], 'Crime'),
            (['war', 'military', 'combat', 'battle'], 'War'),
            (['western', 'cowboy', 'frontier'], 'Western'),
            (['supernatural', 'paranormal', 'ghost', 'haunted'], 'Supernatural'),
            (['romance', 'romantic', 'love story'], 'Romance'),
            (['sci-fi', 'scifi', 'science fiction', 'space', 'future', 'alien'], 'Sci-Fi'),
            (['fantasy', 'magic', 'mythic', 'dragon', 'wizard'], 'Fantasy'),
            (['document', 'documentary'], 'Documentary'),
            (['anim', 'cartoon', 'anime'], 'Animation'),
            (['music', 'musical', 'concert', 'band'], 'Musical'),
            (['sport', 'athletic', 'boxing', 'football', 'racing', 'basketball'], 'Sports'),
            (['family', 'kid', 'child', 'children'], 'Family'),
            (['indie', 'independent', 'arthouse'], 'Indie'),
            (['classic', 'vintage', 'cult', 'retro'], 'Classic'),
        ]

        genre_lower = genre_clean.lower()
        for keywords, genre in keyword_mapping:
            if any(kw in genre_lower for kw in keywords):
                return genre

    title_keyword_mapping = [
        (['christmas', 'holiday'], 'Family'),
        (['haunting', 'haunted', 'exorcist', 'conjuring', 'paranormal'], 'Horror'),
        (['killer', 'murder', 'crime', 'gangster', 'mafia'], 'Crime'),
        (['detective', 'mystery', 'case files'], 'Mystery'),
        (['space', 'alien', 'future', 'robot', 'galaxy'], 'Sci-Fi'),
        (['dragon', 'wizard', 'magic', 'kingdom'], 'Fantasy'),
        (['love', 'romance', 'wedding'], 'Romance'),
        (['war', 'battle', 'soldier'], 'War'),
        (['anime', 'animated'], 'Animation'),
        (['documentary', 'untold', 'history of'], 'Documentary'),
    ]

    for keywords, genre in title_keyword_mapping:
        if any(kw in title_lower for kw in keywords):
            return genre

    return "Unknown"


def filter_adult_content(row):
    """Safely categorize adult content based on title keywords."""
    title = str(row.get('Project', '')).lower()
    current_genre = row.get('Genre', 'Other')

    if current_genre in PROTECTED_GENRES:
        return current_genre

    if any(word in title for word in ADULT_KEYWORDS):
        return "Adult"

    return current_genre


def safe_read_csv(filepath, max_rows=2_000_000):
    """
    Robustly read CSV files with multiple fallback methods.
    Returns DataFrame or None if all methods fail.
    """
    filename = os.path.basename(filepath)

    try:
        return pd.read_csv(filepath, low_memory=False, nrows=max_rows)
    except Exception:
        pass

    try:
        return pd.read_csv(filepath, low_memory=False, on_bad_lines='skip', nrows=max_rows)
    except Exception:
        pass

    try:
        return pd.read_csv(filepath, low_memory=False, engine='python',
                           on_bad_lines='skip', nrows=max_rows)
    except Exception:
        pass

    try:
        chunks = []
        rows_read = 0
        for chunk in pd.read_csv(filepath, chunksize=100000, low_memory=False,
                                 on_bad_lines='skip'):
            chunks.append(chunk)
            rows_read += len(chunk)
            if rows_read >= max_rows:
                break
        if chunks:
            return pd.concat(chunks, ignore_index=True)
    except Exception as e:
        log(f"All read methods failed for {filename}: {e}", "ERROR")

    return None


def process_and_merge():
    """Main processing pipeline - merge all data into unified database."""
    movie_frames = []
    cast_frames = []
    crew_frames = []
    genre_frames = []
    review_frames = []

    all_csvs = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    raw_csvs = [f for f in all_csvs if os.path.basename(f) not in PRESERVE_FILES]

    log(f"Found {len(raw_csvs)} raw CSV files to process", "FILE")

    for csv_file in raw_csvs:
        filename = os.path.basename(csv_file).lower()
        df = safe_read_csv(csv_file)

        if df is None:
            log(f"Skipping {filename} (could not read)", "WARNING")
            continue

        if len(df) == 0:
            log(f"Skipping {filename} (empty file)", "WARNING")
            continue

        try:
            cols_lower = [c.lower() for c in df.columns]

            if 'cast' in filename or ('character' in cols_lower):
                if 'name' in cols_lower or 'cast' in cols_lower:
                    cast_frames.append(df)
                    log(f"Routed to CAST: {filename} ({len(df)} rows)", "DATA")
                    continue

            if 'crew' in filename or 'director' in filename:
                if 'job' in cols_lower or 'department' in cols_lower:
                    crew_frames.append(df)
                    log(f"Routed to CREW: {filename} ({len(df)} rows)", "DATA")
                    continue

            if 'review' in filename:
                review_frames.append(df)
                log(f"Routed to REVIEWS: {filename} ({len(df)} rows)", "DATA")
                continue

            if 'genre' in filename and len(df) < 100:
                genre_frames.append(df)
                log(f"Routed to GENRES: {filename} ({len(df)} rows)", "DATA")
                continue

            name_cols = ['title', 'Series_Title', 'movie_title', 'original_title',
                         'track_name', 'name', 'show_title', 'Name']
            for c in name_cols:
                if c in df.columns:
                    df = df.rename(columns={c: 'Project'})
                    break

            genre_cols = ['genres', 'listed_in', 'genre', 'Genre', 'category']
            for c in genre_cols:
                if c in df.columns and c != 'Genre':
                    df = df.rename(columns={c: 'Genre'})
                    break

            talent_cols = ['actors', 'stars', 'cast', 'Star1', 'director', 'Director']
            if 'Lead_Talent' not in df.columns:
                for c in talent_cols:
                    if c in df.columns:
                        df = df.rename(columns={c: 'Lead_Talent'})
                        break

            score_cols = ['vote_average', 'IMDB_Rating', 'rating', 'score',
                          'imdb_score', 'Rating']
            for c in score_cols:
                if c in df.columns:
                    df = df.rename(columns={c: 'Sentiment_Score'})
                    break

            pop_cols = ['popularity', 'view_count', 'No_of_Votes', 'votes',
                        'vote_count', 'numVotes']
            for c in pop_cols:
                if c in df.columns:
                    df = df.rename(columns={c: 'Popularity_Score'})
                    break

            if 'Project' in df.columns:
                if "video" in filename or "youtube" in filename:
                    if 'Genre' not in df.columns or df['Genre'].isna().all():
                        df['Genre'] = 'Reality'

                if 'Sentiment_Score' not in df.columns:
                    df['Sentiment_Score'] = 0.5
                if 'Popularity_Score' not in df.columns:
                    df['Popularity_Score'] = 50
                if 'Genre' not in df.columns:
                    df['Genre'] = 'Unknown'
                if 'Lead_Talent' not in df.columns:
                    df['Lead_Talent'] = 'Ensemble'
                if 'id' not in df.columns:
                    df['id'] = [
                        hashlib.md5(str(x).encode()).hexdigest()[:12]
                        for x in range(len(df))
                    ]

                movie_frames.append(df)
                log(f"Routed to MOVIES: {filename} ({len(df)} rows)", "DATA")
            else:
                log(f"Skipping {filename} (no title column found)", "WARNING")

        except Exception as e:
            log(f"Error processing {filename}: {e}", "ERROR")

    if movie_frames:
        log(f"Merging {len(movie_frames)} movie datasets...", "PROCESS")

        final_df = pd.concat(movie_frames, ignore_index=True, sort=False)
        log(f"Initial merge: {len(final_df)} total rows", "DATA")

        final_df = final_df.loc[:, ~final_df.columns.duplicated(keep='first')]
        log(f"After column dedup: {len(final_df.columns)} columns", "DATA")

        final_df['Sentiment_Score'] = pd.to_numeric(
            final_df['Sentiment_Score'], errors='coerce'
        ).fillna(0.5)

        final_df['Popularity_Score'] = pd.to_numeric(
            final_df['Popularity_Score'], errors='coerce'
        ).fillna(50)

        if final_df['Sentiment_Score'].max() > 1.1:
            final_df['Sentiment_Score'] = (final_df['Sentiment_Score'] - 5) / 5
            log("Normalized Sentiment_Score to -1 to 1 scale", "DATA")

        if 'Lead_Talent' in final_df.columns:
            def clean_talent(x):
                if pd.isna(x):
                    return 'Ensemble'
                x = str(x)
                x = x.split(',')[0]
                x = x.replace('[', '').replace(']', '')
                x = x.replace("'", "").replace('"', '')
                x = x.replace('{', '').replace('}', '')
                x = x.strip()[:100]
                return x if x else 'Ensemble'

            final_df['Lead_Talent'] = final_df['Lead_Talent'].apply(clean_talent)
        else:
            final_df['Lead_Talent'] = 'Ensemble'

        log("Cleaning and mapping genres...", "PROCESS")
        final_df['Genre'] = final_df.apply(
            lambda row: clean_genre(row.get('Genre', ''), row.get('Project', '')),
            axis=1
        )
        final_df['Genre'] = final_df.apply(filter_adult_content, axis=1)

        log("Removing duplicates...", "PROCESS")
        final_df['_norm_title'] = final_df['Project'].apply(normalize_title)
        final_df['_genre_quality'] = (final_df['Genre'] != 'Unknown').astype(int)
        final_df['_talent_quality'] = (final_df['Lead_Talent'] != 'Ensemble').astype(int)

        final_df = final_df.sort_values(
            ['_genre_quality', '_talent_quality', 'Popularity_Score'],
            ascending=[False, False, False]
        )

        before = len(final_df)
        final_df = final_df.drop_duplicates(subset=['Project'], keep='first')
        exact_removed = before - len(final_df)

        before = len(final_df)
        final_df = final_df.drop_duplicates(subset=['_norm_title'], keep='first')
        norm_removed = before - len(final_df)

        final_df = final_df.drop(columns=['_norm_title', '_genre_quality', '_talent_quality'])

        log(f"Removed {exact_removed} exact duplicates", "DATA")
        log(f"Removed {norm_removed} normalized duplicates", "DATA")
        log(f"After deduplication: {len(final_df)} unique projects", "DATA")

                # Keep raw popularity only
        final_df = final_df.sort_values('Popularity_Score', ascending=False)
        final_df = final_df.head(TARGET_MOVIE_COUNT)
        log(f"Selected top {TARGET_MOVIE_COUNT} movies by popularity", "DATA")

        existing_core = [c for c in CORE_COLUMNS if c in final_df.columns]
        other_cols = [c for c in final_df.columns if c not in existing_core]

        def count_non_empty(col):
            non_null = final_df[col].notna().sum()
            non_empty = (final_df[col].astype(str).str.strip() != '').sum()
            return min(non_null, non_empty)

        other_cols_sorted = sorted(other_cols, key=count_non_empty, reverse=True)
        final_df = final_df[existing_core + other_cols_sorted]

        if final_df.columns.duplicated().any():
            final_df = final_df.loc[:, ~final_df.columns.duplicated(keep='first')]

        log("Genre Distribution:", "DATA")
        genre_counts = final_df['Genre'].value_counts()
        for genre, count in genre_counts.head(20).items():
            pct = (count / len(final_df)) * 100
            log(f"  {genre}: {count} ({pct:.1f}%)", "INFO")

        unknown_count = genre_counts.get('Unknown', 0)
        if unknown_count > 0:
            log(f"  ⚠️ Unknown: {unknown_count} ({(unknown_count/len(final_df))*100:.1f}%)", "WARNING")

        log("Data Quality Report:", "DATA")
        log(f"  Total projects: {len(final_df)}", "INFO")
        log(f"  Unique genres: {final_df['Genre'].nunique()}", "INFO")
        log(f"  Missing titles: {final_df['Project'].isna().sum()}", "INFO")
        log(f"  Sentiment range: {final_df['Sentiment_Score'].min():.2f} to {final_df['Sentiment_Score'].max():.2f}", "INFO")
        log(f"  Popularity range: {final_df['Popularity_Score'].min():.1f} to {final_df['Popularity_Score'].max():.1f}", "INFO")
        

        ensemble_count = (final_df['Lead_Talent'] == 'Ensemble').sum()
        log(f"  Generic Lead Talent (Ensemble): {ensemble_count} ({(ensemble_count/len(final_df))*100:.1f}%)", "INFO")

        final_df.to_csv(TARGET_MOVIES, index=False)
        log(f"MOVIES saved: {len(final_df)} projects → {TARGET_MOVIES}", "SUCCESS")
        log(f"Columns: {list(final_df.columns[:8])}... ({len(final_df.columns)} total)", "INFO")
    else:
        log("No movie data found to process!", "ERROR")

    if cast_frames:
        cast_df = pd.concat(cast_frames, ignore_index=True, sort=False)
        cast_df = cast_df.loc[:, ~cast_df.columns.duplicated(keep='first')]
        cast_df = cast_df.drop_duplicates()
        cast_df.to_csv(TARGET_CAST, index=False)
        log(f"CAST saved: {len(cast_df)} records → {TARGET_CAST}", "SUCCESS")

    if crew_frames:
        crew_df = pd.concat(crew_frames, ignore_index=True, sort=False)
        crew_df = crew_df.loc[:, ~crew_df.columns.duplicated(keep='first')]
        crew_df = crew_df.drop_duplicates()
        crew_df.to_csv(TARGET_CREW, index=False)
        log(f"CREW saved: {len(crew_df)} records → {TARGET_CREW}", "SUCCESS")

    if genre_frames:
        genre_df = pd.concat(genre_frames, ignore_index=True, sort=False)
        genre_df = genre_df.loc[:, ~genre_df.columns.duplicated(keep='first')]
        genre_df = genre_df.drop_duplicates()
        genre_df.to_csv(TARGET_GENRES, index=False)
        log(f"GENRES saved: {len(genre_df)} records → {TARGET_GENRES}", "SUCCESS")

    if review_frames:
        review_df = pd.concat(review_frames, ignore_index=True, sort=False)
        review_df = review_df.loc[:, ~review_df.columns.duplicated(keep='first')]
        review_df = review_df.drop_duplicates()
        review_df.to_csv(TARGET_REVIEWS, index=False)
        log(f"REVIEWS saved: {len(review_df)} records → {TARGET_REVIEWS}", "SUCCESS")

    log("=" * 50, "INFO")
    log("PIPELINE COMPLETE", "SUCCESS")
    log("=" * 50, "INFO")


def main():
    """Main entry point."""
    print("\n - prep_data.py:983" + "=" * 50)
    print("🎬 GENRE SYNC ANALYTICS DATA PIPELINE v2.0 - prep_data.py:984")
    print("= - prep_data.py:985" * 50 + "\n")

    refresh_flag = "--refresh" in sys.argv

    if refresh_flag:
        log("Force refresh mode enabled", "INFO")

    if not setup_env():
        log("Cannot proceed without Kaggle authentication", "ERROR")
        sys.exit(1)

    fetch_data(force_refresh=refresh_flag)
    process_and_merge()
    cleanup_raw_data()

    print("\n - prep_data.py:1000" + "=" * 50)
    print("✅ All done! Your data is ready for Genre Sync Analytics. - prep_data.py:1001")
    print("= - prep_data.py:1002" * 50 + "\n")


if __name__ == "__main__":
    main()