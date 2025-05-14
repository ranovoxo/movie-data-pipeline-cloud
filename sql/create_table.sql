-- DIMENSION TABLE: Genre ID ↔ Name mapping
CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY,
    genre_name TEXT NOT NULL
);

-- RAW TABLE: Direct JSON load from API
CREATE TABLE IF NOT EXISTS raw_movies (
    id INTEGER PRIMARY KEY,
    title TEXT,
    overview TEXT,
    release_date DATE,
    popularity FLOAT,
    vote_average FLOAT,
    vote_count INTEGER,
    genre_ids TEXT, -- list of genre IDs stored as text or JSON string
    original_language TEXT,
    adult BOOLEAN,
    video BOOLEAN,
    original_title TEXT,
    backdrop_path TEXT,
    poster_path TEXT,
    json_payload JSONB, -- optional full raw response
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SILVER TABLE: Cleaned & semi-structured format
CREATE TABLE IF NOT EXISTS silver_movies (
    id INTEGER PRIMARY KEY,
    title TEXT,
    release_year INTEGER,
    popularity_score FLOAT,
    average_rating FLOAT,
    total_votes INTEGER,
    primary_genre_id INTEGER,  -- references genres(genre_id)
    language TEXT,
    is_adult BOOLEAN,
    extracted_at TIMESTAMP,
    FOREIGN KEY (primary_genre_id) REFERENCES genres (genre_id)
);
