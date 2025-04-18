BEGIN;

CREATE SCHEMA IF NOT EXISTS movies;

-- Bảng stage_ratings
CREATE TABLE IF NOT EXISTS movies.stage_ratings (
    user_movie_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating NUMERIC
);

-- Bảng ratings
CREATE TABLE IF NOT EXISTS movies.ratings (
    user_movie_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating NUMERIC
);

-- Bảng stage_movies
CREATE TABLE IF NOT EXISTS movies.stage_movies (
    movie_id INT PRIMARY KEY,
    is_adult VARCHAR(5) NOT NULL,
    budget BIGINT NOT NULL,
    original_language CHAR(2) NOT NULL,
    title VARCHAR(300) NOT NULL,
    popularity FLOAT,
    release_date DATE NOT NULL,
    revenue BIGINT NOT NULL,
    vote_count INT,
    vote_average FLOAT
);

-- Bảng movies
CREATE TABLE IF NOT EXISTS movies.movies (
    movie_id INT PRIMARY KEY,
    is_adult VARCHAR(5) NOT NULL,
    budget BIGINT NOT NULL,
    original_language CHAR(2) NOT NULL,
    title VARCHAR(300) NOT NULL,
    popularity FLOAT,
    release_date DATE,
    revenue BIGINT NOT NULL,
    vote_count INT,
    vote_average FLOAT
);

-- Bảng stage_movie_genre
CREATE TABLE IF NOT EXISTS movies.stage_movie_genre (
    movie_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY (movie_id, genre_id)
);

-- Bảng movie_genre
CREATE TABLE IF NOT EXISTS movies.movie_genre (
    movie_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY (movie_id, genre_id)
);

-- Bảng stage_genre
CREATE TABLE IF NOT EXISTS movies.stage_genre (
    genre_id INT PRIMARY KEY,
    genre_name VARCHAR(300)
);

-- Bảng genre
CREATE TABLE IF NOT EXISTS movies.genre (
    genre_id INT PRIMARY KEY,
    genre_name VARCHAR(300)
);

-- Bảng stage_date
CREATE TABLE IF NOT EXISTS movies.stage_date (
    release_date DATE PRIMARY KEY,
    day INT,
    week INT,
    month INT,
    quarter INT,
    year INT
);

-- Bảng date
CREATE TABLE IF NOT EXISTS movies.date (
    release_date DATE PRIMARY KEY,
    day INT,
    week INT,
    month INT,
    quarter INT,
    year INT
);

-- Bảng stage_cpi
CREATE TABLE IF NOT EXISTS movies.stage_cpi (
    date_cd DATE PRIMARY KEY,
    consumer_price_index FLOAT
);

-- Bảng cpi
CREATE TABLE IF NOT EXISTS movies.cpi (
    date_cd DATE PRIMARY KEY,
    consumer_price_index FLOAT
);

END;
