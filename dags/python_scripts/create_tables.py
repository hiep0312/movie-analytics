from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ddl = """
    BEGIN;

    CREATE SCHEMA IF NOT EXISTS movies;

    CREATE TABLE IF NOT EXISTS movies.stage_ratings (
        user_movie_id BIGSERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        rating NUMERIC
    );

    CREATE TABLE IF NOT EXISTS movies.ratings (
        user_movie_id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        rating NUMERIC
    );

    CREATE TABLE IF NOT EXISTS movies.stage_movies (
        movie_id INT PRIMARY KEY,
        is_adult VARCHAR(5) NOT NULL,
        budget BIGINT NOT NULL,
        original_language CHAR(2) NOT NULL,
        title VARCHAR(300) NOT NULL,
        popularity REAL,
        release_date DATE NOT NULL,
        revenue BIGINT NOT NULL,
        vote_count INT,
        vote_average REAL
    );

    CREATE TABLE IF NOT EXISTS movies.movies (
        movie_id INT PRIMARY KEY,
        is_adult VARCHAR(5) NOT NULL,
        budget BIGINT NOT NULL,
        original_language CHAR(2) NOT NULL,
        title VARCHAR(300) NOT NULL,
        popularity REAL,
        release_date DATE,
        revenue BIGINT NOT NULL,
        vote_count INT,
        vote_average REAL
    );

    CREATE TABLE IF NOT EXISTS movies.stage_movie_genre (
        movie_id INT NOT NULL,
        genre_id INT NOT NULL,
        PRIMARY KEY (movie_id, genre_id)
    );

    CREATE TABLE IF NOT EXISTS movies.movie_genre (
        movie_id INT NOT NULL,
        genre_id INT NOT NULL,
        PRIMARY KEY (movie_id, genre_id)
    );

    CREATE TABLE IF NOT EXISTS movies.stage_genre (
        genre_id INT PRIMARY KEY,
        genre_name VARCHAR(300)
    );

    CREATE TABLE IF NOT EXISTS movies.genre (
        genre_id INT PRIMARY KEY,
        genre_name VARCHAR(300)
    );

    CREATE TABLE IF NOT EXISTS movies.stage_date (
        release_date DATE PRIMARY KEY,
        day INT,
        week INT,
        month INT,
        quarter INT,
        year INT
    );

    CREATE TABLE IF NOT EXISTS movies.date (
        release_date DATE PRIMARY KEY,
        day INT,
        week INT,
        month INT,
        quarter INT,
        year INT
    );

    CREATE TABLE IF NOT EXISTS movies.stage_cpi (
        date_cd DATE PRIMARY KEY,
        consumer_price_index REAL
    );

    CREATE TABLE IF NOT EXISTS movies.cpi (
        date_cd DATE PRIMARY KEY,
        consumer_price_index REAL
    );

    END;
    """

def create_tables(params):
    engine = create_engine(params["postgres_conn_string"])

    try:
        with engine.connect() as connection:
            connection.execute(text(ddl))
            logger.info("Tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise