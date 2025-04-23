from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

com = """
BEGIN;

-- Upsert movies table
UPDATE movies.movies 
SET is_adult = mm.is_adult, budget = mm.budget, original_language = mm.original_language,
title = mm.title, popularity = mm.popularity, release_date = mm.release_date,
revenue = mm.revenue, vote_count = mm.vote_count, vote_average = mm.vote_average
FROM movies.stage_movies mm
WHERE movies.movies.movie_id = mm.movie_id; 


INSERT INTO movies.movies 
SELECT mm.* FROM movies.stage_movies mm LEFT JOIN movies.movies 
ON mm.movie_id = movies.movies.movie_id
WHERE movies.movies.movie_id IS NULL;

DROP TABLE movies.stage_movies;

END;


"""

def upsert_movies(params):
    engine = create_engine(params["postgres_conn_string"])

    try:
        with engine.connect() as connection:
            connection.execute(text(com))
            logger.info("Upsert movies successfully.")
    except Exception as e:
        logger.error(f"Error movies genre: {str(e)}")
        raise