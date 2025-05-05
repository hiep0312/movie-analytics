from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

com = """
BEGIN;


UPDATE movies.ratings 
SET rating = ms.rating 
FROM movies.stage_ratings ms 
WHERE movies.ratings.user_id = ms.user_id 
  AND movies.ratings.movie_id = ms.movie_id;

INSERT INTO movies.ratings (user_id, movie_id, rating)
SELECT ms.user_id, ms.movie_id, ms.rating
FROM movies.stage_ratings ms
LEFT JOIN movies.ratings r
  ON ms.user_id = r.user_id AND ms.movie_id = r.movie_id
WHERE r.user_id IS NULL;

DROP TABLE IF EXISTS movies.stage_ratings;

COMMIT;

"""

def upsert_ratings(params):
    engine = create_engine(params["postgres_conn_string"])

    try:
        with engine.connect() as connection:
            connection.execute(text(com))
            logger.info("Upsert ratings successfully.")
    except Exception as e:
        logger.error(f"Error ratings genre: {str(e)}")
        raise