from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

com = """
BEGIN;

-- Upsert date table
UPDATE movies.date 
SET day = md.day, week = md.week, month = md.month,
quarter = md.quarter, year = md.year
FROM movies.stage_date md
WHERE movies.date.release_date = md.release_date; 

INSERT INTO movies.date
SELECT md.* FROM movies.stage_date md LEFT JOIN movies.date
ON md.release_date = movies.date.release_date
WHERE movies.date.release_date IS NULL;

DROP TABLE IF EXISTS movies.stage_date;

END;
"""

def upsert_date(params):
    engine = create_engine(params["postgres_conn_string"])

    try:
        with engine.connect() as connection:
            connection.execute(text(com))
            logger.info("Upsert date successfully.")
    except Exception as e:
        logger.error(f"Error upsert date: {str(e)}")
        raise