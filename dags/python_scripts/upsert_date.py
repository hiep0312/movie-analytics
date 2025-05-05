from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

com = """
BEGIN;

UPDATE movies.date 
SET day = md.day,
    week = md.week,
    month = md.month,
    quarter = md.quarter,
    year = md.year
FROM movies.stage_date md
WHERE movies.date.release_date = md.release_date;

INSERT INTO movies.date (release_date, day, week, month, quarter, year)
SELECT md.release_date, md.day, md.week, md.month, md.quarter, md.year
FROM movies.stage_date md
LEFT JOIN movies.date d
  ON md.release_date = d.release_date
WHERE d.release_date IS NULL;

DROP TABLE IF EXISTS movies.stage_date;

COMMIT;

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