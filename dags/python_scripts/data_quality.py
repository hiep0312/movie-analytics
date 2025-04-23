from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_data_quality(params, table_names):
    engine = create_engine(params["postgres_conn_string"])
    dq_checks = [
        {'table': 'movies.movies', 'check_sql': "SELECT COUNT(*) FROM movies.movies WHERE movie_id IS NULL", 'expected_result': 0},
        {'table': 'movies.genre', 'check_sql': "SELECT COUNT(*) FROM movies.genre WHERE genre_id IS NULL", 'expected_result': 0},
        {'table': 'movies.date', 'check_sql': "SELECT COUNT(*) FROM movies.date WHERE release_date IS NULL", 'expected_result': 0},
        {'table': 'movies.cpi', 'check_sql': "SELECT COUNT(*) FROM movies.cpi WHERE date_cd IS NULL", 'expected_result': 0},
    ]

    with engine.connect() as conn:
        try:
            for table in table_names:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                if count is None or count == 0:
                    raise ValueError(f"Data quality check failed. {table} returned no results")

            for check in dq_checks:
                result = conn.execute(text(check['check_sql']))
                count = result.scalar()
                if count != check['expected_result']:
                    logger.info(f"Data quality issue in table {check['table']}:")
                    logger.info(f" - Found {count} NULL values")
                    logger.info(f" - Expected {check['expected_result']}")
                    raise ValueError(f"Data quality check failed for {check['table']}")
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            raise
    logger.info("✅ Data quality checks passed.")