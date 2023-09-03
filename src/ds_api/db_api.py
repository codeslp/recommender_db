import os
import logging

from dotenv import load_dotenv
import pandas as pd
import psycopg2


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class db_api:
    def __init__(self):
        self.connection = self.connect_to_db()

    def connect_to_db(self):
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DS_USER"),
            password=os.getenv("DS_PASSWORD"),
        )

    def run_query(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        except psycopg2.Error as e:
            logger.error(f"Query caused this error: {e}")
            result, columns = None, None
        finally:
            cursor.close()
        return result, columns

    def close_connection(self):
        if self.connection:
            self.connection.close()


def query_db(query, verbose=True):
    api = db_api()
    result, columns = api.run_query(query)
    api.close_connection()
    if result is None or columns is None:
        logger.error("Query returned None.")
        return None
    df = pd.DataFrame(result, columns=columns)
    if verbose:
        logger.info(df.to_string())
    return df
