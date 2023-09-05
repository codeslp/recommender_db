import os
import logging

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from tabulate import tabulate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class db_api:
    def __init__(self):
        self.engine = self.connect_to_db()
        self.set_search_path()

    def connect_to_db(self):
        db_url = f"postgresql+psycopg2://{os.getenv('DS_USER')}:{os.getenv('DS_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        return create_engine(db_url)

    def set_search_path(self):
        with self.engine.begin() as conn:
            conn.execute(text("SET search_path TO relational;"))

    def read(self, query, params=None):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query).bindparams(**params if params else {}))
                data = result.fetchall()
                columns = result.keys()
        except SQLAlchemyError as e:
            logger.error(f"Query caused this error: {e}")
            data, columns = None, None
        return data, columns
    
    def write(self, query, params=None):
        try:
            with self.engine.begin() as conn:
                conn.execute(text(query).bindparams(**params if params else {}))
            logger.info("Data written to database.")
        except SQLAlchemyError as e:
            logger.error(f"Failed to write to database due to this error: {e}")

api = db_api()

def read(query, **kwargs):
    data, columns = api.read(query, params=kwargs.get('params'))
    if data is None or columns is None:
        logger.error("Query returned None.")
        return None
    df = pd.DataFrame(data, columns=columns)
    if kwargs.get('verbose', True):
        print(tabulate(df, headers='keys', tablefmt='rounded_outline'))
    return df

def write(query, **kwargs):
    api.write(query, params=kwargs.get('params'))