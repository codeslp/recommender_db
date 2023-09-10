
import os
import logging

from dotenv import load_dotenv
import pandas as pd
from pydantic import BaseModel
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, OperationalError, StatementError, DataError
from tabulate import tabulate
from typing import Optional, List

from db_local_api import read, write

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s]',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class RecoMaker:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.content_id_list = List[str]
        self.reco_list = List[str]
        self.genre = str

        self.get_last_5()
        self.get_similar_genre()
        self.write_reco()

    def get_last_5(self):
        user_id = self.user_id
        query=f"""
            SELECT content_id 
            FROM relational.sessions
            WHERE user_id = {user_id}
            ORDER BY start_timestamp DESC
            LIMIT 5;
        """
        df = read(query)
        self.content_id_list = df['content_id'].tolist()

    def get_similar_genre(self):
        ids = str(self.content_id_list).split('[')[1].split(']')[0]
        query=f"""
            WITH genre_list AS (
                SELECT genre
                FROM relational.genres
                WHERE content_id IN ({ids})
            )
            SELECT DISTINCT content_id
            FROM relational.genres
            WHERE genre IN (SELECT genre FROM genre_list)
            AND content_id NOT IN ({ids})
            LIMIT 10;
        """
        df = read(query)
        self.reco_list = df['content_id'].tolist()


    def write_reco(self):
        user_id = self.user_id
        
        for content_id in self.reco_list:
            query = f""" 
            INSERT INTO recommendations (user_id, content_id)
            VALUES ({user_id}, '{content_id}');
            """
            write(query)

        logger.info(f"Successfully wrote {len(self.reco_list)} new recommendations for user {user_id}.")


recommendation = RecoMaker(280)
