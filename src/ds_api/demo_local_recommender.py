"""
This module aids in generating content recommendations based on users' viewing history.

It fetches the last five viewed content by the user and then finds similar content
based on genres. These recommendations are then written into a `recommendations` database table.
"""

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
    """
    A class responsible for generating content recommendations based on a user's viewing history.
    """

    def __init__(self, user_id: str):
        """
        Constructor method to initialize the RecoMaker class.

        Parameters:
            user_id (str): ID of the user for whom the recommendations are to be made.
        """
        self.user_id = user_id
        self.content_id_list = List[str]
        self.reco_list = List[str]
        self.genre = str

        self.get_last_5()
        self.get_similar_genre()
        self.write_reco()

    def get_last_5(self):
        try:
            user_id = self.user_id
            query = f"""
                SELECT content_id 
                FROM relational.sessions
                WHERE user_id = {user_id}
                ORDER BY start_timestamp DESC
                LIMIT 5;
            """
            df = read(query)
            self.content_id_list = df['content_id'].tolist()
            if len(self.content_id_list) == 0:
                logger.error(f"User {user_id} has not viewed any content.")
        except OperationalError as oe:
            logger.error(f"Operational Error fetching last 5 viewing sessions for user {user_id}: {oe}")
        except IntegrityError as ie:
            logger.error(f"Integrity Error: Maybe there's a constraint being violated while fetching data for user {user_id}: {ie}")
        except StatementError as se:
            logger.error(f"Statement Error: Maybe there's an issue with SQL statement for user {user_id}: {se}")
        except DataError as de:
            logger.error(f"Data Error: Maybe there's an issue with the returned data or its format for user {user_id}: {de}")
        except Exception as e:
            logger.error(f"Unexpected error occurred while fetching last 5 viewing sessions for user {user_id}: {e}")


    def get_similar_genre(self):
        """
        Identify content with similar genres to those in the user's viewing history.

        This method avoids recommending any content that the user has already viewed.
        """
        try:
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
            if len(self.reco_list) == 0:
                logger.error(f"No other titles similar to {self.user_id}'s viewing history that are unviewed by user.")
        except OperationalError as oe:
            logger.error(f"Operational Error fetching similar genre content for user {self.user_id}: {oe}")
        except IntegrityError as ie:
            logger.error(f"Integrity Error: Maybe there's a constraint being violated while fetching similar genre content for user {self.user_id}: {ie}")
        except StatementError as se:
            logger.error(f"Statement Error: Maybe there's an issue with SQL statement for user {self.user_id}: {se}")
        except DataError as de:
            logger.error(f"Data Error: Maybe there's an issue with the returned data or its format for user {self.user_id}: {de}")
        except Exception as e:
            logger.error(f"Unexpected error occurred while fetching similar genre content for user {self.user_id}: {e}")


    def write_reco(self):
        """
        Save the generated content recommendations to the `recommendations` database table.
        """
        user_id = self.user_id
        if len(self.reco_list) > 0:        
            for content_id in self.reco_list:
                try:
                    query = f""" 
                    INSERT INTO recommendations (user_id, content_id)
                    VALUES ({user_id}, '{content_id}');
                    """
                    write(query)
                    logger.info(f"Successfully wrote recommendation {content_id} for user {user_id}.")
                except OperationalError as oe:
                    logger.error(f"Operational Error writing recommendation {content_id} for user {user_id}: {oe}")
                    continue
                except IntegrityError as ie:
                    logger.error(f"Integrity Error: Maybe there's a constraint being violated while writing recommendation {content_id} for user {user_id}: {ie}")
                    continue
                except StatementError as se:
                    logger.error(f"Statement Error: Maybe there's an issue with SQL statement for recommendation {content_id} for user {self.user_id}: {se}")
                    continue
                except DataError as de:
                    logger.error(f"Data Error: Maybe there's an issue with the data format for recommendation {content_id} for user {self.user_id}: {de}")
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error occurred while writing recommendation {content_id} for user {user_id}: {e}")
                    continue
        else:
            logger.error(f"No recommendations to write for user {user_id}.")


recommendation = RecoMaker(280)
