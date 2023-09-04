import pytest
import psycopg2

from dotenv import load_dotenv
import os

load_dotenv()

@pytest.fixture(scope="module")
def test_db():
    connection = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DS_USER"),
        password=os.getenv("DS_PASSWORD"),
    )
    yield connection
    connection.close()
