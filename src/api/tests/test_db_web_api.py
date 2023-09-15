import logging

import pytest
from fastapi.testclient import TestClient
import json
import random
from datetime import datetime, timedelta, date

from ds_web_api import app
from models import (Titles, Genres, ProdCountries, Credits, Users, ViewSessions, SubscriptionType,
                    TitleFilter, GenreFilter, ProdCountryFilter, CreditFilter, UserFilter, ViewSessionFilter)

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s]',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


client = TestClient(app)


# 1. Helper functions to generate random data
def random_date(start: datetime, end: datetime) -> date:
    return (start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))).date()

def random_string(length: int) -> str:
    return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=length))

def random_bool() -> bool:
    return bool(random.getrandbits(1))

# 2. Generate sample data for each model
class SampleData:
    def __init__(self):
        self.title_key = None
        self.user_key = None

    def generate_title(self) -> Titles:
        new_title = Titles(
            content_id=random_string(10),
            title="Test Title",
            content_type=random.choice(["movie", "show"]),
            release_year=random.randint(1900, 2023),
            age_certification="PG",
            runtime=random.randint(60, 180),
            number_of_seasons=random.randint(1, 10) if random_bool() else None,
            imdb_id=random_string(9),
            imdb_score=random.uniform(1, 10),
            imdb_votes=random.randint(1000, 1000000),
            is_year_best=random_bool(),
            is_all_time_best=random_bool()
        )
        self.title_key = new_title.content_id
        return new_title
    
    def generate_genre(self) -> Genres:
        new_genre = Genres(
            content_id=self.title_key,
            genre="Action",
            is_main_genre=random_bool()
        )
        return new_genre

    def generate_prod_country(self) -> ProdCountries:
        new_prod_country = ProdCountries(
            content_id=self.title_key,
            country="USA",
            is_main_country=random_bool()
        )
        return new_prod_country

    def generate_credit(self) -> Credits:
        new_credit = Credits(
            content_id=self.title_key,
            person_id=random_string(7),
            role=random.choice(["actor", "director", "writer"]),
            first_name="Test",
            middle_name="Test",
            last_name="Test",
            character="Test"
        )
        return new_credit
    
    def generate_user(self) -> Users:
        new_user = Users(
            user_id=random.randint(1, 10000),
            birth_date=(random_date(datetime(1970, 1, 1), datetime(2003, 1, 1))),
            subscription_date=(random_date(datetime(2020, 1, 1), datetime(2023, 1, 1))),
            subscription_type=random.choice(list(SubscriptionType))
        )
        self.user_key = new_user.user_id
        return new_user

    def generate_view_session(self) -> ViewSessions:
        start = random_date(datetime(2022, 1, 1), datetime(2023, 1, 1))
        end = start + timedelta(hours=random.randint(1, 5))
        new_view_session = ViewSessions(
            start_timestamp=start,
            end_timestamp=end,
            content_id=self.title_key,
            user_id=self.user_key,
            user_rating=random.randint(1, 5) if random_bool() else None
        )
        return new_view_session
    
    def __repr__(self):
        return f"<SampleData(title_key={self.title_key}, user_key={self.user_key})>"
    
    def to_dict(self):
        return {
            "title": self.generate_title().dict(),
            "genre": self.generate_genre().dict(),
            "prod_country": self.generate_prod_country().dict(),
            "credit": self.generate_credit().dict(),
            "user": self.generate_user().dict(),
            "view_session": self.generate_view_session().dict()
        }


def test_title_crud():
    sample_data = SampleData().to_dict()
    title_data = sample_data["title"]
    # 1. Create a title
    title_data = sample_data["title"]
    create_response = client.post("/title/", json=title_data)
    assert create_response.status_code == 200

    # 2. Search for the created title
    title_filter = {
        "content_id": title_data["content_id"]
    }
    search_response = client.post("/title/search/", json=title_filter)
    assert search_response.status_code == 200

    titles = search_response.json()
    assert len(titles) == 1
    assert titles[0]["content_id"] == title_data["content_id"]

    # 3. Delete the searched title
    delete_response = client.post("/title/delete/", json=title_filter)
    assert titles[0]["content_id"] == title_data["content_id"]
    assert delete_response.status_code == 200


def test_genre_crud():
    # Step 1: Create a sample data instance and convert it to a dictionary format.
    sample_data = SampleData().to_dict()
    new_title = sample_data["title"]
    new_genre = sample_data["genre"]

    # Step 2: Create a new title.
    create_title_response = client.post("/title/", json=new_title)
    assert create_title_response.status_code == 200
    logger.info(f"create_title_response: {create_title_response.json()}")

    # Step 3: Create a new genre associated with the title.
    create_genre_response = client.post("/genre/", json=new_genre)
    assert create_genre_response.status_code == 200
    created_genre = create_genre_response.json()
    assert created_genre["content_id"] == new_genre["content_id"]

    # Step 4: Search for the created genre using its content_id.
    genre_filter = {"content_id": new_genre["content_id"]}
    search_genre_response = client.post("/genre/search/", json=genre_filter)
    assert search_genre_response.status_code == 200
    searched_genres = search_genre_response.json()
    assert len(searched_genres) == 1
    assert searched_genres[0]["content_id"] == new_genre["content_id"]

    # Step 5: Delete the created genre.
    delete_genre_response = client.post("/genre/delete/", json=genre_filter)  # using DELETE method for deletion
    assert searched_genres[0]["content_id"] == new_genre["content_id"]
    assert delete_genre_response.status_code == 200

    # Step 6: Delete the associated title.
    title_filter = {"content_id": new_title["content_id"]}
    delete_title_response = client.post("/title/delete/", json=title_filter)  # using DELETE method for deletion
    assert delete_title_response.status_code == 200


def test_prod_country_crud():
    sample_data = SampleData().to_dict()
    new_title = sample_data["title"]
    new_prod_country = sample_data["prod_country"]

    # Create a new title.
    create_title_response = client.post("/title/", json=new_title)
    assert create_title_response.status_code == 200
    logger.info(f"create_title_response: {create_title_response.json()}")

    # Create a new prod_country associated with the title.
    create_prod_country_response = client.post("/prod_country/", json=new_prod_country)
    assert create_prod_country_response.status_code == 200
    created_prod_country = create_prod_country_response.json()
    assert created_prod_country["content_id"] == new_prod_country["content_id"]

    # Search for the created prod_country using its content_id.
    prod_country_filter = {"content_id": new_prod_country["content_id"]}
    search_prod_country_response = client.post("/prod_country/search/", json=prod_country_filter)
    assert search_prod_country_response.status_code == 200
    searched_prod_countries = search_prod_country_response.json()
    assert len(searched_prod_countries) == 1
    assert searched_prod_countries[0]["content_id"] == new_prod_country["content_id"]

    # Delete the created prod_country.
    delete_prod_country_response = client.post("/prod_country/delete/", json=prod_country_filter)  # using DELETE method for deletion
    assert searched_prod_countries[0]["content_id"] == new_prod_country["content_id"]
    assert delete_prod_country_response.status_code == 200

    # Delete the associated title.
    title_filter = {"content_id": new_title["content_id"]}
    delete_title_response = client.post("/title/delete/", json=title_filter)  # using DELETE method for deletion
    assert delete_title_response.status_code == 200


def test_credit_crud():
    sample_data = SampleData().to_dict()
    new_title = sample_data["title"]
    new_credit = sample_data["credit"]

    # Create a new title.
    create_title_response = client.post("/title/", json=new_title)
    assert create_title_response.status_code == 200
    logger.info(f"create_title_response: {create_title_response.json()}")

    # Create a new credit associated with the title.
    create_credit_response = client.post("/credit/", json=new_credit)
    assert create_credit_response.status_code == 200
    created_credit = create_credit_response.json()
    assert created_credit["content_id"] == new_credit["content_id"]

    # Search for the created credit using its content_id.
    credit_filter = {"content_id": new_credit["content_id"]}
    search_credit_response = client.post("/credit/search/", json=credit_filter)
    assert search_credit_response.status_code == 200
    searched_credits = search_credit_response.json()
    assert len(searched_credits) == 1
    assert searched_credits[0]["content_id"] == new_credit["content_id"]

    # Delete the created credit.
    delete_credit_response = client.post("/credit/delete/", json=credit_filter)  # using DELETE method for deletion
    assert searched_credits[0]["content_id"] == new_credit["content_id"]
    assert delete_credit_response.status_code == 200

    # Delete the associated title.
    title_filter = {"content_id": new_title["content_id"]}
    delete_title_response = client.post("/title/delete/", json=title_filter)  # using DELETE method for deletion
    assert delete_title_response.status_code == 200
    

def test_user_crud():
    sample_data = SampleData().to_dict()
    sample_data['user']['birth_date'] = (random_date(datetime(1970, 1, 1), datetime(2003, 1, 1))).isoformat()
    sample_data['user']['subscription_date'] = (random_date(datetime(2020, 1, 1), datetime(2023, 1, 1))).isoformat()
    new_user = sample_data["user"]

    # Create a new user.
    create_user_response = client.post("/user/", json=new_user)
    assert create_user_response.status_code == 200
    created_user = create_user_response.json()
    assert created_user["user_id"] == new_user["user_id"]

    # Search for the created user using its user_id.
    user_filter = {"user_id": new_user["user_id"]}
    search_user_response = client.post("/user/search/", json=user_filter)
    assert search_user_response.status_code == 200
    searched_users = search_user_response.json()
    assert len(searched_users) == 1
    assert searched_users[0]["user_id"] == new_user["user_id"]

    # Delete the created user.
    delete_user_response = client.post("/user/delete/", json=user_filter)  # using DELETE method for deletion
    assert searched_users[0]["user_id"] == new_user["user_id"]
    assert delete_user_response.status_code == 200

def view_session_crud():
    sample_data = SampleData().to_dict()
    new_title = sample_data["title"]
    sample_data['user']['birth_date'] = (random_date(datetime(1970, 1, 1), datetime(2003, 1, 1))).isoformat()
    sample_data['user']['subscription_date'] = (random_date(datetime(2020, 1, 1), datetime(2023, 1, 1))).isoformat()
    sample_data['view_session']['start_timestamp'] = random_date(datetime(2022, 1, 1), datetime(2023, 1, 1)).isoformat()
    sample_data['view_session']['end_timestamp'] = (sample_data['view_session']['start_timestamp'] + timedelta(hours=random.randint(1, 5))).isoformat()
    new_user = sample_data["user"]
    new_view_session = sample_data["view_session"]

    # Create a new title.
    create_title_response = client.post("/title/", json=new_title)
    assert create_title_response.status_code == 200
    logger.info(f"create_title_response: {create_title_response.json()}")

    # Create a new user.
    create_user_response = client.post("/user/", json=new_user)
    assert create_user_response.status_code == 200
    created_user = create_user_response.json()
    assert created_user["user_id"] == new_user["user_id"]

    # Create a new view_session associated with the title and user.
    create_view_session_response = client.post("/view_session/", json=new_view_session)
    assert create_view_session_response.status_code == 200
    created_view_session = create_view_session_response.json()
    assert created_view_session["content_id"] == new_view_session["content_id"]
    assert created_view_session["user_id"] == new_view_session["user_id"]

    # Search for the created view_session using its content_id and user_id.
    view_session_filter = {
        "content_id": new_view_session["content_id"],
        "user_id": new_view_session["user_id"]
    }
    search_view_session_response = client.post("/view_session/search/", json=view_session_filter)   
    assert search_view_session_response.status_code == 200
    searched_view_sessions = search_view_session_response.json()
    assert len(searched_view_sessions) == 1
    assert searched_view_sessions[0]["content_id"] == new_view_session["content_id"]
    assert searched_view_sessions[0]["user_id"] == new_view_session["user_id"]

    # Delete the created view_session.
    delete_view_session_response = client.post("/view_session/delete/", json=view_session_filter)  # using DELETE method for deletion
    assert searched_view_sessions[0]["content_id"] == new_view_session["content_id"]
    assert searched_view_sessions[0]["user_id"] == new_view_session["user_id"]
    assert delete_view_session_response.status_code == 200

    # Delete the associated title.
    title_filter = {"content_id": new_title["content_id"]}
    delete_title_response = client.post("/title/delete/", json=title_filter)  # using DELETE method for deletion
    assert delete_title_response.status_code == 200

    # Delete the associated user.
    user_filter = {"user_id": new_user["user_id"]}
    delete_user_response = client.post("/user/delete/", json=user_filter)  # using DELETE method for deletion
    assert delete_user_response.status_code == 200

