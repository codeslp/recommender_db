import pytest
from fastapi.testclient import TestClient
import json
import random
from datetime import datetime, timedelta, date

from ds_web_api import app
from models import (Titles, Genres, ProdCountries, Credits, Users, ViewSessions, SubscriptionType,
                    TitleFilter, GenreFilter, ProdCountryFilter, CreditFilter, UserFilter, ViewSessionFilter)

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
            role=random.choice(["actor", "director", "writer"])
        )
        return new_credit
    
    def generate_user(self) -> Users:
        new_user = Users(
            user_id=random.randint(1, 10000),
            birth_date=random_date(datetime(1970, 1, 1), datetime(2003, 1, 1)),
            subscription_date=random_date(datetime(2020, 1, 1), datetime(2023, 1, 1)),
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


# def test_title_crud():
#     # 1. Create a title
#     title_data = sample_data["title"]
#     create_response = client.post("/title/", json=title_data)
#     assert create_response.status_code == 200

#     # 2. Search for the created title
#     title_filter = {
#         "content_id": title_data["content_id"]
#     }
#     search_response = client.post("/title/search/", json=title_filter)
#     assert search_response.status_code == 200

#     titles = search_response.json()
#     assert len(titles) == 1
#     assert titles[0]["content_id"] == title_data["content_id"]

#     # 3. Delete the searched title
#     delete_response = client.post("/title/delete/", json=title_filter)
#     assert titles[0]["content_id"] == title_data["content_id"]
#     assert delete_response.status_code == 200


def test_genre_crud():
    # Step 1: Create a sample data instance and convert it to a dictionary format.
    sample_data = SampleData().to_dict()
    new_title = sample_data["title"]
    new_genre = sample_data["genre"]

    # Step 2: Create a new title.
    print(new_title)
    create_title_response = client.post("/title/", json=new_title)
    print(create_title_response.json())
    assert create_title_response.status_code == 200

    # Step 3: Create a new genre associated with the title.
    create_genre_response = client.post("/genre/", json=new_genre)
    assert create_genre_response.status_code == 200
    created_genre = create_genre_response.json()
    assert created_genre["content_id"] == new_genre["content_id"]

    # Step 4: Search for the created genre using its content_id.
    genre_filter = {"content_id": new_genre["content_id"]}
    search_genre_response = client.post("/genres/search/", json=genre_filter)
    assert search_genre_response.status_code == 200
    searched_genres = search_genre_response.json()
    assert len(searched_genres) == 1
    assert searched_genres[0]["content_id"] == new_genre["content_id"]

    # Step 5: Delete the created genre.
    delete_genre_response = client.delete("/genre/delete/", json=genre_filter)  # using DELETE method for deletion
    assert delete_genre_response.status_code == 200
    assert delete_genre_response.json()["message"].startswith("1 genre(s) deleted successfully")

    # Step 6: Delete the associated title.
    delete_title_response = client.delete("/title/delete/", json=new_title)  # using DELETE method for deletion
    assert delete_title_response.status_code == 200



# def test_prod_country_crud():
#     # 1. Create a prod_country
#     prod_country_data = generate_prod_country().dict()
#     create_response = client.post("/prod_country/", json=prod_country_data)
#     assert create_response.status_code == 200
#     created_prod_country = create_response.json()
#     assert created_prod_country["content_id"] == prod_country_data["content_id"]
    
#     # 2. Search for the created prod_country
#     prod_country_filter = {
#         "content_id": prod_country_data["content_id"]
#     }
#     search_response = client.post("/prod_country/search/", json=prod_country_filter)
#     assert search_response.status_code == 200

#     prod_countries = search_response.json()
#     assert len(prod_countries) == 1
#     assert prod_countries[0]["content_id"] == prod_country_data["content_id"]

#     # 3. Delete the searched prod_country
#     delete_response = client.post("/prod_country/", json=prod_country_filter)
#     assert delete_response.status_code == 200
#     assert delete_response.json()["message"].startswith("1 prod_country(s) deleted successfully")


# def test_credit_crud():
#     # Create
#     credit_data = generate_credit().dict()
#     create_response = client.post("/credit/", json=credit_data)
#     assert create_response.status_code == 200
#     created_credit = create_response.json()
#     assert created_credit["content_id"] == credit_data["content_id"]
    
#     # Search
#     credit_filter = {
#         "content_id": credit_data["content_id"]
#     }
#     search_response = client.post("/credit/search/", json=credit_filter)
#     assert search_response.status_code == 200
#     credits = search_response.json()
#     assert len(credits) == 1
#     assert credits[0]["content_id"] == credit_data["content_id"]

#     # Delete
#     delete_response = client.post("/credit/", json=credit_filter)
#     assert delete_response.status_code == 200
#     assert delete_response.json()["message"].startswith("1 credit(s) deleted successfully")


# def test_user_crud():
#     # Create
#     user_data = generate_user().dict()
#     create_response = client.post("/user/", json=user_data)
#     assert create_response.status_code == 200
#     created_user = create_response.json()
#     assert created_user["user_id"] == user_data["user_id"]

#     # Search
#     user_filter = {
#         "user_id": user_data["user_id"]
#     }
#     search_response = client.post("/user/search/", json=user_filter)
#     assert search_response.status_code == 200
#     users = search_response.json()
#     assert len(users) == 1
#     assert users[0]["user_id"] == user_data["user_id"]

#     # Delete
#     delete_response = client.post("/user/", json=user_filter)
#     assert delete_response.status_code == 200
#     assert delete_response.json()["message"].startswith("1 user(s) deleted successfully")



# def test_view_session_crud():
#     # Create
#     view_session_data = generate_view_session().dict()
#     create_response = client.post("/view_session/", json=view_session_data)
#     assert create_response.status_code == 200
#     created_view_session = create_response.json()
#     assert created_view_session["start_timestamp"] == view_session_data["start_timestamp"]

#     # Search
#     view_session_filter = {
#         "start_timestamp": view_session_data["start_timestamp"]
#     }
#     search_response = client.post("/view_session/search/", json=view_session_filter)
#     assert search_response.status_code == 200
#     view_sessions = search_response.json()
#     assert len(view_sessions) == 1
#     assert view_sessions[0]["start_timestamp"] == view_session_data["start_timestamp"]

#     # Delete
#     delete_response = client.post("/view_session/", json=view_session_filter)
#     assert delete_response.status_code == 200
#     assert delete_response.json()["message"].startswith("1 view_session(s) deleted successfully")


# def test_title_error_cases():
#     # Try to fetch a non-existent title
#     title_filter = {"content_id": "NON_EXISTING_ID"}
#     search_response = client.post("/title/search/", json=title_filter)
#     assert search_response.status_code == 404

#     # Try to create a title with missing data
#     incomplete_title_data = {
#         "content_id": random_string(10),
#         "title": "Incomplete Test Title"
#     }
#     create_response = client.post("/title/", json=incomplete_title_data)
#     assert create_response.status_code == 400

#     # Try to delete a non-existent title
#     delete_response = client.post("/title/", json=title_filter)
#     assert delete_response.status_code == 404


# def test_genre_error_cases():
#     # Try to fetch a non-existent genre
#     genre_filter = {"content_id": "NON_EXISTING_ID"}
#     search_response = client.post("/genre/search/", json=genre_filter)
#     assert search_response.status_code == 404

#     # Try to create a genre with missing data
#     incomplete_genre_data = {
#         "content_id": random_string(10)
#     }
#     create_response = client.post("/genre/", json=incomplete_genre_data)
#     assert create_response.status_code == 400

#     # Try to delete a non-existent genre
#     delete_response = client.post("/genre/", json=genre_filter)
#     assert delete_response.status_code == 404


# def test_prod_country_error_cases():
#     # Try to fetch a non-existent production country
#     prod_country_filter = {"content_id": "NON_EXISTING_ID"}
#     search_response = client.post("/prod_country/search/", json=prod_country_filter)
#     assert search_response.status_code == 404

#     # Try to create a production country with missing data
#     incomplete_prod_country_data = {
#         "content_id": random_string(10)
#     }
#     create_response = client.post("/prod_country/", json=incomplete_prod_country_data)
#     assert create_response.status_code == 400

#     # Try to delete a non-existent production country
#     delete_response = client.post("/prod_country/", json=prod_country_filter)
#     assert delete_response.status_code == 404


# def test_credit_error_cases():
#     # Try to fetch a non-existent credit
#     credit_filter = {"content_id": "NON_EXISTING_ID"}
#     search_response = client.post("/credit/search/", json=credit_filter)
#     assert search_response.status_code == 404

#     # Try to create a credit with missing data
#     incomplete_credit_data = {
#         "content_id": random_string(10),
#         "person_id": random_string(7)
#     }
#     create_response = client.post("/credit/", json=incomplete_credit_data)
#     assert create_response.status_code == 400

#     # Try to delete a non-existent credit
#     delete_response = client.post("/credit/", json=credit_filter)
#     assert delete_response.status_code == 404


# def test_user_error_cases():
#     # Try to fetch a non-existent user
#     user_filter = {"user_id": 999999}
#     search_response = client.post("/user/search/", json=user_filter)
#     assert search_response.status_code == 404

#     # Try to create a user with missing data
#     incomplete_user_data = {
#         "user_id": random.randint(1, 10000)
#     }
#     create_response = client.post("/user/", json=incomplete_user_data)
#     assert create_response.status_code == 400

#     # Try to delete a non-existent user
#     delete_response = client.post("/user/", json=user_filter)
#     assert delete_response.status_code == 404


# def test_view_session_error_cases():
#     # Try to fetch a non-existent view session
#     view_session_filter = {"start_timestamp": "2999-01-01 00:00:00"}
#     search_response = client.post("/view_session/search/", json=view_session_filter)
#     assert search_response.status_code == 404

#     # Try to create a view session with missing data
#     incomplete_view_session_data = {
#         "start_timestamp": random_date(datetime(2022, 1, 1), datetime(2023, 1, 1))
#     }
#     create_response = client.post("/view_session/", json=incomplete_view_session_data)
#     assert create_response.status_code == 400

#     # Try to delete a non-existent view session
#     delete_response = client.post("/view_session/", json=view_session_filter)
#     assert delete_response.status_code == 404
