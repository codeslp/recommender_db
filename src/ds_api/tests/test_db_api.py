import pytest
from src.ds_api.db_api import query_db

def test_titles_table_exists(test_db):
    result, _ = query_db("SELECT EXISTS (SELECT FROM relational.tables WHERE table_name='titles');", False)
    assert result[0][0] == True

def test_genres_table_exists(test_db):
    result, _ = query_db("SELECT EXISTS (SELECT FROM relational.tables WHERE table_name='genres');", False)
    assert result[0][0] == True

def test_prod_countries_table_exists(test_db):
    result, _ = query_db("SELECT EXISTS (SELECT FROM relational.tables WHERE table_name='prod_countries');", False)
    assert result[0][0] == True

def test_credits_table_exists(test_db):
    result, _ = query_db("SELECT EXISTS (SELECT FROM relational.tables WHERE table_name='credits');", False)
    assert result[0][0] == True

def test_users_table_exists(test_db):
    result, _ = query_db("SELECT EXISTS (SELECT FROM relational.tables WHERE table_name='users');", False)
    assert result[0][0] == True

def test_sessions_table_exists(test_db):
    result, _ = query_db("SELECT EXISTS (SELECT FROM relational.tables WHERE table_name='sessions');", False)
    assert result[0][0] == True

def test_content_type_constraint(test_db):
    with pytest.raises(Exception):
        query_db("INSERT INTO relational.titles (content_id, title, content_type, release_year) VALUES ('invalid12345', 'Invalid Movie', 'invalid', 2020);", False)

def test_subscription_type_constraint(test_db):
    with pytest.raises(Exception):
        query_db("INSERT INTO relational.users (user_id, subscription_type) VALUES (99999, 'invalid');", False)

# Additional tests for CRUD operations, relationships, etc., should be added as needed.
