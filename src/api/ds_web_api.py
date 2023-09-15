import os
import logging

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, OperationalError, StatementError, DataError
from typing import Optional, List

from models import (Titles, Genres, ProdCountries, Credits, Users, ViewSessions, TitleFilter,
                    GenreFilter, CreditFilter, UserFilter, ProdCountryFilter, ViewSessionFilter)

load_dotenv()

# Logging setup.
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s]',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Initialize FastAPI app.
app = FastAPI()

# Set up database connection.
db_url = f"postgresql+psycopg2://{os.getenv('DS_USER')}:{os.getenv('DS_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

def set_search_path(dbapi_connection, connection_record):
    """
    Set the search path for the current connection to use the 'relational' schema.
    
    Args:
        dbapi_connection: The current raw database connection.
        connection_record: The connection record associated with the connection.
    """
    cursor = dbapi_connection.cursor()
    cursor.execute("SET search_path TO relational;")
    cursor.close()

event.listen(engine, "connect", set_search_path)

@app.exception_handler(IntegrityError)
def handle_integrity_error(request, exc):
    """
    Handle IntegrityError exceptions raised during request processing.

    Args:
        request: The request that caused the exception.
        exc: The exception instance.
    
    Returns:
        HTTPException with status code and details about the error.
    """
    logger.error(f"Integrity Error: {exc}")
    raise HTTPException(status_code=400, detail=f"This entry may already exist or violate a constraint: {exc}")

@app.exception_handler(OperationalError)
def handle_operational_error(request, exc):
    """
    Handle OperationalError exceptions raised during request processing.

    Args:
        request: The request that caused the exception.
        exc: The exception instance.
    
    Returns:
        HTTPException with status code and details about the error.
    """
    logger.error(f"Operational Error: {exc}")
    raise HTTPException(status_code=500, detail=f"An issue with the database operation occurred: {exc}")

@app.exception_handler(StatementError)
def handle_statement_error(request, exc):
    """
    Handle StatementError exceptions raised during request processing.

    Args:
        request: The request that caused the exception.
        exc: The exception instance.
    
    Returns:
        HTTPException with status code and details about the error.
    """
    logger.error(f"Statement Error: {exc}")
    raise HTTPException(status_code=400, detail=f"SQL statement issue: {exc}")

@app.exception_handler(DataError)
def handle_data_error(request, exc):
    """
    Handle DataError exceptions raised during request processing.

    Args:
        request: The request that caused the exception.
        exc: The exception instance.
    
    Returns:
        HTTPException with status code and details about the error.
    """
    logger.error(f"Data Error: {exc}")
    raise HTTPException(status_code=400, detail=f"Issue with the processed data: {exc}")

@app.exception_handler(RequestValidationError)
def handle_request_validation_error(request, exc):
    """
    Handle RequestValidationError exceptions raised during request processing.

    Args:
        request: The request that caused the exception.
        exc: The exception instance.
    
    Returns:
        HTTPException with status code and details about the error.
    """
    logger.error(f"Request Validation Error: {exc}")
    raise HTTPException(status_code=422, detail=f"Invalid request: {exc}")

"""
I have used POST instead of GET for the search filtering, because I don't 
want to expose the search parameters in the URL and delete endpoints won't
allow a JSON payload in the body.
"""

def get_session():
    """
    Generate a new SQLAlchemy session.
    
    Returns:
        session: An active SQLAlchemy session. The session is automatically closed after use.
    """
    session = Session(engine)
    try:
        yield session
    except:
        session.rollback()
        raise
    finally:
        session.close()

@app.post("/title/")
def create_title(titles: Titles, session: Session = Depends(get_session)):
    """
    Create a new title entry in the database.
    
    Args:
        titles (Titles): The title object to be added.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        dict: The created title entry, to be returned as JSON.
    """
    session.add(titles)
    session.commit()
    session.refresh(titles)
    return titles

@app.post("/title/delete/")
def delete_title(title_filter: TitleFilter, session: Session = Depends(get_session)):
    """
    Delete title entries based on provided filters.
    
    Args:
        title_filter (TitleFilter): Filters for the title entries to be deleted.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        dict: A message indicating how many title entries were deleted to be returned as JSON.
    """
    non_none_filter = {k: v for k, v in title_filter.dict().items() if v is not None}
    query = session.query(Titles).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in titles table found with provided filter: {title_filter.dict()}")
    for title in results:
        session.delete(title)
    session.commit()
    return {"message": f"{len(results)} title(s) deleted successfully."}

@app.post("/title/search/")
def search_title(title_filter: TitleFilter, session: Session = Depends(get_session)):
    """
    Search for title entries based on provided filters.
    
    Args:
        title_filter (TitleFilter): Filters for the title search.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        list: A list of title entries that match the provided filters to be returned as JSON.
    """
    non_none_filter = {k: v for k, v in title_filter.dict().items() if v is not None}
    query = session.query(Titles).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in titles table found with provided filter: {non_none_filter}")
    return results




@app.post("/genre/")
def create_genre(genre: Genres, session: Session = Depends(get_session)):
    """
    Create a new genre entry in the database.
    
    Args:
        genre (Genres): The genre object to be added.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        dict: The created genre entry to be returned as JSON.
    """
    session.add(genre)
    session.commit()
    session.refresh(genre)
    return genre

@app.post("/genre/delete/")
def delete_genre(genre_filter: GenreFilter, session: Session = Depends(get_session)):
    """
    Delete genre entries based on provided filters.
    
    Args:
        genre_filter (GenreFilter): Filters for the genre entries to be deleted.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        dict: A message indicating how many genre entries were deleted to be returned as JSON.
    """
    non_none_filter = {k: v for k, v in genre_filter.dict().items() if v is not None}
    query = session.query(Genres).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in genres table found with provided filter: {genre_filter.dict()}")
    for genre in results:
        session.delete(genre)
    session.commit()
    return {"message": f"{len(results)} genre(s) deleted successfully."}

@app.post("/genre/search/")
def search_genre(genre_filter: GenreFilter, session: Session = Depends(get_session)):
    """
    Search for genre entries based on provided filters.
    
    Args:
        genre_filter (GenreFilter): Filters for the genre search.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        list: A list of genre entries that match the provided filters to be returned as JSON.
    """
    non_none_filter = {k: v for k, v in genre_filter.dict().items() if v is not None}
    query = session.query(Titles).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in genres table found with provided filter: {genre_filter.dict()}")
    return results




@app.post("/prod_country/")
def create_prod_country(prod_country: ProdCountries, session: Session = Depends(get_session)):
    session.add(prod_country)
    session.commit()
    session.refresh(prod_country)
    return prod_country

@app.post("/prod_country/delete/")
def delete_prod_country(prod_country_filter: ProdCountryFilter, session: Session = Depends(get_session)):
    non_none_filter = {k: v for k, v in prod_country_filter.dict().items() if v is not None}
    query = session.query(ProdCountries).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in prod_countries table found with provided filter: {prod_country_filter.dict()}")
    for prod_country in results:
        session.delete(prod_country)
    session.commit()
    return {"message": f"{len(results)} prod_country(s) deleted successfully."}

@app.post("/prod_country/search/")
def search_prod_country(prod_country_filter: ProdCountryFilter, session: Session = Depends(get_session)):
    non_none_filter = {k: v for k, v in prod_country_filter.dict().items() if v is not None}
    query = session.query(ProdCountries).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in prod_countries table found with provided filter: {prod_country_filter.dict()}")
    return results



@app.post("/credit/")
def create_credit(credit: Credits, session: Session = Depends(get_session)):
    """
    Create a new credit entry in the database.
    
    Args:
        credit (Credits): The credit object to be added.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        dict: The created credit entry to be returned as JSON.
    """
    session.add(credit)
    session.commit()
    session.refresh(credit)
    return credit

@app.post("/credit/delete/")
def delete_credit(credit_filter: CreditFilter, session: Session = Depends(get_session)):
    """
    Delete credit entries based on provided filters.
    
    Args:
        credit_filter (CreditFilter): Filters for the credit entries to be deleted.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        dict: A message indicating how many credit entries were deleted to be returned as JSON.
    """
    non_none_filter = {k: v for k, v in credit_filter.dict().items() if v is not None}
    query = session.query(Credits).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in credits table found with provided filter: {credit_filter.dict()}")
    for credit in results:
        session.delete(credit)
    session.commit()
    return {"message": f"{len(results)} credit(s) deleted successfully."}

@app.post("/credit/search/")
def search_credit(credit_filter: CreditFilter, session: Session = Depends(get_session)):
    """
    Search for credit entries based on provided filters.
    
    Args:
        credit_filter (CreditFilter): Filters for the credit search.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        list: A list of credit entries that match the provided filters to be returned as JSON.
    """
    non_none_filter = {k: v for k, v in credit_filter.dict().items() if v is not None}
    query = session.query(Credits).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in credits table found with provided filter: {credit_filter.dict()}")
    return results




@app.post("/user/")
def create_user(user: Users, session: Session = Depends(get_session)):
    """
    Create a new user entry in the database.
    
    Args:
        user (Users): The user object to be added.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        dict: The created user entry to be returned as JSON.
    """
    session.add(user)
    session.commit()
    session.refresh(user)
    return user

@app.post("/user/delete/")
def delete_user(user_filter: UserFilter, session: Session = Depends(get_session)):
    """
    Delete user entries based on provided filters.
    
    Args:
        user_filter (UserFilter): Filters for the user entries to be deleted.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        dict: A message indicating how many user entries were deleted to be returned as JSON.
    """
    non_none_filter = {k: v for k, v in user_filter.dict().items() if v is not None}
    query = session.query(Users).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in users table found with provided filter: {user_filter.dict()}")
    for user in results:
        session.delete(user)
    session.commit()
    return {"message": f"{len(results)} user(s) deleted successfully."}

@app.post("/user/search/")
def search_user(user_filter: UserFilter, session: Session = Depends(get_session)):
    """
    Search for user entries based on provided filters.
    
    Args:
        user_filter (UserFilter): Filters for the user search.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        list: A list of user entries that match the provided filters to be returned as JSON.
    """
    non_none_filter = {k: v for k, v in user_filter.dict().items() if v is not None}
    query = session.query(Users).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in users table found with provided filter: {user_filter.dict()}")
    return results




@app.post("/view_session/")
def create_view_session(view_session: ViewSessions, session: Session = Depends(get_session)):
    """
    Create a new view session entry in the database.
    
    Args:
        view_session (ViewSessions): The view session object to be added.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        dict: The created view session entry to be returned as JSON.
    """
    session.add(view_session)
    session.commit()
    session.refresh(view_session)
    return view_session

@app.post("/view_session/delete/")
def delete_view_session(view_session_filter: ViewSessionFilter, session: Session = Depends(get_session)):
    """
    Delete view session entries based on provided filters.
    
    Args:
        view_session_filter (ViewSessionFilter): Filters for the view session entries to be deleted.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        dict: A message indicating how many view session entries were deleted to be returned as JSON.
    """
    non_none_filter = {k: v for k, v in view_session_filter.dict().items() if v is not None}
    query = session.query(ViewSessions).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in view_sessions table found with provided filter: {view_session_filter.dict()}")
    for view_session in results:
        session.delete(view_session)
    session.commit()
    return {"message": f"{len(results)} view_session(s) deleted successfully."}

@app.post("/view_session/search/")
def search_view_session(view_session_filter: ViewSessionFilter, session: Session = Depends(get_session)):
    """
    Search for view session entries based on provided filters.
    
    Args:
        view_session_filter (ViewSessionFilter): Filters for the view session search.
        session (Session): An active SQLAlchemy session.
    
    Returns:
        list: A list of view session entries that match the provided filters to be returned as JSON.
    """
    non_none_filter = {k: v for k, v in view_session_filter.dict().items() if v is not None}
    query = session.query(ViewSessions).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in view_sessions table found with provided filter: {view_session_filter.dict()}")
    return results


@app.on_event("startup")
async def startup_event():
    logger.info("FastAPI application started")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("FastAPI application stopped")