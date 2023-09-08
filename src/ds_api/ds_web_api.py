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

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s]',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


app = FastAPI()


db_url = f"postgresql+psycopg2://{os.getenv('DS_USER')}:{os.getenv('DS_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

def set_search_path(dbapi_connection, connection_record):
    """
    Set the search path for the current connection to use the 'relational' schema.
    """
    cursor = dbapi_connection.cursor()
    cursor.execute("SET search_path TO relational;")
    cursor.close()

event.listen(engine, "connect", set_search_path)

@app.exception_handler(IntegrityError)
def handle_integrity_error(request, exc):
    logger.error(f"Integrity Error: {exc}")
    raise HTTPException(status_code=400, detail=f"This entry may already exist or violate a constraint: {exc}")

@app.exception_handler(OperationalError)
def handle_operational_error(request, exc):
    logger.error(f"Operational Error: {exc}")
    raise HTTPException(status_code=500, detail=f"An issue with the database operation occurred: {exc}")

@app.exception_handler(StatementError)
def handle_statement_error(request, exc):
    logger.error(f"Statement Error: {exc}")
    raise HTTPException(status_code=400, detail=f"SQL statement issue: {exc}")

@app.exception_handler(DataError)
def handle_data_error(request, exc):
    logger.error(f"Data Error: {exc}")
    raise HTTPException(status_code=400, detail=f"Issue with the processed data: {exc}")

@app.exception_handler(RequestValidationError)
def handle_request_validation_error(request, exc):
    logger.error(f"Request Validation Error: {exc}")
    raise HTTPException(status_code=422, detail=f"Invalid request: {exc}")



"""
I have used POST instead of GET for the search filtering, because I don't 
want to expose the search parameters in the URL and I don't want to cache
the results of the search.
"""

def get_session():
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
    session.add(titles)
    session.commit()
    session.refresh(titles)
    return titles

@app.post("/title/delete/")
def delete_title(title_filter: TitleFilter, session: Session = Depends(get_session)):
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
    non_none_filter = {k: v for k, v in title_filter.dict().items() if v is not None}
    query = session.query(Titles).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in titles table found with provided filter: {non_none_filter}")
    return results



@app.post("/genre/")
def create_genre(genre: Genres, session: Session = Depends(get_session)):
    session.add(genre)
    session.commit()
    session.refresh(genre)
    return genre

@app.post("/genre/delete/")
def delete_genre(genre_filter: GenreFilter, session: Session = Depends(get_session)):
    non_none_filter = {k: v for k, v in genre_filter.dict().items() if v is not None}
    query = session.query(Titles).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in genres table found with provided filter: {genre_filter.dict()}")
    for genre in results:
        session.delete(genre)
    session.commit()
    return {"message": f"{len(results)} genre(s) deleted successfully."}

@app.post("/genre/search/")
def search_genre(genre_filter: GenreFilter, session: Session = Depends(get_session)):
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
    session.add(credit)
    session.commit()
    session.refresh(credit)
    return credit

@app.post("/credit/delete/")
def delete_credit(credit_filter: CreditFilter, session: Session = Depends(get_session)):
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
    non_none_filter = {k: v for k, v in credit_filter.dict().items() if v is not None}
    query = session.query(Credits).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in credits table found with provided filter: {credit_filter.dict()}")
    return results



@app.post("/user/")
def create_user(user: Users, session: Session = Depends(get_session)):
    session.add(user)
    session.commit()
    session.refresh(user)
    return user

@app.post("/user/delete/")
def delete_user(user_filter: UserFilter, session: Session = Depends(get_session)):
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
    non_none_filter = {k: v for k, v in user_filter.dict().items() if v is not None}
    query = session.query(Users).filter_by(**non_none_filter)
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in users table found with provided filter: {user_filter.dict()}")
    return results



@app.post("/view_session/")
def create_view_session(view_session: ViewSessions, session: Session = Depends(get_session)):
    session.add(view_session)
    session.commit()
    session.refresh(view_session)
    return view_session

@app.post("/view_session/delete/")
def delete_view_session(view_session_filter: ViewSessionFilter, session: Session = Depends(get_session)):
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