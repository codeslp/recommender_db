import os
import logging

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, OperationalError, StatementError, DataError
from typing import Optional, List

from models import (Titles, Genre, ProdCountry, Credit, User, ViewSession, TitleFilter,
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
    return HTTPException(status_code=400, detail=f"This entry may already exist or violate a constraint: {exc}")

@app.exception_handler(OperationalError)
def handle_operational_error(request, exc):
    logger.error(f"Operational Error: {exc}")
    return HTTPException(status_code=500, detail=f"An issue with the database operation occurred: {exc}")

@app.exception_handler(StatementError)
def handle_statement_error(request, exc):
    logger.error(f"Statement Error: {exc}")
    return HTTPException(status_code=400, detail=f"SQL statement issue: {exc}")

@app.exception_handler(DataError)
def handle_data_error(request, exc):
    logger.error(f"Data Error: {exc}")
    return HTTPException(status_code=400, detail=f"Issue with the processed data: {exc}")

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

@app.post("/titles/")
def create_title(titles: Titles, session: Session = Depends(get_session)):
    session.add(titles)
    session.commit()
    session.refresh(titles)
    return titles

@app.post("/title/search/")
def search_title(title_filter: TitleFilter, session: Session = Depends(get_session)):
    query = session.query(Title).filter_by(**title_filter.dict())
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in titles table found with provided filter: {title_filter.dict()}")
    return results

@app.post("/genre/")
def create_genre(genre: Genre, session: Session = Depends(get_session)):
    session.add(genre)
    session.commit()
    session.refresh(genre)
    return genre

@app.post("/genre/search/")
def search_genre(genre_filter: GenreFilter, session: Session = Depends(get_session)):
    query = session.query(Genre).filter_by(**genre_filter.dict())
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in genres table found with provided filter: {genre_filter.dict()}")
    return results

@app.post("/prod_country/")
def create_prod_country(prod_country: ProdCountry, session: Session = Depends(get_session)):
    session.add(prod_country)
    session.commit()
    session.refresh(prod_country)
    return prod_country

@app.post("/prod_country/search/")
def search_prod_country(prod_country_filter: ProdCountryFilter, session: Session = Depends(get_session)):
    query = session.query(ProdCountry).filter_by(**prod_country_filter.dict())
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in prod_countries table found with provided filter: {prod_country_filter.dict()}")
    return results


@app.post("/credit/")
def create_credit(credit: Credit, session: Session = Depends(get_session)):
    session.add(credit)
    session.commit()
    session.refresh(credit)
    return credit

@app.post("/credit/search/")
def search_credit(credit_filter: CreditFilter, session: Session = Depends(get_session)):
    query = session.query(Credit).filter_by(**credit_filter.dict())
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in credits table found with provided filter: {credit_filter.dict()}")
    return results

@app.post("/user/")
def create_user(user: User, session: Session = Depends(get_session)):
    session.add(user)
    session.commit()
    session.refresh(user)
    return user

@app.post("/user/search/")
def search_user(user_filter: UserFilter, session: Session = Depends(get_session)):
    query = session.query(User).filter_by(**user_filter.dict())
    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail=f"No entries in users table found with provided filter: {user_filter.dict()}")
    return results

@app.post("/view_session/")
def create_view_session(view_session: ViewSession, session: Session = Depends(get_session)):
    session.add(view_session)
    session.commit()
    session.refresh(view_session)
    return view_session

@app.post("/view_session/search/")
def search_view_session(view_session_filter: ViewSessionFilter, session: Session = Depends(get_session)):
    query = session.query(ViewSession).filter_by(**view_session_filter.dict())
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