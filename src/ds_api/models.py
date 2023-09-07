import os
import logging

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from datetime import date
from enum import Enum
from sqlalchemy import PrimaryKeyConstraint, ForeignKeyConstraint
from sqlmodel import SQLModel, Field
from typing import Optional

class Title(SQLModel, table=True):
    content_id: str = Field(primary_key=True, max_length=10)
    title: Optional[str] = Field(max_length=200)
    content_type: str = Field(max_length=5)
    release_year: Optional[int]
    age_certification: Optional[str] = Field(max_length=10)
    runtime: Optional[int]
    number_of_seasons: Optional[int]
    imdb_id: Optional[str] = Field(max_length=15)
    imdb_score: Optional[float]
    imdb_votes: Optional[int]
    is_year_best: Optional[bool]
    is_all_time_best: Optional[bool]

class TitleFilter(SQLModel):
    content_id: Optional[str] = Field(max_length=10)
    title: Optional[str] = Field(max_length=200)
    content_type: Optional[str] = Field(max_length=5)
    release_year: Optional[int]
    age_certification: Optional[str] = Field(max_length=10)
    runtime: Optional[int]
    number_of_seasons: Optional[int]
    imdb_id: Optional[str] = Field(max_length=15)
    imdb_score: Optional[float]
    imdb_votes: Optional[int]
    is_year_best: Optional[bool]
    is_all_time_best: Optional[bool]

class Genre(SQLModel, table=True):
    content_id: str = Field(foreign_key="titles.content_id")
    genre: Optional[str] = Field(max_length=20)
    is_main_genre: bool

    __table_args__ = (PrimaryKeyConstraint('content_id', 'genre'),)

class GenreFilter(SQLModel):
    content_id: Optional[str] = Field(max_length=10)
    genre: Optional[str] = Field(max_length=20)
    is_main_genre: Optional[bool]

class ProdCountry(SQLModel, table=True):
    content_id: str = Field(foreign_key="titles.content_id")
    country: str = Field(max_length=20)
    is_main_country: bool

    __table_args__ = (PrimaryKeyConstraint('content_id', 'country'),)

class ProdCountryFilter(SQLModel):
    content_id: Optional[str] = Field(max_length=10)
    country: Optional[str] = Field(max_length=20)
    is_main_country: Optional[bool]

class Credit(SQLModel, table=True):
    content_id: str = Field(foreign_key="titles.content_id")
    person_id: str = Field(max_length=7)
    first_name: str = Field(max_length=35)
    middle_name: Optional[str] = Field(max_length=35)
    last_name: str = Field(max_length=40)
    character: str = Field(max_length=400)
    role: str = Field(max_length=15)

    __table_args__ = (PrimaryKeyConstraint('content_id', 'person_id', 'first_name', 'last_name', 'character', 'role'),)

class CreditFilter(SQLModel):
    content_id: Optional[str] = Field(max_length=10)
    person_id: Optional[str] = Field(max_length=7)
    first_name: Optional[str] = Field(max_length=35)
    middle_name: Optional[str] = Field(max_length=35)
    last_name: Optional[str] = Field(max_length=40)
    character: Optional[str] = Field(max_length=400)
    role: Optional[str] = Field(max_length=15)

class SubscriptionType(str, Enum):
    basic = "basic"
    standard = "standard"
    premium = "premium"

class User(SQLModel, table=True):
    user_id: int = Field(primary_key=True)
    birth_date: Optional[date]
    subscription_date: Optional[date]
    subscription_type: SubscriptionType

class UserFilter(SQLModel):
    user_id: Optional[int]
    birth_date: Optional[date]
    subscription_date: Optional[date]
    subscription_type: Optional[SubscriptionType]

class ViewSession(SQLModel, table=True):
    start_timestamp: date
    end_timestamp: date
    content_id: str = Field(foreign_key="titles.content_id")
    user_id: int = Field(foreign_key="users.user_id")
    user_rating: Optional[int]

    __table_args__ = (PrimaryKeyConstraint('start_timestamp', 'end_timestamp', 'content_id', 'user_id'),)

class ViewSessionFilter(SQLModel):
    start_timestamp: Optional[date]
    end_timestamp: Optional[date]
    content_id: Optional[str] = Field(max_length=10)
    user_id: Optional[int]
    user_rating: Optional[int]