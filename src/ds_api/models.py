import os
import logging

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from datetime import date
from enum import Enum
from pydantic import validator
from sqlalchemy import PrimaryKeyConstraint, ForeignKeyConstraint
from sqlmodel import Field, SQLModel, Session, Relationship
from typing import Optional, List

class Titles(SQLModel, table=True):
    __tablename__ = "titles"
    __table_args__ = {"schema": "relational"}
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
    genres: List["Genres"] = Relationship(back_populates="titles")
    prod_countries: List["ProdCountries"] = Relationship(back_populates="titles")
    credits: List["Credits"] = Relationship(back_populates="titles")
    sessions: List["ViewSessions"] = Relationship(back_populates="titles")
    recommendations: List["Recommendations"] = Relationship(back_populates="titles")

    @validator("content_type", pre=True, always=True)
    def set_content_type_to_lower(cls, v: str) -> str:
        return v.lower()

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

class Genres(SQLModel, table=True):
    __tablename__ = "genres"
    __table_args__ = (PrimaryKeyConstraint('content_id', 'genre'), {'schema': 'relational'})
    content_id: str = Field(foreign_key="relational.titles.content_id")
    genre: Optional[str] = Field(max_length=20)
    is_main_genre: bool
    titles: "Titles" = Relationship(back_populates="genres")


class GenreFilter(SQLModel):
    content_id: Optional[str] = Field(max_length=10)
    genre: Optional[str] = Field(max_length=20)
    is_main_genre: Optional[bool]

class ProdCountries(SQLModel, table=True):
    __tablename__ = "prod_countries"
    content_id: str = Field(foreign_key="relational.titles.content_id")
    country: str = Field(max_length=20)
    is_main_country: bool
    __table_args__ = (PrimaryKeyConstraint('content_id', 'country'), {'schema': 'relational'})
    titles: "Titles" = Relationship(back_populates="prod_countries")


class ProdCountryFilter(SQLModel):
    content_id: Optional[str] = Field(max_length=10)
    country: Optional[str] = Field(max_length=20)
    is_main_country: Optional[bool]

class Credits(SQLModel, table=True):
    __table_name__ = "credits"
    content_id: str = Field(foreign_key="relational.titles.content_id")
    person_id: str = Field(max_length=7)
    first_name: str = Field(max_length=35)
    middle_name: Optional[str] = Field(max_length=35)
    last_name: str = Field(max_length=40)
    character: str = Field(max_length=400)
    role: str = Field(max_length=15)
    __table_args__ = (PrimaryKeyConstraint('content_id', 'person_id', 'first_name', 'last_name', 'character', 'role'), {'schema': 'relational'})
    titles: "Titles" = Relationship(back_populates="credits")

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

class Users(SQLModel, table=True):
    __tablename__ = "users"
    __table_args__ = {'schema': 'relational'}
    user_id: int = Field(primary_key=True)
    birth_date: Optional[date]
    subscription_date: Optional[date]
    subscription_type: SubscriptionType
    sessions: List["ViewSessions"] = Relationship(back_populates="users")
    recommendations: List["Recommendations"] = Relationship(back_populates="users")


class UserFilter(SQLModel):
    user_id: Optional[int]
    birth_date: Optional[date]
    subscription_date: Optional[date]
    subscription_type: Optional[SubscriptionType]

class ViewSessions(SQLModel, table=True):
    __tablename__ = "sessions"
    start_timestamp: date
    end_timestamp: date
    content_id: str = Field(foreign_key="relational.titles.content_id")
    user_id: int = Field(foreign_key="relational.users.user_id")
    user_rating: Optional[int]
    __table_args__ = (PrimaryKeyConstraint('start_timestamp', 'end_timestamp', 'content_id', 'user_id'), {'schema': 'relational'})
    users: "Users" = Relationship(back_populates="sessions")
    titles: "Titles" = Relationship(back_populates="sessions")

class ViewSessionFilter(SQLModel):
    start_timestamp: Optional[date]
    end_timestamp: Optional[date]
    content_id: Optional[str] = Field(max_length=10)
    user_id: Optional[int]
    user_rating: Optional[int]

class Recommendations(SQLModel):
    content_id: str = Field(foreign_key="relational.titles.content_id")
    user_id: int = Field(foreign_key="relational.users.user_id")
    __table_args__ = (PrimaryKeyConstraint('content_id', 'user_id'), {'schema': 'relational'})
    users: "Users" = Relationship(back_populates="recommendations")
    titles: "Titles" = Relationship(back_populates="recommendations")