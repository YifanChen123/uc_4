from sqlmodel import SQLModel, Field, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.inspection import inspect
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve environment variables
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_SERVER = os.getenv("POSTGRES_SERVER")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Construct DATABASE_URL from environment variables
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Create database engine
engine = create_engine(DATABASE_URL)


# Define SQLModel class
class Weather(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    time: datetime = Field(index=True)
    temperature: float
    humidity: int
    description: Optional[str]
    pressure: int
    wind_speed: float
    wind_direction: int
    cloudiness: int


# Define SQLModel class for Sun
class Sun(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    date: str
    sunrise: str
    sunset: str


# Define SQLModel class for Light
class Light(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    date: str
    light_off: datetime
    light_on: datetime


# Function to create weather table
def create_weather_table():
    if not inspect(engine).has_table("weather"):
        SQLModel.metadata.create_all(engine)


# Function to create sun table
def create_sun_table():
    if not inspect(engine).has_table("sun"):
        SQLModel.metadata.create_all(engine)


# Function to create light table
def create_light_table():
    if not inspect(engine).has_table("light"):
        SQLModel.metadata.create_all(engine)
