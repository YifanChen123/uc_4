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


# Create database engine
engine = create_engine(DATABASE_URL)


# Function to create tables
def create_db_and_tables():
    global engine
    if not inspect(engine).has_table("weather"):
        SQLModel.metadata.create_all(engine)
