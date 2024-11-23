from fastapi import FastAPI, HTTPException, Query
import os
from dotenv import load_dotenv
from service.weatherService import fetch_and_store_weather  # 导入刚创建的函数
from service.weatherService import get_weather_data
from service.sunService import fetch_and_store_sun_data, get_sun_data
from datetime import datetime
from service.lightService import store_light_data, retrieve_light_data

app = FastAPI()

# Load environment variables
load_dotenv()

# Read the API key and request parameters from environment variables
API_KEY = os.getenv("API_KEY")
lat = float(os.getenv("LATITUDE", 53.3498))
lon = float(os.getenv("LONGITUDE", -6.2603))


@app.post("/weather/")
async def weather_endpoint():
    try:
        result = fetch_and_store_weather(API_KEY, lat, lon)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/weather/")
async def get_weather(
    page: int = Query(default=1, ge=1), per_page: int = Query(default=10, ge=1)
):
    try:
        weather_data = get_weather_data(page, per_page)
        if not weather_data:
            raise HTTPException(status_code=404, detail="No weather data found.")
        return weather_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sun/")
async def store_sun_data():
    start_date = datetime.strptime("2024-06-20", "%Y-%m-%d")
    end_date = datetime.strptime("2024-12-21", "%Y-%m-%d")
    try:
        fetch_and_store_sun_data(start_date, end_date, lat, lon)
        return {"message": "Sun data stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sun/")
async def retrieve_sun_data(
    page: int = Query(default=1, ge=1), per_page: int = Query(default=10, ge=1)
):
    try:
        sun_data = get_sun_data(page, per_page)
        if not sun_data:
            raise HTTPException(status_code=404, detail="No sun data found.")
        return sun_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/light/")
async def store_light():
    try:
        store_light_data()
        return {"message": "Light data stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/light/")
async def get_light(
    page: int = Query(default=1, ge=1), per_page: int = Query(default=10, ge=1)
):
    try:
        light_data = retrieve_light_data(page, per_page)
        if not light_data:
            raise HTTPException(status_code=404, detail="No light data found.")
        return light_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
