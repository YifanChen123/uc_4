from fastapi import FastAPI, HTTPException, Query
import os
from dotenv import load_dotenv
from service.weatherService import fetch_and_store_weather  # 导入刚创建的函数
from service.weatherService import get_weather_data

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
