from fastapi import FastAPI, HTTPException
import os
from dotenv import load_dotenv
from service.weatherService import fetch_and_store_weather  # 导入刚创建的函数

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
