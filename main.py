from fastapi import FastAPI, HTTPException
import requests
import csv
from datetime import datetime, timedelta

app = FastAPI()

# Define the API key and request parameters
API_KEY = "a3a4713a36f51b0dcc8dd1862e28e69d"
lat = 53.3498  # Latitude of Dublin
lon = -6.2603  # Longitude of Dublin

@app.post("/weather/")
async def fetch_and_store_weather():
    csv_filename = "dublin_historical_weather2.csv"
    fields = ['Time', 'Temperature', 'Humidity', 'Weather Description', 'Pressure', 'Wind Speed', 'Wind Direction', 'Cloudiness']

    # Initialize the CSV file and write field names
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(fields)

    data_count = 0  # Initialize data counter
    days_back = 0  # Start from one day ago
    max_days_back = 365  # Set a maximum look-back period, such as one year

    # Loop through requests until at least 1000 rows of data are collected
    while data_count < 1000 and days_back < max_days_back:
        end = int((datetime.now() - timedelta(days=days_back)).timestamp())
        start = int((datetime.now() - timedelta(days=days_back + 1)).timestamp())

        # Construct the API request URL
        url = f"https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&type=hour&start={start}&end={end}&appid={API_KEY}"

        # Make the request
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'list' in data:
                with open(csv_filename, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    for item in data['list']:
                        # Extract all relevant weather data
                        time = datetime.fromtimestamp(item['dt']).strftime('%Y-%m-%d %H:%M:%S')
                        temperature = item['main'].get('temp')
                        humidity = item['main'].get('humidity')
                        pressure = item['main'].get('pressure')
                        wind_speed = item['wind'].get('speed')
                        wind_deg = item['wind'].get('deg')
                        clouds = item['clouds'].get('all')
                        description = item['weather'][0]['description'] if item['weather'] else ''
                        # Write to the CSV file
                        writer.writerow([time, temperature, humidity, description, pressure, wind_speed, wind_deg, clouds])
                        data_count += 1
                        if data_count >= 1000:
                            break
                print(f"Added data for {datetime.fromtimestamp(end).strftime('%Y-%m-%d')}, total entries: {data_count}")
            else:
                print(f"No data for {datetime.fromtimestamp(end).strftime('%Y-%m-%d')}")
        else:
            print(f"Failed to retrieve data for {datetime.fromtimestamp(end).strftime('%Y-%m-%d')}: {response.status_code}")
            raise HTTPException(status_code=500, detail="Failed to fetch data from the API")

        days_back += 1  # Move the window back one day

    return {"status": "Data retrieval complete", "total_entries": data_count}
