import logging
from datetime import datetime, timedelta
import requests
import csv
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_and_store_weather(API_KEY, lat, lon):
    os.makedirs("data", exist_ok=True)
    csv_filename = "data/dublin_historical_weather2.csv"
    fields = [
        "Time",
        "Temperature",
        "Humidity",
        "Weather Description",
        "Pressure",
        "Wind Speed",
        "Wind Direction",
        "Cloudiness",
    ]

    with open(csv_filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(fields)

    data_count = 0
    days_back = 0
    max_days_back = 365

    while data_count < 1000 and days_back < max_days_back:
        end = int((datetime.now() - timedelta(days=days_back)).timestamp())
        start = int((datetime.now() - timedelta(days=days_back + 1)).timestamp())
        url = f"https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&type=hour&start={start}&end={end}&appid={API_KEY}"

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if "list" in data:
                with open(csv_filename, mode="a", newline="") as file:
                    writer = csv.writer(file)
                    for item in data["list"]:
                        time = datetime.fromtimestamp(item["dt"]).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        temperature = item["main"].get("temp")
                        humidity = item["main"].get("humidity")
                        pressure = item["main"].get("pressure")
                        wind_speed = item["wind"].get("speed")
                        wind_deg = item["wind"].get("deg")
                        clouds = item["clouds"].get("all")
                        description = (
                            item["weather"][0]["description"] if item["weather"] else ""
                        )
                        writer.writerow(
                            [
                                time,
                                temperature,
                                humidity,
                                description,
                                pressure,
                                wind_speed,
                                wind_deg,
                                clouds,
                            ]
                        )
                        data_count += 1
                        if data_count >= 1000:
                            break
                logging.info(
                    f"Added data for {datetime.fromtimestamp(end).strftime('%Y-%m-%d')}, total entries: {data_count}"
                )
            else:
                logging.info(
                    f"No data for {datetime.fromtimestamp(end).strftime('%Y-%m-%d')}"
                )
        else:
            logging.error(
                f"Failed to retrieve data for {datetime.fromtimestamp(end).strftime('%Y-%m-%d')}: {response.status_code}"
            )
            raise Exception("Failed to fetch data from the API")

        days_back += 1

    return {"status": "Data retrieval complete", "total_entries": data_count}
