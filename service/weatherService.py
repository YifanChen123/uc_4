import logging
from datetime import datetime, timedelta
import requests
from sqlmodel import Session, text
from model import Weather, create_db_and_tables, engine
from sqlalchemy.inspection import inspect

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_and_store_weather(API_KEY, lat, lon):
    with Session(engine) as session:
        # 查询是否需要创建表
        if not inspect(engine).has_table("weather"):  # 使用inspect方法检查表是否存在
            create_db_and_tables()

        # 清空表中数据以保持只有最新数据
        session.execute(text("DELETE FROM weather"))
        # id序列编号从1开始
        session.execute(text("ALTER SEQUENCE weather_id_seq RESTART WITH 1"))
        session.commit()  # 确保删除操作在添加新数据前完成

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
                    for item in data["list"]:
                        weather = Weather(
                            time=datetime.fromtimestamp(item["dt"]),
                            temperature=item["main"].get("temp"),
                            humidity=item["main"].get("humidity"),
                            pressure=item["main"].get("pressure"),
                            wind_speed=item["wind"].get("speed"),
                            wind_direction=item["wind"].get("deg"),
                            cloudiness=item["clouds"].get("all"),
                            description=(
                                item["weather"][0]["description"]
                                if item["weather"]
                                else ""
                            ),
                        )
                        session.add(weather)
                        data_count += 1
                        if data_count >= 1000:
                            break
                    session.commit()
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
