import requests
from datetime import datetime, timedelta
from sqlmodel import Session, text, select
from model import Sun, create_sun_table, engine
from sqlalchemy.inspection import inspect
import logging
from sqlalchemy.exc import NoResultFound

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_and_store_sun_data(start_date, end_date, lat, lng):
    with Session(engine) as session:
        # 查询是否需要创建表
        if not inspect(engine).has_table("sun"):  # 使用inspect方法检查表是否存在
            create_sun_table()

        # Clear the existing data
        session.execute(text("DELETE FROM sun"))
        session.execute(text("ALTER SEQUENCE sun_id_seq RESTART WITH 1"))
        session.commit()

        current_date = start_date
        while current_date <= end_date:
            date = current_date.strftime("%Y-%m-%d")
            data = get_sunrise_sunset(date, lat, lng)
            sunrise = datetime.fromisoformat(data["sunrise"]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            sunset = datetime.fromisoformat(data["sunset"]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            sun_entry = Sun(date=date, sunrise=sunrise, sunset=sunset)
            session.add(sun_entry)
            logging.info(
                f"Storing data for {date}: Sunrise at {sunrise}, Sunset at {sunset}"
            )
            current_date += timedelta(days=1)
        session.commit()


def get_sunrise_sunset(date, lat, lng):
    url = f"https://api.sunrise-sunset.org/json?lat={lat}&lng={lng}&date={date}&formatted=0"
    response = requests.get(url)
    return response.json()["results"]


def get_sun_data(page, per_page):
    with Session(engine) as session:
        if not inspect(engine).has_table("sun"):
            raise Exception("Sun table does not exist.")

        offset = (page - 1) * per_page
        # 使用 order_by() 以 date 字段逆序排序，确保日期从最新到最旧
        statement = select(Sun).order_by(Sun.date.desc()).offset(offset).limit(per_page)
        results = session.exec(statement).all()
        if not results:
            raise NoResultFound("No sun data available on this page.")

        return results
