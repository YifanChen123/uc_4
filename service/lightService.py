from datetime import timedelta
from sqlmodel import Session, select, text
from model import Light, Sun, create_light_table, engine
from sqlalchemy.exc import NoResultFound
import logging
from sqlalchemy.inspection import inspect
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def store_light_data():
    with Session(engine) as session:
        if not inspect(engine).has_table("light"):
            create_light_table()

        session.execute(text("DELETE FROM light"))
        session.execute(text("ALTER SEQUENCE light_id_seq RESTART WITH 1"))
        session.commit()

        sun_data = session.exec(select(Sun)).all()
        if not sun_data:
            raise NoResultFound("No sun data available to compute light data.")

        light_data = [
            Light(
                date=sun.date,
                light_off=datetime.fromisoformat(sun.sunrise) + timedelta(minutes=15),
                light_on=datetime.fromisoformat(sun.sunset) - timedelta(minutes=15),
            )
            for sun in sun_data
        ]
        session.add_all(light_data)
        session.commit()
        logging.info("Light data stored successfully")


def retrieve_light_data(page, per_page):
    with Session(engine) as session:
        if not inspect(engine).has_table("light"):
            raise Exception("Light table does not exist.")

        offset = (page - 1) * per_page
        # 使用 order_by() 以 date 字段逆序排序，确保日期从最新到最旧
        statement = (
            select(Light).order_by(Light.date.desc()).offset(offset).limit(per_page)
        )
        results = session.exec(statement).all()
        if not results:
            raise NoResultFound("No light data available on this page.")

        return results
