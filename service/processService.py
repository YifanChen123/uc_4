from sqlmodel import Session, select, text
from model import Process, create_process_table, engine, Weather, Sun
import pandas as pd
from scipy import stats
import numpy as np
from sqlalchemy.inspection import inspect
from scipy import stats
from sqlalchemy.exc import NoResultFound


def load_and_clean_data(dataframe):
    # 显示数据的基本信息
    print(dataframe.info())
    print(dataframe.head())

    # 处理缺失值
    numeric_cols = dataframe.select_dtypes(include=["float64", "int64"]).columns
    for col in numeric_cols:
        median_value = dataframe[col].median()
        dataframe[col].fillna(median_value, inplace=True)

    categorical_cols = dataframe.select_dtypes(include=["object"]).columns
    for col in categorical_cols:
        mode_value = dataframe[col].mode()[0]
        dataframe[col].fillna(mode_value, inplace=True)

    # 去除异常值
    dataframe = dataframe[
        (np.abs(stats.zscore(dataframe[numeric_cols])) < 3).all(axis=1)
    ]

    # 删除重复项
    dataframe.drop_duplicates(inplace=True)

    # 转换日期时间格式
    date_cols = ["time", "sunrise", "sunset", "date"]
    for col in date_cols:
        if col in dataframe.columns:
            dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce")

    return dataframe


def fetch_process_data():
    with Session(engine) as session:
        if not inspect(engine).has_table("process"):
            create_process_table()
        else:
            session.execute(text("DELETE FROM process"))
            session.execute(text("ALTER SEQUENCE process_id_seq RESTART WITH 1"))
            session.commit()

        # 从数据库中读取数据并转换为DataFrame
        weather_query = select(
            Weather.time,
            Weather.temperature,
            Weather.humidity,
            Weather.description,
            Weather.pressure,
            Weather.wind_speed,
            Weather.wind_direction,
            Weather.cloudiness,
        )
        sun_query = select(Sun.date, Sun.sunrise, Sun.sunset)

        weather_data = pd.read_sql(weather_query, session.bind)
        sun_data = pd.read_sql(sun_query, session.bind)

        # 确保日期列被正确格式化为 datetime 类型
        weather_data["date"] = pd.to_datetime(weather_data["time"]).dt.date
        sun_data["date"] = pd.to_datetime(sun_data["date"]).dt.date

        # 合并数据框，仅保留需要的列
        merged_df = pd.merge(weather_data, sun_data, on="date", how="left")

        # 清洗数据
        cleaned_data = load_and_clean_data(merged_df)

        # 应用判断灯光逻辑
        cleaned_data["light"] = cleaned_data.apply(judge_light, axis=1)

        # 为清洗后的数据生成新的自增 id
        cleaned_data.reset_index(drop=True, inplace=True)
        cleaned_data.insert(0, "id", cleaned_data.index + 1)

        # 存储到数据库
        cleaned_data.to_sql(
            "process", con=session.bind, if_exists="append", index=False
        )


def judge_light(row):
    if pd.isna(row["time"]) or pd.isna(row["sunrise"]) or pd.isna(row["sunset"]):
        return False

    sunrise_plus_15 = row["sunrise"] + pd.Timedelta(minutes=15)
    sunset_minus_15 = row["sunset"] - pd.Timedelta(minutes=15)

    if not (sunrise_plus_15 <= row["time"] <= sunset_minus_15):
        return True

    if str(row["description"]).strip().lower() in [
        "fog",
        "mist",
        "heavy intensity rain",
    ]:
        return True

    return False


def get_process_data(page, per_page):
    with Session(engine) as session:
        if not inspect(engine).has_table("process"):
            raise Exception("Process table does not exist.")

        offset = (page - 1) * per_page
        statement = (
            select(Process).order_by(Process.time.desc()).offset(offset).limit(per_page)
        )
        results = session.exec(statement).all()
        if not results:
            raise NoResultFound("No process data available on this page.")

        return results
