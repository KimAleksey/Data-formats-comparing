import os
import uuid
import faker
import logging
import datetime

import pandas as pd

from pathlib import Path

__all__= [
    "generate_data",
    "generate_parquet",
    "generate_csv",
    "generate_compressed_csv",
    "generate_json",
    "read_csv",
    "read_parquet",
    "read_json",
    "measure_formats"
]

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


FILES = {
    "parquet": "data.parquet",
    "csv": "data.csv",
    "csv.gz": "data.csv.gz",
    "json": "data.json",
}


def generate_data(rows: int | None = 100) -> pd.DataFrame:
    """
    Генерирует заданное количество строк. Каждая строка - фэйковый пользователь.

    :param rows: Кол-во пользователей.
    :return: Pandas DataFrame.
    """
    fake = faker.Faker(locale="ru_RU")

    list_of_dicts = []

    logging.info("Start. Запуск процесса генерации пользователей.")
    for _ in range(rows):
        user = {
            "id": str(uuid.uuid4()),
            "created_at": fake.date_time_ad(
                start_datetime=datetime.datetime(2020, 1, 1),
                end_datetime=datetime.datetime(2026, 1, 1),
            ),
            "updated_at": fake.date_time_ad(
                start_datetime=datetime.datetime(2020, 1, 1),
                end_datetime=datetime.datetime(2026, 1, 1),
            ),
            "firstname": fake.first_name(),
            "lastname": fake.last_name(),
            "birthday": fake.date_time_ad(
                start_datetime=datetime.datetime(1945, 1, 1),
                end_datetime=datetime.datetime(2026, 1, 1),
            ),
            "email": fake.email(),
            "username": fake.user_name(),
            "password": fake.password(),
            "phonenumber": fake.phone_number(),
            "country": fake.country(),
            "city": fake.city(),
        }

        list_of_dicts.append(user)

    logging.info(f"End. Сгенерировано {rows} пользователей.")
    df = pd.DataFrame(list_of_dicts)

    return df


def generate_parquet(df: pd.DataFrame, filename: str | None = FILES["parquet"]) -> None:
    """
    Загружает данные в формат parquet.

    :param df: Данные в формате pd.DataFrame.
    :param filename: Названия файла.
    :return: None.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Data should be pandas.DataFrame.")
    logging.info(f"Start. Запись данных в файл {filename}.")
    parquet_path = Path(__file__).parent.parent / "data" / filename
    df.to_parquet(path=parquet_path)
    logging.info(f"End. Файл {filename} записан.")


def read_parquet(filename: str | None = FILES["parquet"]) -> pd.DataFrame:
    """
    Считывает parquet файл и возвращает в виде pandas.DataFrame.

    :param filename: Названия файла.
    :return: Pandas DataFrame.
    """
    logging.info(f"Start. Чтение файла {filename}.")
    parquet_path = Path(__file__).parent.parent / "data" / filename
    df = pd.read_parquet(path=parquet_path)
    logging.info(f"End. файл {filename} успешно прочитан.")

    return df


def generate_csv(df: pd.DataFrame, filename: str | None = FILES["csv"]) -> None:
    """
    Загружает данные в формат csv.

    :param df: Данные в формате pd.DataFrame.
    :param filename: Названия файла.
    :return: None.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Data should be pandas.DataFrame.")
    logging.info(f"Start. Запись данных в файл {filename}.")
    csv_path = Path(__file__).parent.parent / "data" / filename
    df.to_csv(path_or_buf=csv_path, index=False)
    logging.info(f"End. Файл {filename} записан.")


def generate_compressed_csv(df: pd.DataFrame, filename: str | None = FILES["csv.gz"]) -> None:
    """
    Загружает данные в формат csv.gz

    :param df: Данные в формате pd.DataFrame.
    :param filename: Названия файла.
    :return: None.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Data should be pandas.DataFrame.")
    logging.info(f"Start. Запись данных в файл {filename}.")
    csv_path = Path(__file__).parent.parent / "data" / filename
    df.to_csv(
        path_or_buf=csv_path,
        index=False,
        compression="gzip",
    )
    logging.info(f"End. Файл {filename} записан.")


def read_csv(filename: str | None = FILES["csv"]) -> pd.DataFrame:
    """
    Считывает csv файл и возвращает в виде pandas.DataFrame.

    :param filename: Названия файла.
    :return: Pandas DataFrame.
    """
    logging.info(f"Start. Чтение файла {filename}.")
    csv_path = Path(__file__).parent.parent / "data" / filename
    df = pd.read_csv(filepath_or_buffer=csv_path)
    logging.info(f"End. файл {filename} успешно прочитан.")

    return df


def generate_json(df: pd.DataFrame, filename: str | None = FILES["json"]) -> None:
    """
    Загружает данные в формат json

    :param df: Данные в формате pd.DataFrame.
    :param filename: Названия файла.
    :return: None.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Data should be pandas.DataFrame.")
    logging.info(f"Start. Запись данных в файл {filename}.")
    json_path = Path(__file__).parent.parent / "data" / filename
    df.to_json(path_or_buf=json_path)
    logging.info(f"End. Файл {filename} записан.")


def read_json(filename: str | None = FILES["json"]) -> pd.DataFrame:
    """
    Считывает json файл и возвращает в виде pandas.DataFrame.

    :param filename: Названия файла.
    :return: Pandas DataFrame.
    """
    logging.info(f"Start. Чтение файла {filename}.")
    json_path = Path(__file__).parent.parent / "data" / filename
    df = pd.read_json(path_or_buf=json_path)
    logging.info(f"End. файл {filename} успешно прочитан.")

    return df


def measure_formats() -> pd.DataFrame:
    """
    Функция, которая позволяет посчитать объём файлов в разных форматах.
    :return: pd.DataFrame c данными.
    """
    results = []

    for format_name, filename in FILES.items():
        path = Path(__file__).parent.parent / "data" / filename
        if os.path.exists(path):
            size = os.path.getsize(path)

            results.append({
                "format": format_name,
                "size_mb": size / (1024 * 1024),
            })

    return pd.DataFrame(results)