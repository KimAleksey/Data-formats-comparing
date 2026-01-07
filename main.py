import uuid
import faker
import logging
import datetime

import pandas as pd


# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


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


def generate_parquet(df: pd.DataFrame, path: str | None = "./data/data.parquet") -> None:
    """
    Загружает данные в формат parquet.

    :param df: Данные в формате pd.DataFrame.
    :param path: Путь для записи файла с указанием названия файла.
    :return: None.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Data should be pandas.DataFrame.")
    logging.info("Start. Запись данных в файл в формате parquet.")
    df.to_parquet(path=path)
    logging.info(f"End. Файл {path.split("/")[-1]} записан.")


def read_parquet(path: str | None = "./data/data.parquet") -> pd.DataFrame:
    """
    Считывает parquet файл и возвращает в виде pandas.DataFrame.

    :param path: Путь к файлу с указанием названия.
    :return: Pandas DataFrame.
    """
    df = pd.read_parquet(path="./data/data.parquet")
    return df


def main():
    users_df = generate_data()
    generate_parquet(users_df)
    df_from_parquet = read_parquet()

if __name__ == "__main__":
    main()
