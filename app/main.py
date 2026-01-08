from utils.utils import *

def main():
    rows = 100000
    users_df = generate_data(rows)
    generate_parquet(users_df)
    generate_csv(users_df)
    generate_compressed_csv(users_df)
    generate_json(users_df)
    print(measure_formats())

    # Можно посмотреть результаты.
    # print(read_parquet())
    # print(read_csv())
    # print(read_csv(path="./data/data.csv.gz"))

if __name__ == "__main__":
    main()