from utils.utils import *

def main():
    users_df = generate_data()
    generate_parquet(users_df)
    generate_csv(users_df)
    generate_compressed_csv(users_df)

    # Можно посмотреть результаты.
    # print(read_parquet())
    # print(read_csv())
    # print(read_csv(path="./data/data.csv.gz"))

if __name__ == "__main__":
    main()