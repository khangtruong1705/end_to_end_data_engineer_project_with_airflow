import os
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
load_dotenv()



os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME', "")  
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_API_TOKEN', "")
from kaggle.api.kaggle_api_extended import KaggleApi


def extract_data():
    api = KaggleApi()
    api.authenticate()
    DATASET_ID = "olistbr/brazilian-ecommerce"
    RAW_DATA_FOLDER = "data/raw"
    print(f"Đang tải dataset: {DATASET_ID}...")
    api.dataset_download_files(DATASET_ID, path=RAW_DATA_FOLDER, unzip=True)
    print(f"Tải xong! Dữ liệu nằm tại: {os.path.abspath(RAW_DATA_FOLDER)}")
    pg_hook = PostgresHook(postgres_conn_id='olist_staging_area')
    engine = pg_hook.get_sqlalchemy_engine()
    for file in os.listdir(RAW_DATA_FOLDER):
        if file.endswith(".csv"):
            table_name = file.replace(".csv", "").lower()
            file_path = os.path.join(RAW_DATA_FOLDER,file)
            print(f"Đang load file: {file}")
            df = pd.read_csv(file_path)
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False
            )
            print(f"Đã tạo bảng: {table_name}")
