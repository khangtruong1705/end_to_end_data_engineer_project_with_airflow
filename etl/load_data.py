import os
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook




def load_data_to_datawarehouse():
    pg_hook = PostgresHook(postgres_conn_id='olist_datawarehouse')
    engine = pg_hook.get_sqlalchemy_engine()
    PROCESSED_DATA_FOLDER = 'data/processed'
    for file in os.listdir(PROCESSED_DATA_FOLDER):
        if file.endswith('.csv'):
            table_name = file.replace(".csv", "").lower()
            file_path = os.path.join(PROCESSED_DATA_FOLDER,file)
            print(f"Đang load file: {file} đến data-warehouse")
            df = pd.read_csv(file_path)
            with engine.begin() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists="replace",
                    index=False
                )
            print(f"Đã load {table_name} vào data-warehouse")
    print('hoàn thành load dữ liệu vào data-warehouse')        





