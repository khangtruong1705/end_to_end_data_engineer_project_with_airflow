import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def transform_dim_date():
    pg_hook = PostgresHook(postgres_conn_id='olist_staging_area')
    engine = pg_hook.get_sqlalchemy_engine()
    # Lấy min-max date từ các bảng có timestamp
    orders_df = pd.read_sql("SELECT order_purchase_timestamp FROM olist_orders_dataset", con=engine)
    orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'])
    
    start_date = orders_df['order_purchase_timestamp'].min().date()
    end_date = orders_df['order_purchase_timestamp'].max().date()
    
    # Tạo date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({'full_date': date_range})
    
    df['date_key'] = df['full_date'].dt.strftime('%d%m%Y').astype(int)
    df['year'] = df['full_date'].dt.year
    df['quarter'] = df['full_date'].dt.quarter
    df['month'] = df['full_date'].dt.month
    df['day'] = df['full_date'].dt.day
    df['week_of_year'] = df['full_date'].dt.isocalendar().week
    df['day_of_week'] = df['full_date'].dt.day_name()
    df['is_weekend'] = df['full_date'].dt.weekday >= 5
    
    # is_holiday: có thể add thủ công Brazil holidays, hoặc để False tạm
    df['is_holiday'] = False
    
    df = df[['date_key', 'full_date', 'year', 'quarter', 'month', 'day',
             'week_of_year', 'day_of_week', 'is_weekend', 'is_holiday']]
    
    df.to_csv('data/processed/dim_date.csv', index=False, encoding='utf-8')
    print(f"dim_date hoàn thành: từ {start_date} đến {end_date} ({len(df)} ngày)")

