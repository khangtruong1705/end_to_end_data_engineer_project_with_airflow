import pandas as pd
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook





def transform_dim_customers():
    pg_hook = PostgresHook(postgres_conn_id='olist_staging_area')
    engine = pg_hook.get_sqlalchemy_engine()
    # transform customers_dataset
    customers_df = pd.read_sql("olist_customers_dataset", con=engine)    
    customers_df['customer_key'] = customers_df.index + 1
    customers_df['customer_unique_id'] = customers_df['customer_unique_id'].astype(str)
    customers_df['customer_zip_code_prefix'] = customers_df['customer_zip_code_prefix'].astype(str).str.zfill(5)
    customers_df['customer_city'] = customers_df['customer_city'].str.title()
    customers_df['customer_state'] = customers_df['customer_state'].str.upper()
    
    # transform geolocation_dataset
    geolocation_df = pd.read_sql("olist_geolocation_dataset", con=engine)
    geo_avg = geolocation_df.groupby('geolocation_zip_code_prefix').agg({
    'geolocation_lat': 'mean',
    'geolocation_lng': 'mean'
    }).round(6).reset_index()
    # Rename để dễ join
    geo_avg.rename(columns={
        'geolocation_zip_code_prefix': 'customer_zip_code_prefix',
        'geolocation_lat': 'lat',
        'geolocation_lng': 'lng'
    }, inplace=True)
    geo_avg['customer_zip_code_prefix'] = geo_avg['customer_zip_code_prefix'].astype(str).str.zfill(5)

    # transform dim_customers
    df = pd.merge(customers_df, geo_avg, on='customer_zip_code_prefix', how='left')
    current_date = datetime.now().date()
    future_date = current_date + timedelta(days=365*10)
    df['effective_date'] = current_date.strftime('%d/%m/%Y')
    df['end_date'] = future_date.strftime('%d/%m/%Y')
    df['is_current'] = True
    df = df[[
        'customer_key', 'customer_id', 'customer_unique_id', 'customer_zip_code_prefix',
        'customer_city', 'customer_state', 'lat', 'lng',
        'effective_date', 'end_date', 'is_current'
    ]]
    df.to_csv('data/processed/dim_customer.csv',index=False,encoding='utf-8')
    print('transformed and save to dim_customer table !!!')

