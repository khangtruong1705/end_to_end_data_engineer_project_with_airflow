import os
import pandas as pd
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook



def transform_dim_seller():
    pg_hook = PostgresHook(postgres_conn_id='olist_staging_area')
    engine = pg_hook.get_sqlalchemy_engine()
    # transform sellers_dataset
    sellers_df = pd.read_sql("olist_sellers_dataset", con=engine)    
    sellers_df['seller_key'] = sellers_df.index + 1
    sellers_df['seller_id'] = sellers_df['seller_id'].astype(str)
    sellers_df['seller_zip_code_prefix'] = sellers_df['seller_zip_code_prefix'].astype(str).str.zfill(5)
    sellers_df['seller_city'] = sellers_df['seller_city'].str.title()
    sellers_df['seller_state'] = sellers_df['seller_state'].str.upper()
    
    # transform geolocation_dataset
    geolocation_df = pd.read_sql("olist_geolocation_dataset", con=engine)
    geo_avg = geolocation_df.groupby('geolocation_zip_code_prefix').agg({
    'geolocation_lat': 'mean',
    'geolocation_lng': 'mean'
    }).round(6).reset_index()
    # Rename để dễ join
    geo_avg.rename(columns={
        'geolocation_zip_code_prefix': 'seller_zip_code_prefix',
        'geolocation_lat': 'lat',
        'geolocation_lng': 'lng'
    }, inplace=True)
    geo_avg['seller_zip_code_prefix'] = geo_avg['seller_zip_code_prefix'].astype(str).str.zfill(5)

    # transform dim_sellers
    df = pd.merge(sellers_df, geo_avg, on='seller_zip_code_prefix', how='left')
    current_date = datetime.now().date()
    future_date = current_date + timedelta(days=365*10)
    df['effective_date'] = current_date.strftime('%d/%m/%Y')
    df['end_date'] = future_date.strftime('%d/%m/%Y')
    df['is_current'] = True
    df = df[[
        'seller_key', 'seller_id', 'seller_zip_code_prefix',
        'seller_city', 'seller_state', 'lat', 'lng',
        'effective_date', 'end_date', 'is_current'
    ]]
    df.to_csv('data/processed/dim_seller.csv',index=False,encoding='utf-8')
    print('transformed and save to dim_seller table !!!')

