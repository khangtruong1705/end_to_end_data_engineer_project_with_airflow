import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook




def transform_dim_location():
    pg_hook = PostgresHook(postgres_conn_id='olist_staging_area')
    engine = pg_hook.get_sqlalchemy_engine()
    geo_df = pd.read_sql("olist_geolocation_dataset", con=engine)
    
    df = geo_df.groupby('geolocation_zip_code_prefix').agg({
        'geolocation_lat': 'mean',
        'geolocation_lng': 'mean',
        'geolocation_city': lambda x: x.mode()[0] if not x.mode().empty else 'Unknown',
        'geolocation_state': lambda x: x.mode()[0] if not x.mode().empty else 'Unknown'
    }).round(6).reset_index()
    
    df['location_key'] = df.index + 1
    df['zip_code_prefix'] = df['geolocation_zip_code_prefix'].astype(str).str.zfill(5)
    
    df.rename(columns={
        'geolocation_city': 'city',
        'geolocation_state': 'state',
        'geolocation_lat': 'lat',
        'geolocation_lng': 'lng'
    }, inplace=True)
    
    df = df[['location_key', 'zip_code_prefix', 'city', 'state', 'lat', 'lng']]
    
    df.to_csv('data/processed/dim_location.csv', index=False, encoding='utf-8')
    print(f"dim_location hoàn thành: {len(df)} unique zip prefixes")

