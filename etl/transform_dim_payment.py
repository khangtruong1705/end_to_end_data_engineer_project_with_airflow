import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def transform_dim_payment():
    pg_hook = PostgresHook(postgres_conn_id='olist_staging_area')
    engine = pg_hook.get_sqlalchemy_engine()
    payments_df = pd.read_sql("olist_order_payments_dataset", con=engine)
    
    # Chuẩn hóa payment_type
    payments_df['payment_type'] = (
        payments_df['payment_type']
        .str.strip()
        .str.lower()
        .replace('not_defined', None)
    )
    
    # Chuẩn hóa payment_installments
    payments_df['payment_installments'] = (
        pd.to_numeric(payments_df['payment_installments'], errors='coerce')
        .fillna(1)
        .astype(int)
    )
    
    # Lấy các tổ hợp duy nhất
    df = (
        payments_df[['payment_type', 'payment_installments']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    
    # Tạo surrogate key
    df['payment_key'] = df.index + 1
    
    # Sắp xếp để dễ đọc
    df = df.sort_values(['payment_type', 'payment_installments']).reset_index(drop=True)
    df['payment_key'] = df.index + 1
    
    # Thứ tự cột cuối cùng
    df = df[['payment_key', 'payment_type', 'payment_installments']]
    
    df.to_csv('data/processed/dim_payment.csv', index=False, encoding='utf-8')
    print(f"dim_payment hoàn thành: {len(df)} distinct payment methods")
