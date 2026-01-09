import os
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook



def transform_fact_order():
    pg_hook = PostgresHook(postgres_conn_id='olist_staging_area')
    engine = pg_hook.get_sqlalchemy_engine()
    # Đọc các bảng staging
    order_items_df = pd.read_sql("olist_order_items_dataset", con=engine)
    orders_df = pd.read_sql("olist_orders_dataset", con=engine)
    payments_df = pd.read_sql("olist_order_payments_dataset", con=engine)
    reviews_df = pd.read_sql("olist_order_reviews_dataset", con=engine)
    
    # Đọc các dimension đã transform
    dim_customer = pd.read_csv('data/processed/dim_customer.csv')
    dim_seller = pd.read_csv('data/processed/dim_seller.csv')
    dim_product = pd.read_csv('data/processed/dim_product.csv')
    dim_date = pd.read_csv('data/processed/dim_date.csv')
    dim_payment = pd.read_csv('data/processed/dim_payment.csv')
    
    # Bắt đầu từ order_items (grain per item)
    df = order_items_df.copy()
    df['sales_key'] = df.index + 1
    
    # Join với orders để lấy customer_id, status và timestamps
    df = pd.merge(df, orders_df[[
        'order_id', 'customer_id', 'order_status',
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]], on='order_id', how='left')
    
    # Chuyển timestamp sang datetime
    timestamp_cols = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in timestamp_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Tạo date_key (YYYYMMDD int), fill NaN bằng 19000101 (unknown date)
    unknown_date_key = 19000101
    
    df['purchase_date_key'] = df['order_purchase_timestamp'].dt.strftime('%d%m%Y').fillna(unknown_date_key).astype(int)
    df['approved_date_key'] = df['order_approved_at'].dt.strftime('%d%m%Y').fillna(unknown_date_key).astype(int)
    df['carrier_date_key'] = df['order_delivered_carrier_date'].dt.strftime('%d%m%Y').fillna(unknown_date_key).astype(int)
    df['delivery_date_key'] = df['order_delivered_customer_date'].dt.strftime('%d%m%Y').fillna(unknown_date_key).astype(int)
    df['estimated_date_key'] = df['order_estimated_delivery_date'].dt.strftime('%d%m%Y').fillna(unknown_date_key).astype(int)
    
    # Join dim_date (không cần merge data, chỉ để kiểm tra – nhưng đảm bảo unknown_date_key có trong dim_date)
    # Nếu bạn thêm record unknown trong dim_date thì không cần làm gì thêm
    
    # Join dim_customer
    df = pd.merge(df, dim_customer[['customer_id', 'customer_key']], on='customer_id', how='left')
    
    # Join dim_seller
    df = pd.merge(df, dim_seller[['seller_id', 'seller_key']], on='seller_id', how='left')
    
    # Join dim_product
    df = pd.merge(df, dim_product[['product_id', 'product_key']], on='product_id', how='left')
    
    # Join dim_payment: aggregate theo order_id trước để tránh duplicate rows
    # Lấy payment chính (dùng mode hoặc first), vì hầu hết order chỉ có 1 payment
    payment_mode = payments_df.groupby('order_id').agg({
        'payment_type': lambda x: x.mode()[0] if not x.empty else None,
        'payment_installments': lambda x: x.mode()[0] if not x.empty else 1
    }).reset_index()
    
    payment_mode = pd.merge(payment_mode, dim_payment, 
                            on=['payment_type', 'payment_installments'], 
                            how='left')
    
    df = pd.merge(df, payment_mode[['order_id', 'payment_key']], on='order_id', how='left')
    
    # Join review: average score theo order_id
    review_avg = reviews_df.groupby('order_id')['review_score'].mean().round(1).reset_index()
    review_avg.rename(columns={'review_score': 'review_score_avg'}, inplace=True)
    df = pd.merge(df, review_avg, on='order_id', how='left')
    
    # Tính delivery time và late flag
    df['delivery_time_days'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days
    df['is_late_delivery'] = df['order_delivered_customer_date'] > df['order_estimated_delivery_date']
    df['is_late_delivery'] = df['is_late_delivery'].fillna(False)
    
    # Chọn cột cuối cùng cho fact_sales
    df = df[[
        'sales_key','order_id','order_item_id','product_key','seller_key','customer_key',
        'payment_key','purchase_date_key','approved_date_key','carrier_date_key','delivery_date_key',
        'estimated_date_key','order_status','price','freight_value','review_score_avg','delivery_time_days','is_late_delivery']]
    # Lưu file
    os.makedirs('data/processed', exist_ok=True)
    df.to_csv('data/processed/fact_order.csv', index=False, encoding='utf-8')
    print(f"fact_sale hoàn thành: {len(df)} bản ghi (grain: per order item)")

