import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook



def transform_dim_product():
    pg_hook = PostgresHook(postgres_conn_id='olist_staging_area')
    engine = pg_hook.get_sqlalchemy_engine()
    products_df = pd.read_sql("olist_products_dataset", con=engine)
    translation_df = pd.read_sql("product_category_name_translation", con=engine)  # bảng này có trong dataset
    
    # Transform products
    products_df['product_key'] = products_df.index + 1
    
    # Xử lý category
    products_df['product_category_name'] = products_df['product_category_name'].str.lower().str.replace('_', ' ').str.title()
    
    # Merge translation (left join để giữ hết sản phẩm)
    df = pd.merge(products_df, translation_df, on='product_category_name', how='left')
    
    # Ưu tiên English name, fallback về Portuguese nếu null
    df['category_name_english'] = df['product_category_name_english'].fillna(df['product_category_name'])
    
    # Xử lý null cho các đo lường → 0
    measure_cols = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty',
                    'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
    df[measure_cols] = df[measure_cols].fillna(0)
    
    # Rename rõ nghĩa
    df.rename(columns={
        'product_name_lenght': 'name_length',
        'product_description_lenght': 'description_length',
        'product_photos_qty': 'photos_qty',
        'product_weight_g': 'weight_g',
        'product_length_cm': 'length_cm',
        'product_height_cm': 'height_cm',
        'product_width_cm': 'width_cm',
        'category_name_english': 'category_name_english'
    }, inplace=True)
    
    # Chọn cột
    df = df[[
        'product_key', 'product_id', 'category_name_english', 'product_category_name',
        'name_length', 'description_length', 'photos_qty',
        'weight_g', 'length_cm', 'height_cm', 'width_cm'
    ]]
    
    df.to_csv('data/processed/dim_product.csv', index=False, encoding='utf-8')
    print('transformed and save to dim_product table !!!')

