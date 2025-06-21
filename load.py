import pandas as pd
from sqlalchemy import create_engine, text

# --- KONFIGURASI KONEKSI DATABASE ---
staging_engine = create_engine('postgresql+psycopg2://postgres:fmiafi0300@localhost/staging_etl')
dw_engine = create_engine('postgresql+psycopg2://postgres:fmiafi0300@localhost/warehouse_etl')

# --- HAPUS DATA LAMA DI DATA WAREHOUSE ---
with dw_engine.begin() as connection:
    connection.execute(text('DELETE FROM fact_sales;'))
    connection.execute(text('DELETE FROM dim_date;'))
    connection.execute(text('DELETE FROM dim_salesterritory;'))
    connection.execute(text('DELETE FROM dim_customer;'))
    connection.execute(text('DELETE FROM dim_product;'))

# --- LOAD DIM PRODUCT ---
df_product = pd.read_sql_table('dim_product', staging_engine)
df_product.to_sql('dim_product', dw_engine, if_exists='append', index=False)

# --- LOAD DIM CUSTOMER ---
df_customer = pd.read_sql_table('dim_customer', staging_engine)
df_customer.to_sql('dim_customer', dw_engine, if_exists='append', index=False)

# --- LOAD DIM SALESTERRITORY ---
df_territory = pd.read_sql_table('dim_salesterritory', staging_engine)
df_territory.to_sql('dim_salesterritory', dw_engine, if_exists='append', index=False)

# --- LOAD DIM DATE ---
df_date = pd.read_sql_table('dim_date', staging_engine)
df_date.to_sql('dim_date', dw_engine, if_exists='append', index=False)

# --- LOAD FACT SALES ---
df_fact_sales = pd.read_sql_table('fact_sales', staging_engine)
df_fact_sales.to_sql('fact_sales', dw_engine, if_exists='append', index=False)

print("✅ Loading dari staging ke data warehouse selesai.")