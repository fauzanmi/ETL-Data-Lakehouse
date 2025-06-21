import pandas as pd
from sqlalchemy import create_engine, text

# Koneksi ke database warehouse
warehouse_engine = create_engine('postgresql+psycopg2://postgres:fmiafi0300@localhost/warehouse_etl')

# Daftar tabel yang akan dihapus datanya
tables = [
    'fact_sales',
    'dim_date',
    'dim_salesterritory',
    'dim_customer',
    'dim_product'
]

# Hapus data tanpa menghapus tabel
with warehouse_engine.begin() as conn:
    for table in tables:
        conn.execute(text(f'DELETE FROM {table};'))

print("✅ Semua data di database warehouse_etl telah dihapus (tabel tetap ada).")