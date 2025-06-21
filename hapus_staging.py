from sqlalchemy import create_engine, text

# Koneksi ke database staging
staging_engine = create_engine('postgresql+psycopg2://postgres:fmiafi0300@localhost/staging_etl')

# Hapus data dengan urutan yang tepat: mulai dari fact table → dimension → raw tables
with staging_engine.begin() as connection:
    # Tabel fakta
    connection.execute(text('DELETE FROM fact_sales;'))

    # Tabel dimensi
    connection.execute(text('DELETE FROM dim_customer;'))
    connection.execute(text('DELETE FROM dim_product;'))
    connection.execute(text('DELETE FROM dim_salesterritory;'))
    connection.execute(text('DELETE FROM dim_date;'))

    # Tabel staging mentah (raw)
    connection.execute(text('DELETE FROM sales_order_detail;'))
    connection.execute(text('DELETE FROM sales_order_header;'))
    connection.execute(text('DELETE FROM product;'))
    connection.execute(text('DELETE FROM product_subcategory;'))
    connection.execute(text('DELETE FROM product_category;'))
    connection.execute(text('DELETE FROM customer;'))
    connection.execute(text('DELETE FROM person;'))
    connection.execute(text('DELETE FROM store;'))
    connection.execute(text('DELETE FROM sales_territory;'))

print("✅ Data di database staging berhasil dibersihkan tanpa menghapus tabel dan relasi!")
