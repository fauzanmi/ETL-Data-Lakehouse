import pandas as pd
from sqlalchemy import create_engine

#Connect ke database OLTP (AdventureWorks)
oltp_engine = create_engine('postgresql+psycopg2://postgres:fmiafi0300@localhost:5432/adventureworks')

#Connect ke database staging
staging_engine = create_engine('postgresql+psycopg2://postgres:fmiafi0300@localhost:5432/staging_etl')

#Extract tabel-tabel yang dibutuhkan untuk Star Schema
sales_order_header = pd.read_sql_query("SELECT * FROM sales.salesorderheader", oltp_engine)
sales_order_detail = pd.read_sql_query("SELECT * FROM sales.salesorderdetail", oltp_engine)
customer = pd.read_sql_query("SELECT * FROM sales.customer", oltp_engine)
product = pd.read_sql_query("SELECT * FROM production.product", oltp_engine)
product_subcategory = pd.read_sql_query("SELECT * FROM production.productsubcategory", oltp_engine)
product_category = pd.read_sql_query("SELECT * FROM production.productcategory", oltp_engine)
sales_territory = pd.read_sql_query("SELECT * FROM sales.salesterritory", oltp_engine)
store = pd.read_sql_query("SELECT * FROM sales.store", oltp_engine)
person = pd.read_sql_query("SELECT * FROM person.person", oltp_engine)

#Simpan ke database staging
sales_order_header.to_sql('sales_order_header', staging_engine, if_exists='replace', index=False)
sales_order_detail.to_sql('sales_order_detail', staging_engine, if_exists='replace', index=False)
customer.to_sql('customer', staging_engine, if_exists='replace', index=False)
product.to_sql('product', staging_engine, if_exists='replace', index=False)
product_subcategory.to_sql('product_subcategory', staging_engine, if_exists='replace', index=False)
product_category.to_sql('product_category', staging_engine, if_exists='replace', index=False)
sales_territory.to_sql('sales_territory', staging_engine, if_exists='replace', index=False)
store.to_sql('store', staging_engine, if_exists='replace', index=False)
person.to_sql('person', staging_engine, if_exists='replace', index=False)

print("Semua tabel berhasil diekstrak dan dimasukkan ke database staging.")