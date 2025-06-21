import pandas as pd
from sqlalchemy import create_engine, text

# Koneksi ke PostgreSQL
engine = create_engine('postgresql://postgres:fmiafi0300@localhost:5432/staging_etl')

# Hapus data lama agar tidak terjadi duplikasi
with engine.begin() as conn:
    conn.execute(text("DELETE FROM fact_sales"))
    conn.execute(text("DELETE FROM dim_date"))
    conn.execute(text("DELETE FROM dim_salesterritory"))
    conn.execute(text("DELETE FROM dim_product"))
    conn.execute(text("DELETE FROM dim_customer"))

# -------------------------------
# 1. Transformasi ke Dim_Customer
# -------------------------------
query_customer = """
SELECT 
    customerid, 
    personid, 
    storeid, 
    territoryid,
    'Individual' AS customertype
FROM customer
"""
df_customer = pd.read_sql(query_customer, engine)
df_customer.to_sql('dim_customer', engine, if_exists='append', index=False)

# -------------------------------
# 2. Transformasi ke Dim_Product
# -------------------------------
query_product = """
SELECT 
    p.productid, 
    p.name, 
    p.productnumber, 
    p.color, 
    p.size, 
    p.weight, 
    p.standardcost, 
    p.listprice, 
    ps.productsubcategoryid AS subcategoryid,
    pc.productcategoryid AS categoryid
FROM product p
LEFT JOIN product_subcategory ps ON p.productsubcategoryid = ps.productsubcategoryid
LEFT JOIN product_category pc ON ps.productcategoryid = pc.productcategoryid
"""
df_product = pd.read_sql(query_product, engine)
df_product.to_sql('dim_product', engine, if_exists='append', index=False)

# -------------------------------
# 3. Transformasi ke Dim_Date
# -------------------------------
df_orderdate = pd.read_sql("SELECT DISTINCT orderdate FROM sales_order_header", engine)
df_orderdate['orderdate'] = pd.to_datetime(df_orderdate['orderdate'])
df_orderdate['datekey'] = df_orderdate['orderdate'].dt.strftime('%Y%m%d').astype(int)
df_orderdate['day'] = df_orderdate['orderdate'].dt.day
df_orderdate['month'] = df_orderdate['orderdate'].dt.month
df_orderdate['monthname'] = df_orderdate['orderdate'].dt.strftime('%B')
df_orderdate['quarter'] = df_orderdate['orderdate'].dt.quarter
df_orderdate['year'] = df_orderdate['orderdate'].dt.year
df_orderdate['weekofyear'] = df_orderdate['orderdate'].dt.isocalendar().week
df_orderdate['isweekend'] = df_orderdate['orderdate'].dt.weekday >= 5

df_dim_date = df_orderdate.rename(columns={'orderdate': 'fulldate'})
df_dim_date = df_dim_date[['datekey', 'fulldate', 'day', 'month', 'monthname', 'quarter', 'year', 'weekofyear', 'isweekend']]
df_dim_date.to_sql('dim_date', engine, if_exists='append', index=False)

# -------------------------------
# 4. Transformasi ke Dim_SalesTerritory
# -------------------------------
query_territory = """
SELECT 
    territoryid AS salesterritoryid,
    name,
    countryregioncode,
    "group" AS groupname
FROM sales_territory
"""
df_territory = pd.read_sql(query_territory, engine)
df_territory.to_sql('dim_salesterritory', engine, if_exists='append', index=False)

# -------------------------------
# 5. Transformasi ke Fact_Sales
# -------------------------------
query_fact = """
SELECT
    soh.salesorderid,
    sod.salesorderdetailid,
    TO_CHAR(soh.orderdate, 'YYYYMMDD')::INT AS orderdatekey,
    soh.customerid,
    sod.productid,
    soh.territoryid AS salesterritoryid,
    sod.orderqty,
    sod.unitprice,
    (sod.orderqty * sod.unitprice) AS linetotal,
    soh.totaldue
FROM sales_order_header soh
JOIN sales_order_detail sod ON soh.salesorderid = sod.salesorderid;
"""
df_fact = pd.read_sql(query_fact, engine)
df_fact.to_sql('fact_sales', engine, if_exists='append', index=False)

print("✅ Transformasi selesai dan data dimuat ke tabel Dim dan Fact di database staging_etl.")