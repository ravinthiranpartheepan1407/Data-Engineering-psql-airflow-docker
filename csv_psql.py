from dotenv import load_dotenv
import os
import pandas as pd
# Python Adapter for SQL
import psycopg as psq
# import vertezml as vz
import numpy as np
# import matplotlib.pyplot as plt

load_dotenv()


postgres_port = os.getenv("POSTGRES_PORT")
postgres_host = os.getenv("POSTGRES_HOST")
postgres_db = os.getenv("POSTRGRES_DB")
postgres_user = os.getenv("POSTGRES_USER")
postgres_pwd = os.getenv("POSTGRES_PWD")
print(f'Port: {postgres_port}')
print(f'Host: {postgres_host}')
print(f'DB: {postgres_db}')
print(f'User: {postgres_user}')
print(f'PWD: {postgres_pwd}')

try:
    conn_est = psq.connect(
        host = postgres_host,
        database = postgres_db,
        user = postgres_user,
        password = postgres_pwd,
        port = postgres_port
    )
    cursor_est = conn_est.cursor()
    print("PostgreSQL Connection Established")
except Exception as err:
    print(f'Error: {err}')

data = "./Amazon_Sale_Report.csv"
read_data = pd.read_csv(data)
def view_data(data):
    read_data = pd.read_csv(data)
    read_data.head()

def data_preprocessing(read_data):
    check_na = read_data.isna().sum()
    print(check_na)
    read_data['Courier Status'] = read_data['Courier Status'].fillna("NA")
    read_data['Amount'] = read_data['Amount'].fillna(read_data['Amount'].median())
    read_data['ship-state'] = read_data['ship-state'].fillna("NA")
    check_after_na = read_data.isna().sum()
    print(f'After Pre-Processing: {check_after_na}')

check_na = read_data.isna().sum()
print(check_na)
read_data['Courier Status'] = read_data['Courier Status'].fillna("NA")
read_data['Amount'] = read_data['Amount'].fillna(read_data['Amount'].median())
read_data['ship-state'] = read_data['ship-state'].fillna("NA")
check_after_na = read_data.isna().sum()
print(f'After Pre-Processing: {check_after_na}')

def create_table(data):
    try:
        cursor_est.execute(""" CREATE TABLE IF NOT EXISTS  amazon_sales (index INTEGER PRIMARY KEY, OrderID VARCHAR(50), Date VARCHAR(50), Status VARCHAR(50), Fulfilment VARCHAR(50), SalesChannel VARCHAR(50), ShipServiceLevel VARCHAR(50), Category VARCHAR(50), CourierStatus VARCHAR(50), Amount FLOAT, ShipState VARCHAR(50))""")
        conn_est.commit()
        print(f'Created Table in Postgres Server')
    except Exception as err:
        print(f'Cannot create table due to: {err}')

def insert_data(data):
    # read_data = pd.read_csv(data)
    row_count = 0
    for _, row in read_data.iterrows():
        count = f"""SELECT COUNT(*) FROM amazon_sales WHERE index = {row["index"]}"""
        cursor_est.execute(count)
        result = cursor_est.fetchone()

        if(result[0]) == 0:
            row_count += 1
            cursor_est.execute(""" INSERT INTO amazon_sales(index, OrderID, Date, Status, Fulfilment, SalesChannel, ShipServiceLevel, Category, CourierStatus, Amount, ShipState) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (int(row[0]),str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6]), str(row[7]), str(row[8]), float(row[9]) if str(row[9]).replace(".","").isdigit() else None, str(row[10])))
    
    conn_est.commit()
    print(f'Total Inserted Rows: {row_count}')

def category_freq(data):
    categoriesIndex = read_data["Category"].value_counts().index
    categoriesVal = read_data["Category"].value_counts().values
    # category = {
    #     categoriesIndex: categoriesVal
    # }
    print(f'Category: {categoriesIndex} | Frequency: {categoriesVal}')
    print(f'First Item: {categoriesIndex[0]}')
    Items = []
    SoldFrequency = []
    for elements in range(len(categoriesIndex)):
        Items.append(categoriesIndex[elements])

    for elements in range(len(categoriesVal)):
        SoldFrequency.append(categoriesVal[elements])
    
    print(f'Items: {Items}')
    print(f'SoldFrequency: {SoldFrequency}')
    # read_data["Items"] = read_data["Category"].value_counts()
    # read_data["Sold_Frequency"] = read_data["Category"].value_counts().values
    # try:
    #     cursor_est.execute("""CREATE TABLE IF NOT EXISTS amazon_sales_segmentation(Items VARCHAR(50), SoldFrequency INTEGER)""")
    #     conn_est.commit()
    #     print("Amazon Sales Segmentation DB Created")
    # except Exception as err:
    #     print(f'DB not created due to {err}')
    
    insert_data = "INSERT INTO amazon_sales_segmentation(Items, SoldFrequency) VALUES (%s,%s)"
    data_count = 0
    for elements in range(len(Items)):
        values = (Items[elements], SoldFrequency[elements])
        cursor_est.execute(insert_data,values)
        data_count += 1
    conn_est.commit()
    print(f'Data Inserted successfully')

# def regression_analysis():
#     categories = read_data["Category"].value_counts().index
#     print(f'Item: {categories}')


view_data(data)
# data_preprocessing(read_data)
# create_table(data)
# insert_data(data)
category_freq(data)