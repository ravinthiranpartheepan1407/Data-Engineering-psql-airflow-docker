from dotenv import load_dotenv
import os
import pandas as pd
# Python Adapter for SQL
import psycopg as psq

load_dotenv()

def get_postgres_meta():
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
def view_data(data):
    read_data = pd.read_csv(data)
    read_data.head()

def create_table(data):
    try:
        cur_est = get_postgres_meta()

get_postgres_meta()
view_data(data)