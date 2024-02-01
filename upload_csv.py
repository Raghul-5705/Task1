import pandas as pd
import psycopg2


def upload_csv_file():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="5705"
    )
    df = pd.read_csv('file.csv')
    df.to_sql('EV_database', conn, if_exists='append', index=False)
