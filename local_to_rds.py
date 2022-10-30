import boto3
import psycopg2
import pandas as pd
import io
import csv
from decouple import config

AWS_RDS_HOST = config('AWS_RDS_HOST') 
AWS_RDS_USERNAME = config('AWS_RDS_USERNAME')
AWS_RDS_PORT = config('AWS_RDS_PORT')
AWS_RDS_DB_NAME = config('AWS_RDS_DB_NAME')
AWS_RDS_PASSWORD = config('AWS_RDS_PASSWORD')


def connect_postgres(host, port, user, db, password):
    '''Connect to RDS Postgres via psycopg2
        
        params:
            host (str): Url of client to RDS database
            port (int): Port to connect on
            user (string): Username for db
            db (string): Name of database
            password (string): Result of AWS session token
        returns:
            conn: psycopg2 connection object
    '''
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, database=db, password=password, sslmode='require', sslrootcert='SSLCERTIFICATE')
    except Exception as e:
        print(e)
        return False

    return conn


def get_data(conn):

    sql = f'''
    "SELECT id, user_id, movie_name, rating, genre FROM movies"
    '''

    results = pd.read_sql(sql, conn)
    df = pd.DataFrame(results)

    return df


def create_table(conn):

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE movies(
        id integer PRIMARY KEY,
        user_id integer,
        movie_name text,
        rating decimal,
        genre text
    )
    """)
    conn.commit()
    print("Table Created")

def insert_data_into_table(cursor, conn):

    csv_path = "datasets/movies_dataset.csv"
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row.
        i = 0
        for row in reader:
            if i > 100:
                break
            else:
                cursor.execute(
                "INSERT INTO movies VALUES (%s, %s, %s, %s, %s)",
                row
            )
            i += 1

    conn.commit()   
    print("Insert Successful")

if __name__ == '__main__':

    connection = connect_postgres(AWS_RDS_HOST, AWS_RDS_PORT, AWS_RDS_USERNAME, AWS_RDS_DB_NAME, AWS_RDS_PASSWORD)
    if connection:
        print("Connection Successful")
        # create_table(connection)
        cursor = connection.cursor()
        insert_data_into_table(cursor, connection)
    else:
        print("Connection Fail")

    
    # cursor = connection.cursor()
    # insert_data_into_table(cursor, connection)
    # get_data(connection)
