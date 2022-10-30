import boto3
import psycopg2
import pandas as pd
import io
from decouple import config

def aws_connect(aws_access_key, aws_secret_key, region, db_name, port, username):
    '''Sets up session with AWS account and returns session token
    
        params:
            aws_access_key (str): AWS access key id
            aws_secret_key (str): AWS secret key for account
            region (str): Default region for resources
            db_name (str): Database name for RDS
            port (int): Port to connect on
            username (str): User for accessing RDS db
        returns:
            token (str): Session token/password
    '''
    
    session = boto3.Session(aws_access_key_id=aws_access_key, aws_seret_access_key=aws_secret_key, region=region)
    client = session.client('rds')
    token = client.generate_db_auth_token(dbhostname=db_name, port=port, dbusername=username, region=region)
    
    return token


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

    conn = psycopg2.connect(host=host, port=port, user=user, database=db, password=password, sslmode='require', sslrootcert='SSLCERTIFICATE')
    
    return conn

def connect_s3(aws_access_key, aws_secret_key, region):

    '''Establishes connection to s3 bucket
    
        params:
            aws_access_key (str): AWS access key id
            aws_secret_key (str): AWS secret access key associated with account
            region (str): Default region name for resources
        returns:
            None
    '''
    
    session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region)
    s3 = session.resource('s3')
    
    return s3
    
    # Get s3 connection and in-stream buffer for df
    s3 = connect_s3(aws_access_key, aws_secret_key, region)
    csv_buffer = io.StringIO()

    # Upload csv to s3 bucket
    df.to_csv(csv_buffer, index=False)
    s3.Object(bucket_name, object_path).put(Body=csv_buffer.getvalue())

    # close text stream


def get_data(conn):

    sql = f'''
    <data_query>
    '''

    results = pd.read_sql(sql, conn)
    df = pd.DataFrame(results)

    return df


 

if __name__ == '__main__':

    pass