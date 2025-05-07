import oracledb
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

ORACLE_CONFIG = {
    "user": os.getenv("ORACLE_USER"),
    "password": os.getenv("ORACLE_PASSWORD"),
    "dsn": os.getenv("ORACLE_DSN"),
    "mode": oracledb.AuthMode.DEFAULT
}

# oracledb.init_oracle_client(lib_dir=r"F:\OracleDB\instantclient_23_8")

class Database:
    def __init__(self):
        self.config = ORACLE_CONFIG
        self.connection = None
    
    def connect(self):
        if self.connection is None:
            self.connection = oracledb.connect(**self.config)
        return self.connection
    
    def get_cml_data(self):
        chunk_size = 1000
        conn = self.connect()
        cursor = conn.cursor()

        # query = """SELECT * FROM wet_topup WHERE rownum <= 10"""
        query = open("app/utils/queries/CML-page-transaction.sql", "r").read()
        # query = open("app/utils/queries/CML-page-transaction-v2.sql", "r").read()

        # cursor.execute(query, P63_USER_ID="HPWIN1015")
        cursor.execute(query)
        # chunks = pd.read_sql(query, con=conn, chunksize=chunk_size)
        # for chunk in chunks:
        #     yield chunk
            
        records = cursor.fetchall()
        
        cursor.close()
        return records

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

def connect_to_oracle():
    return oracledb.connect(**ORACLE_CONFIG)

