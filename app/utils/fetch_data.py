import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

ORACLE_CONFIG = {
    "user": os.getenv("ORACLE_USER"),
    "password": os.getenv("ORACLE_PASSWORD"),
    "dsn": os.getenv("ORACLE_DSN"),
    "mode": oracledb.AuthMode.DEFAULT
}

class Database:
    def __init__(self):
        self.config = ORACLE_CONFIG
        self.connection = None
    
    def connect(self):
        if self.connection is None:
            self.connection = oracledb.connect(**self.config)
        return self.connection
    
    def get_cml_data(self):
        conn = self.connect()
        cursor = conn.cursor()

        # query = """SELECT * FROM wet_topup WHERE rownum <= 10"""
        query = open("app/utils/queries/CML-page-transaction.sql", "r").read()

        # cursor.execute(query, P63_USER_ID="HPWIN1015")
        cursor.execute(query)
        records = cursor.fetchall()
        print('records: ', records)
        
        cursor.close()
        return records

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

def connect_to_oracle():
    return oracledb.connect(**ORACLE_CONFIG)




