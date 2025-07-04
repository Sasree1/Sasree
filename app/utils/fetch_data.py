import oracledb
import os
import asyncio
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
    
    def get_cml_data_sync(self):
        conn = self.connect()
        # cursor = conn.cursor()

        # query = """SELECT * FROM wet_topup WHERE rownum <= 10"""
        query = open("app/utils/queries/CML-page-transaction-v4.sql", "r").read()
        # query = open("app/utils/queries/CML-page-transaction-v2.sql", "r").read()

        # cursor.execute(query, P63_USER_ID="HPWIN1015")
        # cursor.execute(query)
        # records = cursor.fetchall()
        
        # cursor.close()
        # return records

        data = pd.read_sql(query, con=conn)
        return data
        
    
    async def get_cml_data_async(self):
        def generate():
            chunk_size = 100
            conn = self.connect()
            # cursor = conn.cursor()

            # query = """SELECT * FROM wet_topup WHERE rownum <= 10"""
            # query = open("app/utils/queries/CML-page-transaction.sql", "r").read()
            query = open("app/utils/queries/CML-page-transaction-v4.sql", "r").read()

            # cursor.execute(query, P63_USER_ID="HPWIN1015")
            # cursor.execute(query)
            chunks = pd.read_sql(query, con=conn, chunksize=chunk_size)
            for chunk in chunks:
                yield chunk
        
        loop = asyncio.get_running_loop()
        for chunk in await loop.run_in_executor(None, lambda: list(generate())):
            yield chunk


    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

def connect_to_oracle():
    return oracledb.connect(**ORACLE_CONFIG)

