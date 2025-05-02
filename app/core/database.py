import os

import json
import oracledb
from dotenv import load_dotenv

from app.utils.update_schema import SCHEMA_FILE_PATH

load_dotenv()

connection = None

def init_db():
    try:
        global connection
        if connection is None:
            connection = oracledb.connect(
                user=os.getenv("ORACLE_USER"),
                password=os.getenv("ORACLE_PASSWORD"),
                dsn=os.getenv("ORACLE_DSN"),
                mode=oracledb.SYSDBA
            )

    except oracledb.Error as e:
        print("Database connection failed:", e)
        return None

def get_db():
    return connection

def load_schema_from_file():
    if os.path.exists(SCHEMA_FILE_PATH):
        with open(SCHEMA_FILE_PATH, "r") as f:
            data = json.load(f)
            return data["schema"], data["relations"]
    return None, None
