# update_schema.py

import oracledb
import json
import os

from dotenv import load_dotenv

load_dotenv()

ORACLE_CONFIG = {
    "user": os.getenv("ORACLE_USER"),
    "password": os.getenv("ORACLE_PASSWORD"),
    "dsn": os.getenv("ORACLE_DSN"),
    "mode": oracledb.AuthMode.DEFAULT
}

static_dir = os.path.join(os.path.dirname(__file__), "..", "static")

SCHEMA_FILE_PATH = os.path.join(static_dir, "db_schema/db_schema.json")

def connect_to_oracle():
    return oracledb.connect(**ORACLE_CONFIG)

def get_schema_and_relations(conn):
    cursor = conn.cursor()

    # Get all tables
    cursor.execute("SELECT table_name FROM user_tables")
    tables = cursor.fetchall()

    schema = {}
    for (table_name,) in tables:
        cursor.execute(f"""
            SELECT column_name
            FROM user_tab_columns
            WHERE table_name = '{table_name}'
        """)
        columns = [col[0] for col in cursor.fetchall()]
        schema[table_name] = columns

    # Get foreign key relationships
    cursor.execute("""
        SELECT
            a.table_name, a.column_name,
            c.table_name AS ref_table, c.column_name AS ref_column
        FROM
            user_cons_columns a
            JOIN user_constraints b ON a.constraint_name = b.constraint_name
            JOIN user_cons_columns c ON b.r_constraint_name = c.constraint_name
        WHERE
            b.constraint_type = 'R'
    """)
    foreign_keys = cursor.fetchall()

    relations = {}
    for table, column, ref_table, ref_column in foreign_keys:
        if table not in relations:
            relations[table] = []
        relations[table].append((column, ref_table, ref_column))

    cursor.close()
    return schema, relations

def save_schema_file(schema, relations):
    os.makedirs(os.path.dirname(SCHEMA_FILE_PATH), exist_ok=True)
    with open(SCHEMA_FILE_PATH, "w") as f:
        json.dump({"schema": schema, "relations": relations}, f, indent=2)
    print(f"[✓] Schema saved to {SCHEMA_FILE_PATH}")


def get_data(conn):
    cursor = conn.cursor()
    
    query = """SELECT * FROM wet_topup WHERE rownum <= 10"""

    # Get all tables
    cursor.execute(query)
    records = cursor.fetchall()
    for record in records:
        print(record)


def main():
    print("[*] Connecting to Oracle DB...")
    conn = connect_to_oracle()
    print("[*] Fetching schema and relations...")
    schema, relations = get_schema_and_relations(conn)
    print("[*] Saving schema to file...")
    save_schema_file(schema, relations)
    conn.close()

if __name__ == "__main__":
    main()

