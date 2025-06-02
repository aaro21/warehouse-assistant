import os
import pyodbc
from typing import List, Tuple
from dotenv import load_dotenv

load_dotenv()

def get_sqlserver_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        f"DATABASE={os.getenv('SQL_DB')};"
        f"UID={os.getenv('SQL_USER')};"
        f"PWD={os.getenv('SQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def fetch_procedures(limit: int = 1) -> List[Tuple[str, str]]:
    conn = get_sqlserver_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT TOP ({limit}) OBJECT_NAME(object_id) as proc_name, definition
        FROM sys.sql_modules
        WHERE definition LIKE '%INTO%'
    """)
    return cursor.fetchall()