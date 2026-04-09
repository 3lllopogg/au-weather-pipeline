"""
config.py — centralised database connection using Windows Auth + .env
"""

import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

DB_SERVER = os.getenv("DB_SERVER", r"localhost\SQLEXPRESS")
DB_NAME   = os.getenv("DB_NAME",   "AUWeatherPipeline")
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")


def get_connection() -> pyodbc.Connection:
    """Return an open pyodbc connection. Caller must close it."""
    conn_str = (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        "Trusted_Connection=yes;" #Can be extended to support SQL authentication if required
    )
    return pyodbc.connect(conn_str)
