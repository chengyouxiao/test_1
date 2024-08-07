import pyodbc
import pandas as pd
import os
from sqlalchemy import URL, create_engine, text

from dotenv import load_dotenv
load_dotenv(override=True)

class SqlConnection(object):
    def __init__(self):
        connection_string = (f"DRIVER={{FreeTDS}};"
                         f"Server={os.environ['SQL_SERVER']};"
                         f"Database={os.environ['SQL_DB']};"
                         f"UID={os.environ.get("SQL_USER")};"
                         f"PWD={os.environ.get("SQL_PWD")};"
                         "TDS_Version=7.4;Port=1433;UseNTLMv2=Yes;Authentication=NTLM")
    
        self.__engine = create_engine(URL.create(
           "mssql+pyodbc", query={"odbc_connect": connection_string}))

    def execute(self,qry):
        with self.__engine.connect() as conn:
            conn.execute(text(qry))
   
    def select(self,qry):
        return pd.read_sql_query(text(qry), self.__engine)
    

rms_db = SqlConnection()
