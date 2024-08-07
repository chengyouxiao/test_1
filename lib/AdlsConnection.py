import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv(override=True)


class AdlsConnection(object):
    def __init__(self):
        self.__connect_str=(
            "Driver={Simba Spark ODBC Driver};"
            f"Host={os.environ['ADLS_HOST']};"
            f"httpPath={os.environ['ADLS_HTTPPATH']};"
            "Port=443;"
            "TransportMode=http;"
            f"Catalog={os.environ['ADLS_CATALOG']};"
            f"Schema={os.environ['ADLS_SCHEMA']};"
            "SSL=1;"
            "AllowSelfSignedServerCert=1;"
            "ThriftTransport=2;"
            "AuthMech=3;"
            f"UID={os.environ['ADLS_UID']};"
            f"PWD={os.environ['ADLS_PWD']};"
        )

    def execute(self,qry):
        with pyodbc.connect(self.__connect_str, autocommit=True) as conn:
            conn.execute(qry)
   
    def select(self,qry):
        with pyodbc.connect(self.__connect_str, autocommit=True) as conn:
            return pd.read_sql_query(qry, conn)



adls_db = AdlsConnection()
