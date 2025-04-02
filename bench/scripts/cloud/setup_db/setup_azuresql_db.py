import argparse
import pyodbc

DEFAULT_DB = 'ycsb'
DEFAULT_TABLE = 'usertable'
DEFAULT_SERVER = 'your_server.database.windows.net'
DEFAULT_PORT = 1433
DEFAULT_USER = 'your_username'
DEFAULT_PASSWORD = 'your_password'

def setup_azure_sql(db, table, server, port, user, password):
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server},{port};DATABASE={db};UID={user};PWD={password}'
    conn = pyodbc.connect(conn_str)
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(f"IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE [{table}]")
    cursor.execute(f"CREATE TABLE {table} ("
                    "YCSB_KEY varchar(255) NOT NULL,"
                    "FIELD0 varchar(100) NOT NULL, "
                    "FIELD1 varchar(100) NOT NULL, " 
                    "FIELD2 varchar(100) NOT NULL, " 
                    "FIELD3 varchar(100) NOT NULL, " 
                    "FIELD4 varchar(100) NOT NULL, " 
                    "FIELD5 varchar(100) NOT NULL, " 
                    "FIELD6 varchar(100) NOT NULL, " 
                    "FIELD7 varchar(100) NOT NULL, " 
                    "FIELD8 varchar(100) NOT NULL, " 
                    "FIELD9 varchar(100) NOT NULL "
                    "CONSTRAINT pk_usertable PRIMARY KEY (YCSB_KEY));")

    conn.close()
    print(f'Prepared an empty {table} table in {db} database')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Setup Azure SQL Database for benchmarking.')
    parser.add_argument('--db', default=DEFAULT_DB, help='Database name')
    parser.add_argument('--table', default=DEFAULT_TABLE, help='Table name')
    parser.add_argument('--server', default=DEFAULT_SERVER, help='Azure SQL server name')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT, help='Azure SQL server port')
    parser.add_argument('--user', default=DEFAULT_USER, help='Username for Azure SQL')
    parser.add_argument('--password', default=DEFAULT_PASSWORD, help='Password for Azure SQL')

    args = parser.parse_args()
    setup_azure_sql(args.db, args.table, args.server, args.port, args.user, args.password)