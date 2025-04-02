import argparse
import psycopg2

# Default values
DEFAULT_DB = 'ycsb'
DEFAULT_TABLE = 'usertable'
DEFAULT_IP = '127.0.0.1'
DEFAULT_PORT = 5431
DEFAULT_USER = 'postgres'
DEFAULT_PASSWORD = ''  # Assuming no password by default, update if needed

def setup_postgres(db, table, ip, port, user, password):
    # To create a new database in PostgreSQL
    # We need to connect to an existing database first—typically the default postgres database
    conn = psycopg2.connect(database='postgres', host=ip, port=port, user=user, password=password)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f'drop database if exists {db}')
    cursor.execute(f'create database {db}')
    conn.close()
    
    conn = psycopg2.connect(database=db, host=ip, port=port, user=user, password=password)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f'drop table if exists {table}')
    cursor.execute(f'create table {table} '
                   '(YCSB_KEY VARCHAR(255) PRIMARY KEY, FIELD0 VARCHAR(255), FIELD1 VARCHAR(255), '
                   'FIELD2 VARCHAR(255), FIELD3 VARCHAR(255), FIELD4 VARCHAR(255), FIELD5 VARCHAR(255), '
                   'FIELD6 VARCHAR(255), FIELD7 VARCHAR(255), FIELD8 VARCHAR(255), FIELD9 VARCHAR(255))')
    conn.close()
    print(f'Prepared an empty {table} table in {db} database')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Setup PostgreSQL for benchmarking.')
    parser.add_argument('--db', default=DEFAULT_DB, help='Database name')
    parser.add_argument('--table', default=DEFAULT_TABLE, help='Table name')
    parser.add_argument('--ip', default=DEFAULT_IP, help='PostgreSQL IP address')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT, help='PostgreSQL port')
    parser.add_argument('--user', default=DEFAULT_USER, help='Username for PostgreSQL')
    parser.add_argument('--password', default=DEFAULT_PASSWORD, help='Password for PostgreSQL')

    args = parser.parse_args()
    setup_postgres(args.db, args.table, args.ip, args.port, args.user, args.password)
