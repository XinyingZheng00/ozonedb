import logging
import psycopg2

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)



def copy_table(server, port, db_name, source_table, dest_table, username, password):
    """Copy data from the original table to a cached table within the same database."""
    conn = psycopg2.connect(host=server, port=port, dbname=db_name, user=username, password=password)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {dest_table} (LIKE {source_table} INCLUDING ALL);")
        cur.execute(f"TRUNCATE TABLE {dest_table};")
        cur.execute(f"INSERT INTO {dest_table} SELECT * FROM {source_table};")
    conn.close()

def clear_table(server, port, db_name, table_name, username, password):
    """Clear the usertable in the database only if it exists."""
    conn = psycopg2.connect(host=server, port=port, dbname=db_name, user=username, password=password)
    conn.autocommit = True
    with conn.cursor() as cur:
        # Check if the table exists
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            );
        """, (table_name,))
        table_exists = cur.fetchone()[0]
        if table_exists:
            # If the table exists, truncate it
            cur.execute(f"TRUNCATE TABLE {table_name};")
        else:
            print(f"Table {table_name} does not exist.")
    conn.close()

