import pyodbc

def clear_table(conn_str, table_name):
    conn = pyodbc.connect(conn_str)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"truncate table {table_name}")
    conn.close()


def copy_table(conn_str, old_table, new_table):
    conn = pyodbc.connect(conn_str)
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(f"IF OBJECT_ID(N'{new_table}', N'U') IS NOT NULL DROP TABLE [{new_table}]")
    cursor.execute(f"SELECT * INTO {new_table} FROM {old_table}")
    cursor.execute(f"SELECT name, type_desc, is_primary_key FROM sys.indexes WHERE object_id = OBJECT_ID('{old_table}')")
    indexes = cursor.fetchall()

    for index in indexes:
        if index[1] == "CLUSTERED" and index[2] == 1: 
            print(f"Creating primary key and clustered index on {new_table}(YCSB_KEY)")
            if new_table == 'usertable':
                cursor.execute(f"ALTER TABLE {new_table} ADD CONSTRAINT PK_USERTABLE PRIMARY KEY CLUSTERED (YCSB_KEY)")
            else:
                cursor.execute(f"ALTER TABLE {new_table} ADD CONSTRAINT PK_YCSB_KEY PRIMARY KEY CLUSTERED (YCSB_KEY)")
        elif index[1] == "CLUSTERED":
            pass
        else:
            print(f"Creating index {index[0]} on {new_table}({index[0]})")
            cursor.execute(f"CREATE INDEX {index[0]} ON {new_table}({index[0]})")

    conn.close()

    