import sqlite3


def connection():
    conn = sqlite3.connect('dataDB.db')
    return conn


def create_data_table(conn, table_name, columns):
    cur = conn.cursor()
    columns_str = ','.join([col + " " + columns[col] for col in columns])
    query = f"""CREATE TABLE {table_name} ({columns_str})"""
    cur.execute(query)


def insert_data(conn, table_name, data):
    if not data:
        return
    cur = conn.cursor()
    cols_str = ','.join(['?' for val in data[0]])
    query = f"INSERT INTO {table_name} VALUES({cols_str})"
    cur.executemany(query, data)
    conn.commit()


def fetch_all_data(conn, table_name):
    query = f"SELECT * FROM {table_name}"
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()
