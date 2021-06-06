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


def fetch_chunk_data(conn, table_name, size=2000):
    query = f"SELECT * FROM {table_name}"
    cur = conn.cursor()
    cur.execute(query)
    chunk = cur.fetchmany(size)
    yield chunk
    while chunk:
        chunk = cur.fetchmany(size)
        yield chunk


def fetch_first_words(conn, table_name, top=None):
    cur = conn.cursor()
    cur.execute(f"select DATA from {table_name}")
    if top:
        data = cur.fetchmany(top)
    else:
        data = cur.fetchall()
    return data