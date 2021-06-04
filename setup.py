import connection_db as db
import random
import string
import uuid

ADS_TABLE_NAME = "ADS_DATA"
ADS_TABLE_COLUMNS = {
    "IDX": "INTEGER",
    "ID": "TEXT",
    "DATA": "TEXT"
}
STATISTICS_TABLE_NAME = "RESULTS"
STATISTICS_TABLE_COLUMNS = {
    "STEP_ID": "TEXT",
    "PROCESS_TIME": "REAL"
}
RESULTS_STEP1_TABLE_NAME = "RESULTS1"
RESULTS_STEP1_TABLE_COLUMNS = {
    "IDX": "INTEGER",
    "ID": "TEXT",
    "DATA": "TEXT"
}
RESULTS_STEP2_TABLE_NAME = "RESULTS2"
RESULTS_STEP2_TABLE_COLUMNS = {
    "IDX": "INTEGER",
    "ID": "TEXT",
    "DATA": "TEXT"
}
RESULTS_STEP3_TABLE_NAME = "RESULTS3"
RESULTS_STEP3_TABLE_COLUMNS = {
    "IDX": "INTEGER",
    "ID": "TEXT",
    "DATA": "TEXT"
}



def fill_ads_data(conn, table_name, size=20000):
    def get_random_name():
        length = random.randint(5, 15)
        name = ''
        for i in range(length):
            char = random.choice(string.ascii_letters)
            name = name + char
        return name

    data = []
    for i in range(size):
        row = (i, str(uuid.uuid4()), get_random_name())
        data.append(row)

    db.insert_data(conn, table_name, data)


def create_tables():
    db.create_data_table(conn, ADS_TABLE_NAME, ADS_TABLE_COLUMNS)
    db.create_data_table(conn, STATISTICS_TABLE_NAME, STATISTICS_TABLE_COLUMNS)
    db.create_data_table(conn, RESULTS_STEP1_TABLE_NAME, RESULTS_STEP1_TABLE_COLUMNS)
    db.create_data_table(conn, RESULTS_STEP2_TABLE_NAME, RESULTS_STEP2_TABLE_COLUMNS)
    db.create_data_table(conn, RESULTS_STEP3_TABLE_NAME, RESULTS_STEP3_TABLE_COLUMNS)


if __name__ == '__main__':
    conn = db.connection()
    create_tables()
    fill_ads_data(conn, ADS_TABLE_NAME)

