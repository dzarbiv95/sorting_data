import connection_db as db
import setup
from datetime import datetime


def sort_data(data, col_num=2):
    """
    This function get list of data and sort it by any column that selected
    :param data: list of tuples
    :param col_num: the index of the column that the sort will be carried out according to it
    :return:
    """
    def col_key(record):
        return record[col_num]
    data.sort(key=col_key)


def sort_process(conn):
    start_time = datetime.now()

    data = db.fetch_all_data(conn, setup.ADS_TABLE_NAME)
    sort_data(data)
    db.insert_data(conn, setup.RESULTS_STEP1_TABLE_NAME, data)

    end_time = datetime.now()
    time_delta = end_time - start_time
    return time_delta.total_seconds()


if __name__ == '__main__':
    conn = db.connection()
    process_time = sort_process(conn)
    statistics_data = [("Sorting-step1", process_time)]
    db.insert_data(conn, setup.STATISTICS_TABLE_NAME, statistics_data)
