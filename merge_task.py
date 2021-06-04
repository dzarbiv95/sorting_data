import connection_db as db
import setup
import sort_task
from datetime import datetime


def marge_sort_data(data1, data1_col_idx, data2, data2_col_idx):
    """
    this function receive two sorted lists of data tuples and merge him to large sorted list.
    the sort based on one column in each list data.
    :param data1: first sorted list of data tuples
    :param data1_col_idx: the index of the sorted column in the first list
    :param data2: second sorted list of data tuples
    :param data2_col_idx: the index of the sorted column in the second list
    :return: large sorted list of data tuples
    """
    if not data1:
        return data2
    if not data2:
        return data1

    all_data = []
    idx1 = 0
    idx2 = 0
    while idx1 < len(data1) and idx2 < len(data2):
        if data1[idx1][data1_col_idx] < data2[idx2][data2_col_idx]:
            all_data.append(data1[idx1])
            idx1 += 1
        else:
            all_data.append(data2[idx2])
            idx2 += 1

    if idx1 == len(data1):
        all_data = all_data + data2[idx2:]
    else:
        all_data = all_data + data1[idx1:]

    return all_data


def sort_merge_process(conn):
    start_time = datetime.now()

    gen_data = db.fetch_chunk_data(conn, setup.ADS_TABLE_NAME, size=2000)
    all_data = []
    for data in gen_data:
        if not data:
            continue
        sort_task.sort_data(data)
        all_data = marge_sort_data(all_data, 2, data, 2)
    db.insert_data(conn, setup.RESULTS_STEP2_TABLE_NAME, all_data)

    end_time = datetime.now()
    time_delta = end_time - start_time
    return time_delta.total_seconds()


if __name__ == '__main__':
    conn = db.connection()
    process_time = sort_merge_process(conn)
    statistics_data = [("Sorting-step2", process_time)]
    db.insert_data(conn, setup.STATISTICS_TABLE_NAME, statistics_data)
