import connection_db as db
import setup
import sort_task
from datetime import datetime


def merge_sort_data(*args, col_idx=2):
    """
    this function receive two sorted lists of data tuples and merge him to large sorted list.
    the sort based on one column in the data lists.

    :param col_idx: the index of the sorted column for the merging
    :param args: same sorted lists of data tuples
    :return: large sorted list of data tuples
    """
    args = [data.copy() for data in args]
    all_data = []
    while any(args):
        min_idx = None
        for i in range(len(args)):
            if not args[i]:
                continue
            elif min_idx is None:
                min_idx = i
            elif args[i][0][col_idx] < args[min_idx][0][col_idx]:
                min_idx = i
        all_data.append(args[min_idx].pop(0))

    return all_data


def sort_merge_process(conn):
    start_time = datetime.now()

    gen_data = db.fetch_chunk_data(conn, setup.ADS_TABLE_NAME, size=2000)
    all_data = []
    for data in gen_data:
        if not data:
            continue
        sort_task.sort_data(data)
        all_data.append(data)

    sorted_data = merge_sort_data(*all_data)
    db.insert_data(conn, setup.RESULTS_STEP2_TABLE_NAME, sorted_data)
    end_time = datetime.now()
    time_delta = end_time - start_time
    return time_delta.total_seconds()


if __name__ == '__main__':
    conn = db.connection()
    process_time = sort_merge_process(conn)
    statistics_data = [("Sorting-step2", process_time)]
    db.insert_data(conn, setup.STATISTICS_TABLE_NAME, statistics_data)