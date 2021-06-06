import connection_db as db
import setup
import sort_task
import merge_task
from datetime import datetime
from threading import Thread


def parallel_merge(data_gen):
    all_data = []
    all_threads = []
    for data in data_gen:
        all_data.append(data)
        thread = Thread(target=sort_task.sort_data, args=(data,))
        all_threads.append(thread)
        thread.start()

    sorted_data = []
    for i, thread in enumerate(all_threads):
        thread.join()
        sorted_data = merge_task.marge_sort_data(sorted_data, all_data[i], 2)

    return sorted_data


def process_parallel_merging(conn):
    start_time = datetime.now()
    data_gen = db.fetch_chunk_data(conn, setup.ADS_TABLE_NAME, 2000)
    data = parallel_merge(data_gen)
    db.insert_data(conn, setup.RESULTS_STEP3_TABLE_NAME, data)
    end_time = datetime.now()
    time_delta = end_time - start_time
    return time_delta.total_seconds()


if __name__ == '__main__':
    conn = db.connection()
    process_time = process_parallel_merging(conn)
    statistics_data = [("Sorting-step3", process_time)]
    db.insert_data(conn, setup.STATISTICS_TABLE_NAME, statistics_data)