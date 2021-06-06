import connection_db as db
import merge_task
import setup
import unittest
import sort_task


class TestMergeMethod(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = db.connection()
        self.data_gen = db.fetch_chunk_data(conn, setup.ADS_TABLE_NAME, size=5)

    @staticmethod
    def is_sorted(data, col_num):
        if not data:
            return True
        start_value = data[0][col_num]
        for row in data:
            if start_value > row[col_num]:
                return False
        return True

    def test_merge_equals_len(self):
        data1 = next(self.data_gen)
        sort_task.sort_data(data1)
        data2 = next(self.data_gen)
        sort_task.sort_data(data2)
        merged_data = merge_task.marge_sort_data(data1, data2, 2)
        self.assertTrue(self.is_sorted(merged_data, 2))
        self.assertEqual(len(merged_data), len(data1) + len(data2))

    def test_merge_filst_long(self):
        data1 = next(self.data_gen) + next(self.data_gen) + next(self.data_gen)
        sort_task.sort_data(data1)
        data2 = next(self.data_gen)
        sort_task.sort_data(data2)
        merged_data = merge_task.marge_sort_data(data1, data2, 2)
        self.assertTrue(self.is_sorted(merged_data, 2))
        self.assertEqual(len(merged_data), len(data1) + len(data2))

    def test_merge_second_long(self):
        data1 = next(self.data_gen) + next(self.data_gen) + next(self.data_gen)
        sort_task.sort_data(data1)
        data2 = next(self.data_gen)
        sort_task.sort_data(data2)
        merged_data = merge_task.marge_sort_data(data1, data2, 2)
        self.assertTrue(self.is_sorted(merged_data, 2))
        self.assertEqual(len(merged_data), len(data1) + len(data2))

    def test_merge_first_empty(self):
        data1 = []
        data2 = next(self.data_gen)
        sort_task.sort_data(data2)
        merged_data = merge_task.marge_sort_data(data1, data2, 2)
        self.assertTrue(self.is_sorted(merged_data, 2))
        self.assertEqual(len(merged_data), len(data2))

    def test_merge_second_empty(self):
            data1 = next(self.data_gen)
            sort_task.sort_data(data1)
            data2 = []
            merged_data = merge_task.marge_sort_data(data1, data2, 2)
            self.assertTrue(self.is_sorted(merged_data, 2))
            self.assertEqual(len(merged_data), len(data1))

    def test_merge_emptys(self):
        self.assertEqual(merge_task.marge_sort_data([], [], 2), [])

if __name__ == "__main__":
    unittest.main()