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
        min_value = data[0][col_num]
        for row in data:
            if min_value > row[col_num]:
                return False
            min_value = row[col_num]
        return True

    def test_merge_equals_len(self):
        all_data = []
        for i in range(5):
            data = next(self.data_gen)
            sort_task.sort_data(data)
            all_data.append(data)
            merged_data = merge_task.merge_sort_data(*all_data, col_idx=2)
            self.assertTrue(self.is_sorted(merged_data, 2))
            self.assertEqual(len(merged_data), sum([len(data) for data in all_data]))
            for data in all_data:
                for rec in data:
                    self.assertIn(rec, merged_data)

    def test_merge_empties(self):
        self.assertEqual(merge_task.merge_sort_data([], [], col_idx=2), [])

if __name__ == "__main__":
    unittest.main()