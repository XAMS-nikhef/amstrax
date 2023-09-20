import os
import unittest
import numpy as np
import matplotlib.pyplot as plt

import amstrax

class TestPlottingFunctions(unittest.TestCase):
    """
    Test cases for functions in plotting.py
    """

    @classmethod
    def setUpClass(cls) -> None:
        # Mock data for raw_records and records
        cls.mock_raw_records = np.array([
            {'channel': 0, 'time': 0, 'length': 10, 'dt': 1, 'data': np.arange(10)},
            {'channel': 1, 'time': 10, 'length': 10, 'dt': 1, 'data': np.arange(10, 20)}
        ], dtype=amstrax.raw_record_dtype())

        cls.mock_records = np.array([
            {'channel': 0, 'time': 20, 'length': 10, 'dt': 1, 'data': np.arange(20, 30)},
            {'channel': 1, 'time': 30, 'length': 10, 'dt': 1, 'data': np.arange(30, 40)}
        ], dtype=amstrax.record_dtype())

        # Mock context and run_id
        cls.mock_context = None  # This can be replaced with a mock strax.Context if needed
        cls.mock_run_id = '12345'

    def test_plot_records_raw_true(self):
        # Test with raw=True
        amstrax.plot_records(self.mock_context, self.mock_run_id, self.mock_raw_records, self.mock_records, raw=True)
        plt.close()  # Close the plot to prevent it from showing during tests

    def test_plot_records_raw_false(self):
        # Test with raw=False
        amstrax.plot_records(self.mock_context, self.mock_run_id, self.mock_raw_records, self.mock_records, raw=False)
        plt.close()  # Close the plot to prevent it from showing during tests

    def test_plot_records_logy_true(self):
        # Test with logy=True
        amstrax.plot_records(self.mock_context, self.mock_run_id, self.mock_raw_records, self.mock_records, logy=True)
        plt.close()  # Close the plot to prevent it from showing during tests

    # Additional test cases can be added for other functions in a similar manner

if __name__ == '__main__':
    unittest.main()
