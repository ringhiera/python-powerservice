""" Tests for the MapReduce"""
from unittest.mock import MagicMock

import pandas as pd

from powerservice.client import PersistenceUnit


def test_save_results():
    trade_date_str = "20220801"
    trade_time_str = "0000"
    aggregated_data = pd.DataFrame()
    aggregated_data.to_csv = MagicMock()

    trade_exceptions_df = pd.DataFrame()
    trade_exceptions_df.to_csv = MagicMock()

    data_quality_summary = pd.DataFrame()
    data_quality_summary.to_csv = MagicMock()

    persistence_unit = PersistenceUnit('C:/testing/')
    actual = persistence_unit.save_results(trade_date_str, trade_time_str, aggregated_data, trade_exceptions_df,
                                           data_quality_summary)

    aggregated_data.to_csv.assert_called_once_with('C:/testing/PowerPosition_20220801_0000.csv', index=False)
    trade_exceptions_df.to_csv.assert_called_once_with('C:/testing/PowerPosition_20220801_0000_data_profiling.csv',
                                                       index=False)
    data_quality_summary.to_csv.assert_called_once_with('C:/testing/PowerPosition_20220801_0000_data_quality.csv')
