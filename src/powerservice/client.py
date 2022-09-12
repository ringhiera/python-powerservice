import os
import time
from datetime import datetime

import pandas as pd

from powerservice.trading import get_trades


class DataValidator:
    """
        This class implements an injectable data validator.
        The main method is validate(df), taking the input dataframe and returning a validated df
        it returns the validated dataframe with coulmns
        - invalid_time_format: true if timeformat is not "HH:MM"
        - unexpected_time: true if time is not on the 5mins mark
        - missing_time: if any record in the 24h/5min slot is missing
        - invalid_volume: if the volume is missing
    """

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        The map_reduce processing step expect the input data to be valid.
        This method orchestrates a battery of validators to
        mark valid and invalid data rows, so that they can be filtered out.
        :param df: The input dataframe for validation
        :return: The input dataframe augmented with the columns carrying the validation flags
        """
        # work on a copy of data to preserve the input dataframe from side effects
        validated_df = df.copy()
        validated_df = df.copy()
        validated_df = self._check_invalid_time_format(validated_df)
        validated_df = self._check_invalid_volume(validated_df)
        validated_df = self._check_unexpected_time(validated_df)
        validated_df = self._check_missing_time(validated_df)
        return validated_df

    def _check_invalid_time_format(self, validated_df: pd.DataFrame) -> pd.DataFrame:
        """
        The processor expect the time column to be formatted as HH:MM
        Therefore, we are marking all rows where volume is not populated or not parsing to the pattern
        :param validated_df: The DataFrame with the (partially) validated trades used for data quality profiling
        :return: The input dataframe augmented with invalid_time_format column marking any invalid value in the time column
        """
        # check if time is valid (we'll need to use apply as time checking in pandas is not vectorized
        validated_df['invalid_time_format'] = ~validated_df['time'].apply(self._is_hh_mm_time)
        return validated_df

    @staticmethod
    def _check_invalid_volume(validated_df: pd.DataFrame) -> pd.DataFrame:
        """
        The processor expect the volume column to be numeric and populated
        NOTE: Given no specific requirement there is no hard condition of positivity implemented.
        Therefore, we are marking all rows where volume is not populated or not parsing to a number
        :param validated_df: The DataFrame with the (partially) validated trades used for data quality profiling
        :return: The input dataframe augmented with invalid_volume column marking any invalid value in the volume column
        """
        # check invalid or missing volume
        validated_df['invalid_volume'] = pd.to_numeric(validated_df['volume'], errors='coerce').isnull()
        return validated_df

    @staticmethod
    def _check_unexpected_time(validated_df: pd.DataFrame) -> pd.DataFrame:
        """
        The processor expect the input to be provided on a 5 minutes frequency starting on midnight
        Therefore, we are expecting one row for each 5 minute interval of the day.
        This method identifies any interval not aligned with the frequency frequency
        :param validated_df: The DataFrame with the (partially) validated trades used for data quality profiling
        :return: The input dataframe augmented with unexpected_time column marking any misaligned row
        """
        # check times not on 5min interval
        unexpected_time = pd.DataFrame(pd.date_range("00:00", "23:59", freq="5min"), columns=['time'])
        unexpected_time['time'] = unexpected_time['time'].dt.strftime("%H:%M")
        unexpected_time['unexpected_time'] = False
        validated_df = validated_df.merge(unexpected_time, how='left', on='time')
        validated_df['unexpected_time'] = validated_df['unexpected_time'].fillna(True)
        return validated_df

    @staticmethod
    def _check_missing_time(validated_df: pd.DataFrame) -> pd.DataFrame:
        """
        The processor expect the input to be provided on a 5 minutes frequency starting on midnight
        Therefore, we are expecting one row for each 5 minute interval of the day.
        This method identifies missing intervals
        :param validated_df: The DataFrame with the (partially) validated trades used for data quality profiling
        :return: The input dataframe augmented with missing_time column and a new row for each missing interval
        """
        # check missing records on expected 5 minutes timeslots
        missing_time = pd.DataFrame(pd.date_range("00:00", "23:59", freq="5min"), columns=['time'])
        missing_time['time'] = missing_time['time'].dt.strftime("%H:%M")
        missing_time['missing_time'] = True
        temp_df = missing_time.merge(validated_df, how='left', on='time')
        temp_df['missing_time'] = temp_df['id'].isnull()
        temp_df = temp_df[temp_df['id'].isnull()]
        validated_df = pd.concat([validated_df, temp_df])
        validated_df['missing_time'] = validated_df['missing_time'].fillna(False)
        validated_df.loc[:, ['date', 'id']] = validated_df.loc[:, ['date', 'id']].ffill()
        return validated_df

    @staticmethod
    def _is_hh_mm_time(time_string):
        """
        Returns true is a string is in format HH:MM else false.
        Basically a wrapper around a try/except block as python does not have a native function
        :param time_string: The DataFrame with the validated trades used for data quality profiling
        :return: True if time_string is formatted as HH:MM, False otherwise
        """
        try:
            time.strptime(str(time_string), '%H:%M')
            return True
        except ValueError:
            return False

    @staticmethod
    def get_valid_trades(validated_trades):
        """
        Given a validated dataframe it returns rows not marked with some data quality issue
        :param validated_trades_df: The DataFrame with the validated trades used for data quality profiling
        :return: The dataframe with the rows without data quality issues
        """
        return validated_trades[~(
            validated_trades['missing_time'] |
            validated_trades['invalid_time_format'] |
            validated_trades['unexpected_time'] |
            validated_trades['invalid_volume']
        )]

    @staticmethod
    def get_trade_exceptions(validated_trades_df):
        """
        Given a validated dataframe it returns rows marked with some issue
        :param validated_trades_df: The DataFrame with the validated trades used for data quality profiling
        :return: The dataframe with the data quality exception
        """
        return validated_trades_df[(
            validated_trades_df['missing_time'] |
            validated_trades_df['invalid_time_format'] |
            validated_trades_df['unexpected_time'] |
            validated_trades_df['invalid_volume']
        )]

    @staticmethod
    def get_data_quality_summary(validated_trades_df):
        """
        Given a validated dataframe it returns the data quality statistics per trade id
        :param validated_trades_df: The DataFrame with the validated trades used for data quality profiling
        :return: The dataframe with the data quality summary

        """
        df = validated_trades_df.groupby('id').agg({
            'missing_time': 'sum',
            'invalid_time_format': 'sum',
            'unexpected_time': 'sum',
            'invalid_volume': 'sum',
            'time': ['min', 'max']
        })
        df.columns = df.columns.map('_'.join)
        df.columns = df.columns.str.replace("_sum", "_count")
        return df


class MapReduce:
    def __init__(self, input_timezone='Europe/London', output_timezone='UTC'):
        self.input_timezone = input_timezone
        self.output_timezone = output_timezone

    def map_reduce(self, valid_trades_df: pd.DataFrame) -> pd.DataFrame:
        """
        Given a valid dataframe with trade volumes on a 5 min frequency,
        returns a dataframe aggregating the data on an hourly basis,
        remapping time from input timezone to output

        :param valid_trades_df: The DataFrame with the validated trades used for aggregation
        :return: The dataframe with the trades aggregated on hourly basis
        """

        # Parsing date and time to create an index we can aggregate on
        valid_trades_df['timestamp'] = \
            pd.to_datetime(valid_trades_df.agg('{0[date]} {0[time]}'.format, axis=1), dayfirst=True) \
                .dt.tz_localize(self.input_timezone)
        valid_trades_df = valid_trades_df.set_index(['timestamp'])

        # Aggregating the data on an hourly basis
        aggregated_trades = valid_trades_df.groupby(pd.Grouper(freq='60Min'))['volume'].sum().reset_index()

        # Mapping timestamp to UTC and then to HH:MM
        aggregated_trades['Local Time'] = \
            aggregated_trades['timestamp'].dt.tz_convert(self.output_timezone).dt.strftime("%H:%M")
        aggregated_trades['Volume'] = aggregated_trades['volume']
        aggregated_trades = aggregated_trades[['Local Time', 'Volume']]
        return aggregated_trades


class PersistenceUnit():

    def __init__(self, output_path):
        self._output_path = output_path

    def save_results(self, trade_date_str, trade_time_str, aggregated_data, trade_exceptions_df, data_quality_summary):
        """
        The method saves the input dataframes in the configured path using a file format like
        PowerPosition_YYYYMMDD_HHMM.csv,
        PowerPosition_YYYYMMDD_HHMM_data_profiling.csv,
        PowerPosition_YYYYMMDD_HHMM_data_quality.csv respectively.
        trade_date_str is replaced for YYYYMMDD and trade_time_str is replaced for HHMM
        For sake of this exercise, the file pattern is hardcoded, not parametrized.
        :param trade_date_str: The trade date in string format YYYYMMDD
        :param trade_time_str: The trade time in string format HHMM
        :param aggregated_data: The dataframe with the trades aggregated on hourly basis
        :param trade_exceptions_df: The dataframe with the exceptions found by the validator
        :param data_quality_summary: The Data quality summary for
        :return: None
        """
        filename = f"PowerPosition_{trade_date_str}_{trade_time_str}.csv"
        aggregated_data.to_csv(os.path.join(self._output_path, filename), index=False)

        filename = f"PowerPosition_{trade_date_str}_{trade_time_str}_data_profiling.csv"
        trade_exceptions_df.to_csv(os.path.join(self._output_path, filename), index=False)

        filename = f"PowerPosition_{trade_date_str}_{trade_time_str}_data_quality.csv"
        data_quality_summary.to_csv(os.path.join(self._output_path, filename))


class PetroineosChallenge:
    def __init__(self, validator, map_reduce, persistence_unit):
        self._validator = validator
        self._map_reduce = map_reduce
        self._persistence_unit = persistence_unit

    def process(self, trades: list[dict], trade_date) -> tuple[pd.DataFrame]:
        """
        The method takes the raw input from teh API, validates it, profiles the data quality and persists the results.
        The process is achieved using a Bridge pattern and delegating the core fuctions to handlers injected in the constructor.
        This make the processor agnostic to the actual processing achieving loose coupling between the parts
        and making testing easier.

        :param trades: A list of dictionaries as provided by the Mock-API
        :param trade_date: The trade date used to fetch the data from the Mock-API
        :return: a copy of aggregated_data, trade_exceptions, data_quality_summary as persisted
        """
        trade_exceptions_list = []
        valid_trades_list = []
        validated_trades_list = []
        # parse all trades in the
        for trade in trades:
            trade_df = pd.DataFrame(trade)
            validated_trades = self._validator.validate(trade_df)
            validated_trades_list.append(validated_trades)
            valid_trades_list.append(self._validator.get_valid_trades(validated_trades))
            trade_exceptions_list.append(self._validator.get_trade_exceptions(validated_trades))

        valid_trades_df = pd.concat(valid_trades_list)
        trade_exceptions_df = pd.concat(trade_exceptions_list)
        validated_trades_df = pd.concat(validated_trades_list)
        data_quality_summary = self._validator.get_data_quality_summary(validated_trades_df)
        aggregated_data = self._map_reduce.map_reduce(valid_trades_df)

        # Save results
        trade_time_str = "0000"
        trade_date_str = datetime.strftime(datetime.strptime(trade_date, "%d/%m/%Y"), "%Y%m%d")
        self._persistence_unit.save_results(trade_date_str, trade_time_str, aggregated_data, trade_exceptions_df,
                                            data_quality_summary)

        return aggregated_data, trade_exceptions_df, data_quality_summary


def main(trade_date: str, output_path: str):
    """
    This is a very simple method showing the invocation sequence and the dependency injection
    """
    # Extract data for provider
    trades = get_trades(date=trade_date)

    # Configure and instantiate the processor
    processor = PetroineosChallenge(DataValidator(), MapReduce(), PersistenceUnit(output_path))

    # process data
    processor.process(trades, trade_date)


if __name__ == '__main__':
    # those values can come from caller or from configuration depending on the runtime design decisions
    trade_date = '01/08/2022'
    output_path = 'c:/tmp'

    main(trade_date, output_path)
