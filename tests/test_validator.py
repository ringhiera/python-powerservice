""" Tests for the DataValidator"""
from pandas.testing import assert_frame_equal
from powerservice.client import DataValidator
from powerservice.utils_for_testing import str_to_df


def test__check_invalid_time_format():

    input = str_to_df("""
        date______  time_   volume  id
    0   01/08/2022	00:00   312     07d01a7a08c646a68a3cc71b72c337c8
    1   01/08/2022	        312     07d01a7a08c646a68a3cc71b72c337c8
    2   01/08/2022	xy:qw   312     07d01a7a08c646a68a3cc71b72c337c8
    """)

    expected = str_to_df("""
        date______  time_   volume  id______________________________  invalid_time_format
    0   01/08/2022	00:00   312     07d01a7a08c646a68a3cc71b72c337c8  False
    1   01/08/2022	        312     07d01a7a08c646a68a3cc71b72c337c8  True
    2   01/08/2022	xy:qw   312     07d01a7a08c646a68a3cc71b72c337c8  True
    """)

    data_validator = DataValidator()
    actual = data_validator._check_invalid_time_format(input)
    assert_frame_equal(actual, expected)

def test__check_invalid_volume():
    input = str_to_df("""
        date______  time_   volume  id______________________________
    0   01/08/2022	00:00   312     07d01a7a08c646a68a3cc71b72c337c8
    1   01/08/2022	00:05           07d01a7a08c646a68a3cc71b72c337c8
    2   01/08/2022	00:10   xyz     07d01a7a08c646a68a3cc71b72c337c8
    """)

    expected = str_to_df("""
        date______  time_   volume  id______________________________  invalid_volume
    0   01/08/2022	00:00   312     07d01a7a08c646a68a3cc71b72c337c8  False
    1   01/08/2022	00:05           07d01a7a08c646a68a3cc71b72c337c8  True
    2   01/08/2022	00:10   xyz     07d01a7a08c646a68a3cc71b72c337c8  True
    """)

    data_validator = DataValidator()
    actual = data_validator._check_invalid_volume(input)
    assert_frame_equal(actual, expected)



def test__check_unexpected_time():
    input = str_to_df("""
        date______  time_   volume  id______________________________
    0   01/08/2022	00:00   312     07d01a7a08c646a68a3cc71b72c337c8
    1   01/08/2022	00:01   123     07d01a7a08c646a68a3cc71b72c337c8
    2   01/08/2022	25:10   123     07d01a7a08c646a68a3cc71b72c337c8
    """)

    expected = str_to_df("""
        date______  time_   volume  id______________________________  unexpected_time
    0   01/08/2022	00:00   312     07d01a7a08c646a68a3cc71b72c337c8  False
    1   01/08/2022	00:01   123     07d01a7a08c646a68a3cc71b72c337c8  True
    2   01/08/2022	25:10   123     07d01a7a08c646a68a3cc71b72c337c8  True
    """)

    data_validator = DataValidator()
    actual = data_validator._check_unexpected_time(input)
    actual.to_csv('c:/tmp/actual.csv')
    assert_frame_equal(actual, expected)

def test__check_missing_time():
    # Test omitted as it is too verbose for the purpose of this exercise
    # in production setup one can define and designate some predefined files for input and expected results
    pass


def test_get_valid_trades():
    input = str_to_df("""
            date______  time_   volume  id______________________________  invalid_time_format  invalid_volume  unexpected_time  missing_time
        0   01/08/2022	00:00   312     07d01a7a08c646a68a3cc71b72c337c8  False                False           False            False
        1   01/08/2022	25:05   123     07d01a7a08c646a68a3cc71b72c337c8  True                 False           False            False
        2   01/08/2022	00:10           07d01a7a08c646a68a3cc71b72c337c8  False                True            False            False
        3   01/08/2022	00:01   123     07d01a7a08c646a68a3cc71b72c337c8  False                False           True             False
        """)
    expected = str_to_df("""
            date______  time_   volume  id______________________________  invalid_time_format  invalid_volume  unexpected_time  missing_time
        0   01/08/2022	00:00   312.    07d01a7a08c646a68a3cc71b72c337c8  False                False           False            False
        """)

    data_validator = DataValidator()
    actual = data_validator.get_valid_trades(input)
    assert_frame_equal(actual, expected)


def test_get_trade_exceptions():
    input = str_to_df("""
            date______  time_   volume  id______________________________  invalid_time_format  invalid_volume  unexpected_time  missing_time
        0   01/08/2022	00:00   312     07d01a7a08c646a68a3cc71b72c337c8  False                False           False            False
        1   01/08/2022	25:05   123     07d01a7a08c646a68a3cc71b72c337c8  True                 False           False            False
        2   01/08/2022	00:10           07d01a7a08c646a68a3cc71b72c337c8  False                True            False            False
        3   01/08/2022	00:01   123     07d01a7a08c646a68a3cc71b72c337c8  False                False           True             False
        """)
    expected = str_to_df("""
            date______  time_   volume  id______________________________  invalid_time_format  invalid_volume  unexpected_time  missing_time
        1   01/08/2022	25:05   123     07d01a7a08c646a68a3cc71b72c337c8  True                 False           False            False
        2   01/08/2022	00:10           07d01a7a08c646a68a3cc71b72c337c8  False                True            False            False
        3   01/08/2022	00:01   123     07d01a7a08c646a68a3cc71b72c337c8  False                False           True             False
        """)

    data_validator = DataValidator()
    actual = data_validator.get_trade_exceptions(input)
    assert_frame_equal(actual, expected)


def test_get_data_quality_summary():
    input = str_to_df("""
            date______  time_   volume  id______________________________  invalid_time_format  invalid_volume  unexpected_time  missing_time
        1   01/08/2022	25:05   123     07d01a7a08c646a68a3cc71b72c337c8  True                 False           False            False
        2   01/08/2022	00:10           07d01a7a08c646a68a3cc71b72c337c8  False                True            False            False
        3   01/08/2022	00:01   123     07d01a7a08c646a68a3cc71b72c337c8  False                False           True             False
        4   01/08/2022	00:02   123     07d01a7a08c646a68a3cc71b72c337c8  False                False           True             False
        """)
    expected = str_to_df("""
        id                                missing_time_count  invalid_time_format_count  unexpected_time_count  invalid_volume_count  time_min  time_max
        07d01a7a08c646a68a3cc71b72c337c8  0                   1                          2                      1                     00:01     25:05
        """)

    data_validator = DataValidator()
    actual = data_validator.get_data_quality_summary(input)
    assert_frame_equal(actual, expected)
