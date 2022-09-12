""" Tests for the MapReduce"""
from pandas.testing import assert_frame_equal

from powerservice.client import MapReduce
from powerservice.utils_for_testing import str_to_df


def test_map_reduce():
    input = str_to_df("""
        date______  time_   volume  id______________________________
    0   01/08/2022	00:00   100     07d01a7a08c646a68a3cc71b72c337c8
    1   01/08/2022	00:05   200     07d01a7a08c646a68a3cc71b72c337c8
    2   01/08/2022	00:10   300     07d01a7a08c646a68a3cc71b72c337c8
    4   01/08/2022	01:00   500     07d01a7a08c646a68a3cc71b72c337c8
    5   01/08/2022	01:05   600     07d01a7a08c646a68a3cc71b72c337c8
    6   01/08/2022	01:10   700     07d01a7a08c646a68a3cc71b72c337c8
    7   01/08/2022	00:00   110     07d01a7a08c646a68a3cc71b72c337c9
    8   01/08/2022	00:05   220     07d01a7a08c646a68a3cc71b72c337c9
    9   01/08/2022	00:10   330     07d01a7a08c646a68a3cc71b72c337c9
    """)

    expected = str_to_df("""
~~~~~~~~Local Time~~Volume
~~~~0~~~23:00~~~~~~~1260
~~~~1~~~00:00~~~~~~~1800
    """, delimiter='~')

    map_reduce = MapReduce()
    actual = map_reduce.map_reduce(input)
    assert_frame_equal(actual, expected)
