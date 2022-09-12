# THis is not the very best place to put this module.
# in a production setup we should have all non-prod utils nicely bundled together and imported in the test_require dependencies
# For sake of this example I am putting all modules together to keep the structure simple

from io import StringIO

import pandas as pd


def str_to_df(s, rstrip_str='_', index_col=0, **kwargs):
    s = s.lstrip('\n').rstrip(' ')
    df = pd.read_fwf(StringIO(s), index_col=index_col, **kwargs)
    if rstrip_str is not None:
        df.columns = [s.rstrip(rstrip_str) for s in df.columns]
        if df.index.name is not None:
            df.index.name.rstrip(rstrip_str)
    return df
