import os
import sqlite3
from datetime import datetime

import pandas as pd

from objects.db_connection import DBConnection
# db = DBConnection(db_name=os.path.join(os.getcwd(), 'data', 'app_data'))

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
KRAKEN_COMPUTED_INTERVALS = sorted([1, 5, 15, 60, 720, 1440])

pd.set_option('display.max_colwidth', None)
conn = sqlite3.connect(os.path.join(DATA_DIR, 'app.db'))
cursor = conn.cursor()


def load_historical_kraken_exports():
    for interval in KRAKEN_COMPUTED_INTERVALS:
        print(f'creating kraken_price_{interval} table')
        df = pd.read_csv(
            os.path.join(DATA_DIR, 'import', f'XBTUSD_{interval}.csv'),
            sep=',',
            names=['time', 'open', 'high', 'low', 'close', 'volume', 'trades'],
            index_col=['time'],
        )
        df.to_sql(
            name=f'kraken_price_{interval}',
            con=conn,
            if_exists='replace',
        )


def __determine_optimal_source_interval(desired_interval: int) -> int:
    assert desired_interval > 0, 'Interval must be an integer greater than 0'
    computed_intervals = KRAKEN_COMPUTED_INTERVALS
    prev_num = computed_intervals[0]

    for interval in computed_intervals:
        if desired_interval == interval:
            return interval
        elif desired_interval < interval:
            return prev_num
        prev_num = interval
    else:
        return computed_intervals[len(computed_intervals) - 1]


def calc_spot_price_by_minute(num_minutes: int = 1440) -> pd.DataFrame:
    source_interval = __determine_optimal_source_interval(num_minutes)
    print(f'Most optimal soure_interval: {source_interval}')
    source_df = pd.read_sql(f'select * from kraken_price_{source_interval};', con=conn)
    if num_minutes in KRAKEN_COMPUTED_INTERVALS:
        return source_df
    desired_col_order = list(source_df.columns)
    source_df['interval_group'] = source_df['time'].apply(lambda x: (x / 60) // num_minutes)
    source_df['time_rank_by_interval'] = source_df.groupby('interval_group')['time'].rank('average')

    open_series = source_df.groupby('interval_group')['open'].min()
    close_series = source_df.groupby('interval_group')['close'].max()
    sum_agg_series = source_df.groupby('interval_group')[['volume', 'trades']].sum()
    oldest_ts_series = source_df.groupby('interval_group')[['time', 'low']].min()
    latest_ts_series = source_df.groupby('interval_group')['high'].max()

    result_df = sum_agg_series \
        .join(
            oldest_ts_series,
            on=['interval_group'],
            how='inner',
        ) \
        .join(
            latest_ts_series,
            on=['interval_group'],
            how='inner',
        ) \
        .join(
            open_series,
            on=['interval_group'],
            how='inner',
        ) \
        .join(
            close_series,
            on=['interval_group'],
            how='inner',
        )
    result_df.reset_index(drop=True, inplace=True)
    result_df = result_df[desired_col_order]
    return result_df


# load_historical_kraken_exports()
df_720 = calc_spot_price_by_minute(721)
print(df_720.head(5))

# TODO:
#  1. historical data has been loaded, use the API to get the missing data.
#  2. determine if hitting the API only for the second interval and then aggregating is faster than hitting for them all
#  3. write a python script to download the hisotrical zip extracts automatically and load them
#  4. create a table to store metadata on latest time which has been loaded, if a new quarter has passed,
#  potentially download the extracts instead of hitting api for such a long time peroid

# it will take ~2 seconds to load a single day at a 1 minute interval

conn.commit()
conn.close()
