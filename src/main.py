import os
import sqlite3
from typing import Union

import pandas as pd


DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
KRAKEN_COMPUTED_INTERVALS = sorted([1, 5, 15, 60, 720, 1440])


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


# this is inefficient compared to __table_exists, keep for now as a reference
# def __interval_computed(interval: int) -> bool:
#     df = pd.read_sql(
#         'select name from sqlite_master where type="table"',
#         con=conn,
#     )
#     df['name'] = df['name'].str[len('kraken_price_'):]
#     return str(interval) in df['name'].values


def __table_exists(table_name: Union[str, int]) -> bool:
    return not pd.read_sql(
        'select name from sqlite_master where type="table" and name=?',
        con=conn,
        params=[str(table_name)]
    ).empty


def __determine_optimal_source_interval(desired_interval: int) -> int:
    assert desired_interval > 0, 'Interval must be an integer greater than 0'
    if __table_exists(f'kraken_price_{desired_interval}'):
        print('Using data from pre-computed table')
        return desired_interval
    computed_intervals = KRAKEN_COMPUTED_INTERVALS  # todo: include all intervals computed (existing tables)
    prev_num = computed_intervals[0]

    for interval in computed_intervals:
        if desired_interval == interval:
            return interval
        elif desired_interval < interval:
            return prev_num
        prev_num = interval
    else:
        return computed_intervals[len(computed_intervals) - 1]


def __resolve_source_df(interval_by_min: int) -> pd.DataFrame:
    source_interval = __determine_optimal_source_interval(interval_by_min)
    table_name = f'kraken_price_{source_interval}'
    print(f'Most optimal table to query: {table_name}')
    source_df = pd.read_sql(f'select * from {table_name};', con=conn)
    return source_df


def __transform_source_df(source_df: pd.DataFrame, interval_by_min: int) -> pd.DataFrame:
    desired_col_order = list(source_df.columns)
    source_df['interval_group'] = source_df['time'].apply(lambda x: (x / 60) // interval_by_min)
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


def calc_spot_price_by_minute(interval_by_min: int = 1440) -> pd.DataFrame:
    source_df = __resolve_source_df(interval_by_min)

    if interval_by_min in KRAKEN_COMPUTED_INTERVALS:
        return source_df
    result_df = __transform_source_df(source_df, interval_by_min)

    if not __table_exists(interval_by_min):
        result_df.set_index('time').to_sql(
            name=f'kraken_price_{interval_by_min}',
            con=conn,
            if_exists='replace',
        )
    return result_df


pd.set_option('display.max_colwidth', None)
conn = sqlite3.connect(os.path.join(DATA_DIR, 'app.db'))
cursor = conn.cursor()

# load_historical_kraken_exports()
df_720 = calc_spot_price_by_minute(2)
print(df_720.head(5))

conn.commit()
conn.close()
