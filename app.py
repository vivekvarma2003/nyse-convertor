from dask import dataframe as dd
columns = ['ticker', 'trade_date', 'open_price', 'low_price', 'high_price', 'close_price', 'volume']


def convert_file_to_json():
    df = dd.read_csv(
        'data/nyse_all/nyse_data/*',
        names=columns,
        blocksize=None
        )   
    df.to_json(
        'data/nyse_all/nyse_json/part-*.json.gz',
        orient = 'records',
        lines=True,
        compression='gzip',
        name_function=lambda i: '%05d' % i
    )
if __name__=='__main__':
    convert_file_to_json()