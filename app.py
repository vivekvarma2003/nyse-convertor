from dask import dataframe as dd
import os
columns = ['ticker', 'trade_date', 'open_price', 'low_price', 'high_price', 'close_price', 'volume']


def convert_file_to_json():
    src_dir = os.environ.setdefault('SRC_DIR','data/nyse_all/nyse_data')
    tgt_dir = os.environ.setdefault('TGT_DIR','data/nyse_all/nyse_json')
    df = dd.read_csv(
        f'{src_dir}/*',
        names=columns,
        blocksize=None
        )   
    df.to_json(
        f'{tgt_dir}/part-*.json.gz',
        orient = 'records',
        lines=True,
        compression='gzip',
        name_function=lambda i: '%05d' % i
    )
    print(f'{df.compute().shape}')
if __name__=='__main__':
    convert_file_to_json()