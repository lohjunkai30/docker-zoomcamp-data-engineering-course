#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pyarrow.parquet as pq
import fsspec
import os
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--pg-user', default='root', help='Postgres username')
@click.option('--pg-pass', default='root', help='Postgres password')
@click.option('--pg-host', default='localhost', help='Postgres host')
@click.option('--pg-port', default=5432, type=int, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', help='Postgres database name')
@click.option('--target-table', help='Target DB table name')
@click.option('--chunksize', default=100000, type=int, help='CSV read chunksize')
@click.option('--url', help='URL link to CSV')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize, url):

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    if url.endswith('.parquet') or url.endswith('.pq'):
        with fsspec.open(url) as f:
            parquet_file = pq.ParquetFile(f)
            first = True
            with tqdm(desc=f"Ingesting {target_table}", unit="batch") as pbar:
                for batch in parquet_file.iter_batches(batch_size = chunksize):
                    df = batch.to_pandas()
                    if first:
                        df.to_sql(name=target_table, con=engine, if_exists = 'replace')
                        first = False
                    else:
                        df.to_sql(name=target_table, con=engine, if_exists = 'append')
                    pbar.update(1)

    elif url.endswith('.csv'):
        df_iter = pd.read_csv(
            url,
            iterator=True,
            chunksize=chunksize
        )

        first = True
        for df_chunk in tqdm(df_iter):
            if first:
                df_chunk.head(0).to_sql(target_table, 
                con=engine, 
                if_exists='replace')
                first = False
                print('Table created')

            df_chunk.to_sql(target_table, con=engine, if_exists='append')
            print("Inserted", len(df_chunk))


if __name__ == '__main__':
    run()