#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command()
@click.option('--pg-user', default='root', help='Postgres username')
@click.option('--pg-pass', default='root', help='Postgres password')
@click.option('--pg-host', default='localhost', help='Postgres host')
@click.option('--pg-port', default=5433, type=int, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', help='Postgres database name')
@click.option('--year', default=2021, type=int, help='Data year')
@click.option('--month', default=1, type=int, help='Data month')
@click.option('--target-table', default='yellow_taxi_data', help='Target DB table name')
@click.option('--chunksize', default=100000, type=int, help='CSV read chunksize')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize, url):

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
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