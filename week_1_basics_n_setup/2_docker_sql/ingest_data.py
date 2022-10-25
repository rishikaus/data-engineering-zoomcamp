#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pwd

from time import time

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

# random bullshit to try and solve for parquet
# ftype = 0

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    
    # RK - in 2022 the files are parquet so this expanding this into if..elif..else to try and cover parquet and csv and gzip cases
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.parquet'):
        file_name = 'output.parquet'
        print('parquet file')
        #ftype == 1
    elif url.endswith('.csv.gz'):
        file_name = 'output.csv.gz'
        print('gzipped csv file')
        #ftype == 2
    else:
        file_name = 'output.csv'
        print('csv file')
        #ftype == 3
    
    os.system(f"wget {url} -O {file_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # RK - modifying this section with if..elif..else to cover csv or parquet cases
    # if ftype == 0:
    #     print('something got fucked on import')  
    # elif ftype == 1:
    #     print('file type is parquet')
    #     df_iter = pd.read_parquet(file_name, iterator=True, chunksize=100000)
    # elif ftype == 2:
    #     print('gzipped csv - not sure what to do')
    # elif ftype == 3:
    #     print ('file type is csv')
    #     df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)

    df_parquet = pd.read_parquet(file_name)
    df_parquet.to_csv('$pwd/output.csv')

    df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.loc[:, ~df.columns.str.contains('airport_fee')]

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            df = df.loc[:, ~df.columns.str.contains('airport_fee')]

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)
