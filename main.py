# built-in imports
import os
from datetime import datetime

# third part import
import yaml
import pandas as pd

# local imports
from connect import postgres_connection
from fetchers import fetch_data, fetch_categories, fetch_names
import credentials
from create_logger import create_logger

# global variables
global logger

def main(
    postgres_server_connection,
    postgres_local_connection,
    postgres_query,
    api_url,
    parquet_url,
):

    # fetching requested data
    logger.info('Fetching data')
    df = fetch_data(postgres_server_connection, postgres_query)
    names_df = fetch_names(list(df['id_funcionario'].unique()), api_url)
    categories_df = fetch_categories(parquet_url)

    # merge fetched data into a definitive dataframe
    logger.info('Merging dataframes')
    df = pd.merge(df, names_df, on='id_funcionario', how='left')
    df = pd.merge(df, categories_df, on='id_categoria', how='left')

    # flush data into local table
    logger.info('Flush data into local table')
    df.to_sql('venda', postgres_local_connection, index=False, if_exists='replace')

if __name__ == '__main__':
    # load configurations
    with open('config.yaml') as config_file:
        config = yaml.safe_load(config_file)

    # initialize logger object
    file_name = f"{datetime.now().strftime('%d-%m-%Y-%H-%M-%S-%f')}.log"
    logger = create_logger(os.path.join(config['Paths']['log'], file_name))

    # calling main procedure    
    main(
        postgres_server_connection=postgres_connection(
            username=credentials.db_user,
            password=credentials.db_pass,
            database_name=config['Postgres']['database'],
            host=config['Postgres']['host'],
            port=config['Postgres']['port'],
        ),
        postgres_local_connection=postgres_connection(
            username=credentials.local_db_user,
            password=credentials.local_db_pass,
            database_name=config['Output']['database'],
            host=config['Output']['host'],
            port=config['Output']['port'],
        ),
        postgres_query=config['Postgres']['query'],
        api_url=config['Urls']['api'],
        parquet_url=config['Urls']['parquet']
    )