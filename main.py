import pandas as pd

# local imports
from connect import postgres_connection
from config import config
from fetchers import fetch_data, fetch_categories, fetch_names
import credentials

def main(
    postgres_server_connection,
    postgres_local_connection,
    postgres_query,
    api_url,
    parquet_url,
):

    # fetching requested data
    print('Fetching data')
    df = fetch_data(postgres_server_connection, postgres_query)
    names_df = fetch_names(list(df['id_funcionario'].unique()), api_url)
    categories_df = fetch_categories(parquet_url)

    # merge fetched data into a definitive dataframe
    print('Merging dataframes')
    df = pd.merge(df, names_df, on='id_funcionario', how='left')
    df = pd.merge(df, categories_df, on='id_categoria', how='left')

    # flush data into local table
    print('Flush data into local table')
    df.to_sql('venda', postgres_local_connection, index=False, if_exists='replace')

if __name__ == '__main__':
    # calling main procedure
    main(
        postgres_server_connection=postgres_connection(
            username=credentials.db_user,
            password=credentials.db_pass,
            database_name=config['server']['dbname'],
            host=config['server']['host']
        ),
        postgres_local_connection=postgres_connection(
            username=credentials.local_db_user,
            password=credentials.local_db_pass,
            database_name=config['local']['dbname'],
        ),
        postgres_query=config['postgres_query'],
        api_url=config['api_url'],
        parquet_url=config['parquet_url']
    )