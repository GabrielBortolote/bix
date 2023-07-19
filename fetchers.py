# built-in imports
import logging

# third-part imports
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import Engine
import asyncio
import aiohttp

# get pre-defined logger
logger = logging.getLogger('default_logger')

def fetch_data(connection:Engine, query:str) -> pd.DataFrame:
    """
    Fetch data from a PostgreSQL server using a provided SQL query.

    Parameters:
        - connection (sqlalchemy.Engine): The connection object to the PostgreSQL database.
        - query (str): The SQL query to retrieve the data.

    Returns:
        - pd.DataFrame: A pandas DataFrame containing the fetched data from the database.
    """

    logger.debug("querying data from postgres server")
    df = pd.read_sql_query(query, connection)
    logger.debug(f'{df.shape[0]} rows fetched from postgres database')

    return df

async def fetch_names(ids:list, base_url:str) -> pd.DataFrame:
    """
    Fetch names for a list of employee IDs from an API.

    Parameters:
        - ids (list): List of employee IDs.
        - base_url (str): The base URL of the API to fetch names from.

    Returns:
        - pd.DataFrame: A pandas DataFrame containing the fetched names for the provided IDs.
    """

    # creating df structure
    logger.debug('creating names dataframe')

    # build all urls to be accessed
    urls = [f"{base_url}?id={value}" for value in ids]
    
    # declare aysnc function to perform the requests
    async def get_url_data(url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.text()
            
    # create tasks for each request
    tasks = [get_url_data(url) for url in urls]

    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks)

    return pd.DataFrame({'id_funcionario': ids, 'nome_funcionario': results})

def fetch_categories(url:str) -> pd.DataFrame:
    """
    Fetch data from a Parquet file using the provided URL.

    Parameters:
        - url (str): The URL of the Parquet file to fetch the data from.

    Returns:
        - pd.DataFrame: A pandas DataFrame containing the fetched data from the Parquet file.
    """

    logger.debug('fetching data from parquet file')
    logger.debug(f'file address: {url}')
    try:
        response = requests.get(url)
        if response.status_code == 200:
            
            # load parquet bytes data
            logger.debug('loading parquet bytes data')
            parquet_data = pa.BufferReader(response.content)

            # read the Parquet data from the bytes and create a PyArrow Table
            logger.debug('building parquet table using pyarrow')
            table = pq.read_table(parquet_data)
            logger.debug(f'{table.num_rows} rows were fetched from the parquet file')

            # convert it to a pandas dataframe
            logger.debug('converting parquet table into a pandas dataframe')
            df = table.to_pandas()

            # rename id columns
            logger.debug('renaming id column')
            df.rename(columns={'id': 'id_categoria'}, inplace=True)
            return df

        else:
            logger.error(f"Failed to fetch data. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logger.error("Error while making the request:", e)

