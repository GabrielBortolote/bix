import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine

def fetch_data(connection, query):

    print("Querying data")
    df = pd.read_sql_query(query, connection)
    print(f'{df.shape[0]} rows fetched from postgres database')

    return df

def fetch_names(ids, base_url):
    # creating df structure
    df = pd.DataFrame(columns=['id_funcionario', 'nome_funcionario'])

    for value in ids:

        # if the name was not fetched yet, fetch it
        if not df['id_funcionario'].isin([value]).any():

            
            print(f'Fetching name for id {value} from API')
            url = f"{base_url}?id={value}"
            try:
                response = requests.get(url)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    df.loc[len(df)] = [value, response.text]
                    
                else:
                    print(f"Failed to fetch data. Status code: {response.status_code}")

            except requests.exceptions.RequestException as e:
                print("Error while making the request:", e)

    
    return df

def fetch_categories(url):

    try:
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            
            # Assuming 'parquet_data_bytes' contains the bytes of the Parquet data from the web request
            parquet_data = pa.BufferReader(response.content)

            # Read the Parquet data from the bytes and create a PyArrow Table
            table = pq.read_table(parquet_data)
            print(f'{table.num_rows} rows were fetched from the parquet file')

            # Convert it to pandas dataframe
            df = table.to_pandas()
            df.rename(columns={'id': 'id_categoria'}, inplace=True)
            return df

        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print("Error while making the request:", e)

