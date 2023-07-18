import requests
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine

# local imports
from credentials import db_user, db_pass, local_db_user, local_db_pass

def fetch_data( dbname, host, port, user, password):

    print("Connecting with postgres server")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

    print("Querying data")
    query = "SELECT * FROM public.venda;"
    df = pd.read_sql_query(query, engine)
    print(f'{df.shape[0]} rows fetched from postgres database')

    return df

def fetch_names(ids):
    df = pd.DataFrame(columns=['id_funcionario', 'nome_funcionario'])

    for value in ids:

        # if the name was not fetched yet, fetch it
        if not df['id_funcionario'].isin([value]).any():
            print(f'Fetching name for id {value} from API')
            url = f"https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={value}"
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

def fetch_categories():
    url = "https://storage.googleapis.com/challenge_junior/categoria.parquet"

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

# fetching requested data
print('Fetching data')
df = fetch_data(
    dbname = "postgres",
    host = "34.173.103.16",
    port = "5432",
    user = db_user,
    password = db_pass,
)
names_df = fetch_names(list(df['id_funcionario'].unique()))
categories_df = fetch_categories()

print('Merging dataframes')
df = pd.merge(df, names_df, on='id_funcionario', how='left')
df = pd.merge(df, categories_df, on='id_categoria', how='left')

# connect to output database
print('Connecting to local postgres server')
host = 'localhost'
port = '5432'
dbname = 'bix_challenge'
engine = create_engine(f'postgresql://{local_db_user}:{local_db_pass}@{host}:{port}/{dbname}')

# flush data into local table
print('Flush data into local table')
df.to_sql('venda', engine, index=False, if_exists='replace')
