# Pipeline data with Pandas

## Challenge description

The technical challenge will consist of solving the following problem. We have three data sources with the following information:

- PostgreSQL database with sales data.
- API with employee data.
- Parquet file with category data.

The objective of this challenge is to create a pipeline to move this data to a database in your environment, in a structure that you find most suitable. The end goal is to have sales, employees, and categories all in one place. We recommend using Python to ingest and process the data, and PostgreSQL as the database.

A requirement of this challenge is that the data movement should be done daily, as all sources receive new data periodically. This ensures that the data in your environment stays updated. For this purpose, it's important to have an orchestrator to automatically trigger your pipeline. We suggest using Apache Airflow as the orchestrator.

To make it clearer, here is an image of the desired architecture:

![challenge](./readme_images/challenge.png/)

## Solution

To solve this data pipeline, we can use simple Python data handling techniques to transform the given data into pandas dataframes and then handle the dataframes the way we want.

Querying data from a SQL database is straightforward using pandas. We just need to create the connection with the provided database and then query the data using an SQL query. Pandas already has a function that receives the connection and the query and then returns the dataframe with the database reply:

```python
df = pd.read_sql_query(query, connection)
```

Querying data from an API is more complicated. We need to make one web request for each employee listed in the query above. We can do it asynchronously to be faster, this way, all the requests are triggered together instead of triggering the next one only when the previous one is completed:

```python

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

```

This way, at the end of the execution, all the responses are going to be inside the variable results, and then we can easily convert this to a dataframe:

```python
pd.DataFrame({'id_funcionario': ids, 'nome_funcionario': results})

```

The parquet file needs a low-level approach. Once we have the link to the file on the web, we can request the bytes of the file and then convert them into a pyarrow table. This kind of table has a function that easily converts itself into a pandas dataframe:

```python
import pyarrow as pa
import pyarrow.parquet as pq

# get the file bytes from the given URL
response = requests.get(url)

# load parquet bytes data
parquet_data = pa.BufferReader(response.content)

# read the Parquet data from the bytes and create a PyArrow Table
table = pq.read_table(parquet_data)

# convert it to a pandas dataframe
df = table.to_pandas()
```

After fetching all data, we are going to have 3 dataframes: the main df, queried from the database, the names_df queried from the API, and the categories_df queried from the parquet web file. Now we can use pandas to easily merge them:

```python
# merge fetched data into a definitive dataframe
df = pd.merge(df, names_df, on='id_funcionario', how='left')
df = pd.merge(df, categories_df, on='id_categoria', how='left')
```

Done, we have a final dataframe, ready to be exported whatever we want.

## DATA ANALYTICS

With the resulting dataframe, it is possible to do some data analysis. Like:

Cumulative sales over time:

![cumulative](./graphs/1-cumulative_sales.png)

Categories comparative by value and by percentage:

![categories bar](./graphs/2-bar_chart.png)
![categories pie](./graphs/3-pie_chart.png)

Employee comparative:

![categories pie](./graphs/4-bar_chart_employee.png)

## Orchestration

To create an orchestrator for this file is very simple using Apache Airflow. First install Apache Airflow in your machine and keep the scheduler running, you can run run the webserver to have a GUI to help you creating the schedules too. To trigger the script we only need to call the main script, like a bash command:

``` bash
python main.py
```

But we need to ensure that the virtual environment is activated, so we need a commnand like this:

```bash
source venv/bin/activate && python main.py
```

So we can create a Bash dag containing the necessary commands and then copy it into the dags standart directory, this way Airflow can read the dag and create the task for us.

```python
with DAG('data_extraction', description='Data extraction using pandas',
        schedule_interval='0 0 * * *',  # Execute the task daily at midnight
        start_date=datetime(2023, 7, 20), catchup=False) as dag:

# Define the BashOperator to run the Bash script
run_bash_script_task = BashOperator(
    task_id='run_bash_script_task',
    bash_command='cd /home/borto/Projects/pipeline-data-with-python && source venv/bin/activate && python main.py',
    dag=dag
    )
```

Open Apache Airflow GUI on the web server and make sure that the DAG is active, this is enough to trigger the script every day.
