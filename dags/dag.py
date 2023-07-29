from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG('data_extraction', description='Data extraction using python',
            schedule_interval='0 0 * * *',  # Execute the task daily at midnight
            start_date=datetime(2023, 7, 20), catchup=False) as dag:

    # Define the BashOperator to run the Bash script
    run_bash_script_task = BashOperator(
        task_id='run_bash_script_task',
        bash_command='cd /home/borto/Projects/pipeline-data-with-python && source venv/bin/activate && python main.py',
        dag=dag
    )