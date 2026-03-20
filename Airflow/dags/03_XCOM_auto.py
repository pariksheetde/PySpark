from airflow.sdk import dag, task
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

@dag(dag_id="XCOM_DAG_AUTO",
        schedule=None,
)
def XCOM_DAG_AUTO():
    
    @task
    def first_python_task():
        print("Extracting data from API")
        fetched_data = {"name": "Airflow", "version": "2.0"}
        return fetched_data

    @task
    def second_python_task(data):
        print(f"Received data: {data}")
        return f"Processed data: {data['name']} version is {data['version']}"
    
    @task.bash
    def bash_task(data):
        load_data = data
        return f"echo 'Loading data: {load_data}'"


    first_python_task() >> second_python_task(first_python_task()) >> bash_task(second_python_task)

# INITIALIZE DAG
XCOM_DAG_AUTO()