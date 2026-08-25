from airflow.sdk import dag, task
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

@dag(dag_id="XCOM_DAG_AUTO",
        schedule=None,
)
def XCOM_DAG_AUTO():
    
    @task
    def extract_data():
        print("Extracting data from API")
        fetched_data = {"orchestrator": ["Airflow", "Prefect", "Luigi"]}
        return fetched_data

    @task
    def transform_data(data):
        print(f"Processing data: {data}")
        transformed_data = f"{len(data['orchestrator'])} big orchestrator tools {', '.join(data['orchestrator'][:-1])} & {data['orchestrator'][-1]}"
        print(f"I have worked with: {transformed_data}")

    @task
    def load_data(data):
        print(f"Loading data: {data}")
        print("Data loaded successfully")

    first_task = extract_data()
    second_task = transform_data(first_task)
    third_task = load_data(second_task)



# INITIALIZE DAG
XCOM_DAG_AUTO()