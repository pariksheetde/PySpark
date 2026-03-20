from airflow.sdk import dag, task
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

@dag(dag_id="XCOM_DAG_KWARGS",
        schedule=None,
)
def XCOM_DAG_KWARGS():
    
    @task
    def extract_data(**kwargs):
        print("Extracting data from API")
        # Extracting `ti` from **kwargs to push data to XCOM
        ti = kwargs['ti']
        fetched_data = {"orchestrator": ["Airflow", "Prefect", "Luigi"]}
        ti.xcom_push(key="ret_fetched_data", value=fetched_data)

    @task
    def transform_data(**kwargs):
        print("Transforming data")
        # Extracting `ti` from **kwargs to pull data from XCOM
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids="extract_data", key="ret_fetched_data")
        print(f"Processing data: {fetched_data}")
        transformed_data = f"{len(fetched_data['orchestrator'])} big orchestrator tools {', '.join(fetched_data['orchestrator'][:-1])} & {fetched_data['orchestrator'][-1]}"
        print(f"I have worked with: {transformed_data}")

    @task
    def load_data(**kwargs):
        print("Loading data")
        # Extracting `ti` from **kwargs to pull data from XCOM
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids="transform_data", key="ret_transformed_data")
        print(f"Loading data: {transformed_data}")
        print("Data loaded successfully")

    extract_data = extract_data()
    transform_date = transform_data()
    load_data = load_data()

    extract_data >> transform_date >> load_data

# INITIALIZE DAG
XCOM_DAG_KWARGS()