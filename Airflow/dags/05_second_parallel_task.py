from airflow.sdk import dag, task
from airflow.decorators import dag, task


@dag(dag_id="PARALLEL_TASKS_DAG"
)
def PARALLEL_TASKS_DAG():
    