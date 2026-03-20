from airflow.sdk import dag, task
from airflow.decorators import dag, task

@dag(dag_id="FIRST_DAG"
)
def FIRST_DAG():
    
    @task
    def first_task():
        print("First Function")

    @task
    def second_task():
        print("Second Function")

    @task
    def third_task():
        print("Third Function")
    
    @task
    def final_task():
        print("DAG executed successfully ")

    first_task() >> second_task() >> third_task() >> final_task()

# INITIALIZE DAG
FIRST_DAG()