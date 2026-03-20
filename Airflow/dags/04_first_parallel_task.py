from airflow.sdk import dag, task
from airflow.decorators import dag, task

@dag(dag_id="FIRST_PARALLEL_DAG"
)
def FIRST_PARALLEL_DAG():
    
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
        print("DAG executed successfully")

    task1 = first_task()
    task2 = second_task()
    task3 = third_task()
    task4 = final_task()

    task1 >> [task2, task3] 
    [task2, task3] >> task4

# INITIALIZE DAG
FIRST_PARALLEL_DAG()