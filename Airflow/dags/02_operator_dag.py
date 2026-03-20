from airflow.sdk import dag, task
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

@dag(dag_id="OPERATOR_DAG",
        schedule=None,
)
def OPERATOR_DAG():
    
    @task.python
    def first_python_task():
        print("First Function")

    @task.python
    def second_python_task():
        print("Second Function")
    
    @task.bash
    def bash_task():
        return "echo 'www.airflow.com'"

    first_python_task() >> second_python_task() >> bash_task()

# INITIALIZE DAG
OPERATOR_DAG()