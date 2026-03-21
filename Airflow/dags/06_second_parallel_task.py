from airflow.sdk import dag, task
from airflow.decorators import dag, task


@dag(dag_id="PARALLEL_TASKS_DAG"
)
def PARALLEL_TASKS_DAG():
    
    @task
    def extract_task(**kwargs):
        print("Extracting data")
        ti = kwargs['ti']
        extracted_data_dict = {"Framework" : ["Spark", 'Flink', 'Ray'],
                         "Database" : ['Snowflake', 'BigQuery', 'Redshift'],
                         "Programming Language" : ['SQL', 'Python', 'Java']}
        ti.xcom_push(key='extracted_data', value=extracted_data_dict)


    @task
    def transform_framework(**kwargs):
        ti = kwargs['ti']
        framwork_extracted = ti.xcom_pull(task_ids ='extract_framework')['framework']
        print(f"Transforming {framwork_extracted} data")
        ti.xcom_push(key='return_value', value=extracted_data_dict)

    @task
    def transform_database(**kwargs):
        ti = kwargs['ti']
        database_extracted = ti.xcom_pull(task_ids ='extract_database')['database']
        print(f"Transforming {database_extracted} data")
        ti.xcom_push(key='return_value', value=extracted_data_dict)

    @task
    def transform_programming_language(**kwargs):
        ti = kwargs['ti']
        programming_language_extracted = ti.xcom_pull(task_ids ='extract_programming_language')['programming_language']
        print(f"Transforming {programming_language_extracted} data")
        ti.xcom_push(key='return_value', value=extracted_data_dict)  


    extract_task() >> [transform_framework(), transform_database(), transform_programming_language()]

# INITIALIZE DAG
PARALLEL_TASKS_DAG()