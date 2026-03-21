from airflow.sdk import dag, task
from airflow.decorators import dag, task


@dag(dag_id="PARALLEL_TASKS_DAG"
)
def PARALLEL_TASKS_DAG():
    
    @task
    def extract_task(**kwargs):
        print("Extracting data")
        ti = kwargs['ti']
        extracted_data_dict = {"framework" : ["SPARK", 'FLINK', 'RAY'],
                         "database" : ['SNOWFLAKE', 'BIGQUERY', 'REDSHIFT'],
                         "programming_language" : ['SQL', 'PYTHON', 'JAVA', 'SCALA']}
        ti.xcom_push(key='return_value', value=extracted_data_dict)


    @task
    def transform_framework(**kwargs):
        ti = kwargs['ti']
        framwork_extracted = ti.xcom_pull(task_ids ='extract_task')['framework']
        print(f"Transforming {framwork_extracted} data")
        transform_framework = [framework.capitalize() for framework in framwork_extracted]
        print(f"Transformed Framework: {transform_framework}")
        ti.xcom_push(key='return_value', value=transform_framework)

    @task
    def transform_database(**kwargs):
        ti = kwargs['ti']
        database_extracted = ti.xcom_pull(task_ids ='extract_task')['database']
        print(f"Transforming {database_extracted} data")
        transform_database = [database.capitalize() for database in database_extracted]
        print(f"Transformed Database: {transform_database}")
        ti.xcom_push(key='return_value', value=transform_database)

    @task
    def transform_programming_language(**kwargs):
        ti = kwargs['ti']
        programming_language_extracted = ti.xcom_pull(task_ids ='extract_task')['programming_language']
        print(f"Transforming {programming_language_extracted} data")
        transform_programming_language = [language.capitalize() for language in programming_language_extracted]
        print(f"Transformed Programming Language: {transform_programming_language}")
        ti.xcom_push(key='return_value', value=transform_programming_language)


    # extract_task() >> [transform_framework(), transform_database(), transform_programming_language()]
    extract_task() >> transform_framework() >> transform_database() >> transform_programming_language()

# INITIALIZE DAG
PARALLEL_TASKS_DAG()