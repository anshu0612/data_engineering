from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


pyspark_app_home = Variable.get("PYSPARK_APP_HOME")

# dag arguments
default_args = {
    'owner': 'anshu',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 27),
    'email': ['anshu.singh@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

with DAG(dag_id="project",
         # run this dag at 1 AM interval daily
         schedule_interval='0 1 * * *',
         default_args=default_args, catchup=False) as dag:

    # task - submitting spark job to read csv and remove nan from the names columns
    read_file_and_drop_nan_names = SparkSubmitOperator(task_id='read_file_and_drop_nan_names',
                                                       conn_id='spark_local',
                                                       application=f'{pyspark_app_home}/spark/read_file_and_drop_nan_names.py',
                                                       total_executor_cores=4,
                                                       executor_cores=2,
                                                       executor_memory='5g',
                                                       driver_memory='5g',
                                                       name='read_file_and_drop_nan_names',
                                                       dag=dag,
                                                       # location of the file
                                                       application_args=[
                                                           "/Users/anshu/Work/Code/Projects/data_engineering/section1_data_pipeline/data"])

    # task - submitting spark job to split name
    split_names = SparkSubmitOperator(task_id='split_names',
                                      conn_id='spark_local',
                                      application=f'{pyspark_app_home}/spark/split_names.py',
                                      total_executor_cores=4,
                                      executor_cores=2,
                                      executor_memory='5g',
                                      driver_memory='5g',
                                      name='split_names',
                                      dag=dag,
                                      application_args=[
                                          "/Users/anshu/Work/Code/Projects/data_engineering/section1_data_pipeline/data"]
                                      )

    # task - submitting spark job to remove prepended zeroes and and add above_100
    remove_prepended_zeroes_add_above_100 = SparkSubmitOperator(task_id='remove_prepended_zeroes_add_above_100',
                                                                conn_id='spark_local',
                                                                application=f'{pyspark_app_home}/spark/remove_prepended_zeroes_add_above_100.py',
                                                                total_executor_cores=4,
                                                                executor_cores=2,
                                                                executor_memory='5g',
                                                                driver_memory='5g',
                                                                name='remove_prepended_zeroes_add_above_100',
                                                                dag=dag,
                                                                application_args=[
                                                                    "/Users/anshu/Work/Code/Projects/data_engineering/section1_data_pipeline/data"]
                                                                )

    # task - submitting spark job to merge prepended zeroes and and add above_100
    merge_cols_and_save_file = SparkSubmitOperator(task_id='merge_cols_and_save_file',
                                                   conn_id='spark_local',
                                                   application=f'{pyspark_app_home}/spark/merge_cols_and_save_file.py',
                                                   total_executor_cores=4,
                                                   executor_cores=2,
                                                   executor_memory='5g',
                                                   driver_memory='5g',
                                                   name='merge_cols_and_save_file',
                                                   dag=dag,
                                                   application_args=[
                                                       "/Users/anshu/Work/Code/Projects/data_engineering/section1_data_pipeline/data/"]
                                                   )

# airflow pipeline
read_file_and_drop_nan_names >> [
    split_names, remove_prepended_zeroes_add_above_100] >> merge_cols_and_save_file
