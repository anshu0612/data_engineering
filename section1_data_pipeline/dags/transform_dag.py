import os
import sys
from datetime import timedelta, datetime

from airflow import DAG
# Operators
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from pandas.api.types import is_string_dtype

import pandas as pd


def read_file(**context):
    path = os.path.join(os.getcwd(), "dags/common/netflix_titles.csv")
    df = pd.read_csv(path)
    context['ti'].xcom_push(key='df', value=df)


def drop_nan_names(**context):
    '''
        Delete any rows which do not have a name 
    '''
    dataframe = context.get("ti").xcom_pull(key="df")
    dataframe = dataframe[dataframe['name'].notna()]
    context['ti'].xcom_push(key='df', value=dataframe)


def split_names(**context):
    '''
        Split the name field into first_name, and last_name
    '''
    dataframe = context.get("ti").xcom_pull(key="df")
    dataframe[['first_name', 'last_name']] = dataframe['name'].str.split(
        ' ', 1, expand=True)
    dataframe.drop('name', axis=1, inplace=True)
    context['ti'].xcom_push(key='df', value=dataframe)


def remove_prepended_zeroes(**context):
    '''
        Remove any zeros prepended to the price field
    '''
    dataframe = context.get("ti").xcom_pull(key="df")
    # Convert price column to string if not already
    if not is_string_dtype(dataframe['price']):
        dataframe['price'].map(str)
    dataframe['price'] = [str(p).lstrip("0") for p in dataframe['price']]
    context['ti'].xcom_push(key='df', value=dataframe)


def add_above_100(**context):
    '''
        Create a new field named above_100, which is true if the price is
        strictly greater than 100

    '''
    dataframe = context.get("ti").xcom_pull(key="df")
    dataframe['above_100'] = [True if float(
        p) > 100 else False for p in dataframe['price']]
    context['ti'].xcom_push(key='df', value=dataframe)


def save_csv(**context):
    '''
        Save dataframe as csv
    '''
    dataframe = context.get("ti").xcom_pull(key="df")
    dataframe = context.get("ti").xcom_pull(key="df")
    dataframe = context.get("ti").xcom_pull(key="df")

    dataframe.to_csv('processed_data/{}'.format(filename))


# def merge_columns(**context):
#     pass

def save_processed_file(**context):
    df = context.get("ti").xcom_pull(key="df")
    path = os.path.join(os.getcwd(), "dags/common/process.csv")
    df.to_csv(path)


default_args = {
    "owner": "airflow",
    "start_date": datetime(year=2017, month=3, day=28),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    'email': ['anshu.singh173@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(dag_id="project",
         # run this dag at 1 AM interval daily
         schedule_interval='0 1 * * *',
         default_args=default_args, catchup=False) as dag:

    read_file = PythonOperator(
        task_id="read_file", python_callable=read_file, provide_context=True,)

    drop_nan_names = PythonOperator(
        task_id="drop_nan_names", python_callable=drop_nan_names, provide_context=True,)

    split_names = PythonOperator(
        task_id="split_names", python_callable=split_names, provide_context=True,)
    remove_prepended_zeroes = PythonOperator(
        task_id="remove_prepended_zeroes", python_callable=remove_prepended_zeroes, provide_context=True,)

    add_above_100 = PythonOperator(
        task_id="add_above_100", python_callable=add_above_100, provide_context=True,)

    merge_columns = PythonOperator(
        task_id="merge_columns", python_callable=merge_columns, provide_context=True,)

    save_processed_file = PythonOperator(
        task_id="save_processed_file", python_callable=save_processed_file, provide_context=True,)


read_file >> drop_nan_names
drop_nan_names >> [split_names, remove_prepended_zeroes, add_above_100]
drop_nan_names >> merge_columns
merge_columns >> save_processed_file
