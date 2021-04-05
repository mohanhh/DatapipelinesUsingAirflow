import datetime
import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

import sql


'''
This subdag creates all the tables required for the ETL. The intent is to run this first.
parent_dag_name: Parent dag name
task_id: Parent Task id 
redshift_conn_id: Redshift connection id
create_sql: SQL File name to use while creating all the tables
'''
def create_tables_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        create_sql_file,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    create_table_task = PostgresOperator(
        task_id=f"create_tables",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_sql_file
    )
    return dag
    
