from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from stage_redshift import StageToRedshiftOperator
from load_fact import LoadFactOperator
from load_dimension import LoadDimensionOperator
from data_quality import DataQualityOperator
from helpers import SqlQueries
from create_tables import create_tables_dag

'''
Main processing function. Setup DAG to retry 3 times on failure and not depend on past runs.
'''
default_args = {
    'owner': 'Mohan Hegde',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'email_on_retry': False
    
}

# Create DAG to load Sparkify data using Airflow. 

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
#          schedule_interval='0 * * * *'
          schedule_interval='@hourly',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create all tables required for execution. This task should run once.
create_all_tables_task_id = "create_all_tables"
create_all_tables_subdag_task = SubDagOperator(
    subdag=create_tables_dag(
        parent_dag_name = "udac_example_dag",
        task_id=create_all_tables_task_id,
        redshift_conn_id="redshift",
        create_sql_file="sql/create_tables.sql",
        start_date=datetime.utcnow(),
        schedule_interval="@once"
    ),
    task_id=create_all_tables_task_id,
    dag=dag,
)


# Once all tables are created copy data from Sparkify's S3 bucket to redshift's staging tables
# staging_events and staging_songs
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
   copy_options="json 's3://udacity-dend/log_json_path.json'"
)
#staging_songs
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    copy_options="FORMAT AS JSON 'auto'"
)

# Now load all songs in to songplays fact table from staging table

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert)

#Load user, song, artist and time dimension tables. 
# All these loaded from staging tables can run parallely.  

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.users",
    sql=SqlQueries.user_table_insert,
    truncate= True)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="public.songs",
    redshift_conn_id="redshift",
    sql=SqlQueries.song_table_insert,
    truncate = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="public.artists",
    redshift_conn_id="redshift",
    sql=SqlQueries.artist_table_insert,
    truncate = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="public.time",
    redshift_conn_id="redshift",
    sql = SqlQueries.time_table_insert,
    truncate = True
)

start_quality_check_operator = DummyOperator(task_id='Start_Quality_check',  dag=dag)

# Check whether there are any records in songplays table
run_quality_checks_1 = DataQualityOperator(
    task_id='Run_data_quality_checks_1',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.songplays"
)

# check for records in songs table
run_quality_checks_2 = DataQualityOperator(
    task_id='Run_data_quality_checks_2',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.songs"
)

# check for records in artists table
run_quality_checks_3 = DataQualityOperator(
    task_id='Run_data_quality_checks_3',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.artists"
)

# all done stop execution
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
# Setup task dependencies 
start_operator >> create_all_tables_subdag_task >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> start_quality_check_operator >> [run_quality_checks_1, run_quality_checks_2, run_quality_checks_3] >> end_operator