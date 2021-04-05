from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

'''
Read data from the table and verify there is at least one record
redshift_conn_id: Redshift connection setup under configuration
table_name: Table name to check number of records
'''
class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        

    def execute(self, context):
        self.log.info('DataQualityOperator implemented by Mohan Hegde')
        postgres_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        records = postgres_hook.get_records(f"Select count (*) from  {self.table_name}") 
        print(records)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table_name} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table_name} contained 0 rows")
        logging.info(f"Data quality on table {self.table_name} check passed with {records[0][0]} records")
        