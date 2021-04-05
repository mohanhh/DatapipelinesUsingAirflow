from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
'''
Create Dimension tables. As dimension tables are small, tables are truncated when new data is being inserted
redshift_conn_id: Connection Id set for Redshift
table: Dimension table name
sql: SQL to load dimension table
'''
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
       
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        postgres_hook.run(f"Delete from {self.table};")
        postgres_hook.run(self.sql)
        
