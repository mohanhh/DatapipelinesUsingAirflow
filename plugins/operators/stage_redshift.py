from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

'''
Copy from S3 to Redshift using AwsHook and PostgresHook
redshift_conn_id: Redshift connection id set under Airflow Connections
aws_connections_id: AWS Connection id set under Airflow connections
table: Redshift table to copy data to
s3_bucket: Source S3 bucket for data
s3_key: s3 key inside s3_bucket where data is located
copy_options: Copy options to apply. For eg: 'json 's3://udacity-dend/log_json_path.json'
or format as json 'auto'
'''
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {};
    """
    @apply_defaults
    def __init__(self,
               
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_options="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        
       
        self.aws_credentials_id = aws_credentials_id
        self.copy_options = copy_options

    def execute(self, context):
        self.log.info('StageToRedshiftOperator implemented by Mohan Hegde')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
       
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(self.table, 
                                                                  s3_path, 
                                                                  credentials.access_key, 
                                                                  credentials.secret_key, 
                                                                  self.copy_options)
        redshift.run(formatted_sql)
                                         
                                         
                                
        





