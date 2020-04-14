from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 append_only=True,
                 table="",
                 redshift_conn_id="",
                 sql="",
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.append_only = append_only
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            self.log.info(f"Truncating table: {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table};")

        self.log.info("Loading data into dimension table")
        redshift.run(self.sql)