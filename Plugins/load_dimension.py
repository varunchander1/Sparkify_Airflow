from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common import final_project_sql_statements

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 action="",
                 query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.action = action
        self.query = query

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.action == "reload":
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Inserting data from staging table into Dim table")
        custom_sql = self.query
        redshift.run(custom_sql)
