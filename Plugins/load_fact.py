from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common import final_project_sql_statements


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 action="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.action = action
        self.query = query

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.action == "reload":
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Inserting data from staging table into fact table")
        custom_sql = self.query
        redshift.run(custom_sql)
