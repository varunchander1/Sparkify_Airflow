import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for key, val in self.tables.items():
            table = self.tables[key][0]
            field = self.tables[key][1]

            check1_sql = f"SELECT Count(*) FROM {table}"
            result = redshift.get_first(check1_sql) 
            logging.info(f"Table: {table} has {result} rows")
            
            check2_sql = f"SELECT Count(*) FROM {table} where {field} IS NULL"
            recs = redshift.get_first(check2_sql) 
            logging.info(f"Field: {field} in table: {table} has {recs} NULL rows")

