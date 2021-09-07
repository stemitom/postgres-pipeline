from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from tempfile import NamedTemporaryFile
import pandas

class LoadCsvtoPostgresOperator(BaseOperator):
    """
    Moves data from a comma seperated file to Postgres
    """
    # template_fields = ('sql',)
    # template_ext = ('.sql',)
    ui_color = '#D2B4DE'

    @apply_defaults
    def __init__(self, postgres_conn_id, table, file_path, *args, **kwargs):
        super(LoadCsvtoPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.file_path = file_path
    
    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info(f"Loading file {self.file_path} into table {self.table}")
        csv_data = pandas.read_csv(self.file_path)
        postgres.bulk_load(self.table, csv_data)
        self.log.info(f"Loaded file {self.file_path} into table {self.table}")