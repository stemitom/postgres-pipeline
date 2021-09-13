from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadCsvtoPostgresOperator(BaseOperator):
    """
    Moves data from a delimiter(tab) seperated file to Postgres
    """
    template_fields = ("file_path",)

    @apply_defaults
    def __init__(self, postgres_conn_id, table, file_path, *args, **kwargs):
        super(LoadCsvtoPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.file_path = file_path
    
    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info(f"Loading file {self.file_path} into table {self.table}")
        try:
            postgres.bulk_load(self.table, self.file_path)
        except Exception as err:
            self.log.error(err)
            raise ValueError(err)
        else:
            self.log.info(f"Loaded file {self.file_path} into table {self.table}")