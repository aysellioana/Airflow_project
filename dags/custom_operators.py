from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgreSQLCountRows(BaseOperator):
    def __init__(self, table_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = table_name

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id='Postgres_db')
        row_count = postgres_hook.get_first(f"SELECT COUNT(*) FROM {self.table_name}")[0]
        return row_count
