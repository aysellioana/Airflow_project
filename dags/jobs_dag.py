import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
from custom_operators import PostgreSQLCountRows

schedule_interval = timedelta(days=1)
# Define the configuration dictionary with parameters for each DAG
config = {
    'dag_id_1': {'schedule': None, "start_date": datetime(2018, 11, 11), "table_name": "table_name_1"},
    'dag_id_2': {'schedule': None, "start_date": datetime(2018, 11, 11), "table_name": "table_name_2"},
    'dag_id_3': {'schedule': None, "start_date": datetime(2018, 11, 11), "table_name": "table_name_3"}
}
# None is used when you only want to trigger dags manually


#Print the following information information into log:
# "{dag_id} start processing tables in database: {database}" (use PythonOperator for that)
def log_info(dag_id, database):
    print(f"{dag_id} start processing tables in database {database}")

# Function to check if the table exists
def check_table_existence(table_name):
    """Check if the given table exists in the database."""
    hook = PostgresHook(postgres_conn_id="Postgres_db")
    print("asdddd")
    print(hook.get_uri())
    # Construct the SQL query to check table existence
    sql_query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')"
    # Execute the SQL query
    table_exists = hook.get_first(sql_query)
    if table_exists[0]:
        return 'create_table_task'
    else:
        return 'dummy_task'



#add xcom_push() to send message to XCom when DAG completes
def send_xcom_message(**context):
    message = f"{context['dag_run'].run_id} ended"
    context['ti'].xcom_push(key='end_message', value=message)
    #ti -> task instance

def get_current_user():
    import getpass
    curent_user = getpass.getuser()
    return curent_user


# Define Python function to generate custom_id_value
def generate_custom_id():
    return uuid.uuid4().int % 123456789

# Define Python function to get timestamp_value
def get_timestamp_value():
    return datetime.now()

# Define Python function to query the table and get row count
def query_table_and_get_row_count():
    postgres_hook = PostgresHook(postgres_conn_id='Postgres_db')
    row_count = postgres_hook.get_first("SELECT COUNT(*) FROM {}".format(dag_config['table_name']))[0]
    return row_count

#Iterate through the config dictionary and generate DAGs in the loop
#Using context manager

for dag_id, dag_config in config.items():
    with DAG(dag_id = dag_id, schedule= dag_config['schedule'], start_date=dag_config['start_date']) as dag:
        # Print processing info
        print_info_task = PythonOperator(
            task_id= 'print_info_task',
            python_callable=log_info,
            op_kwargs={'dag_id': dag_id, 'database':dag_config['table_name']},
            dag = dag
        )


        #check if the table exists or not
        check_table_task = BranchPythonOperator(
            task_id='check_table_existence',
            python_callable=check_table_existence,
            op_kwargs={'table_name': dag_config['table_name']},
            dag=dag
        )
        # Task to execute "whoami" command
        # whoami_task = BashOperator(
        #     task_id='whoami_task',
        #     bash_command='whoami',
        #     dag=dag
        # )
        #task to mock insertion of new row into DB
        # show_tables = PostgresOperator(
        #     task_id='show_tables',
        #     postgres_conn_id='Postgres_db',
        #     sql="""
        #    select table_schema, table_name FROM information_schema.tables
        #    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        #    ORDER BY table_schema, table_name
        #     """,
        #     parameters=[generate_custom_id(), '{{ task_instance.xcom_pull(task_ids="getting_current_user") }}',
        #                 get_timestamp_value()],
        #     dag=dag
        # )

        # show_databases = PostgresOperator(
        #     task_id='show_databases',
        #     postgres_conn_id='Postgres_db',
        #     sql="""
        #         SELECT datname
        #         FROM pg_database
        #         WHERE datistemplate = false
        #         ORDER BY datname;
        #     """,
        #     parameters=[generate_custom_id(), '{{ task_instance.xcom_pull(task_ids="getting_current_user") }}',
        #                 get_timestamp_value()],
        #     dag=dag
        # )

        #task to mock insertion of new row into DB
        insert_new_row = PostgresOperator(
            task_id='insert_new_row',
            postgres_conn_id='Postgres_db',
            sql='INSERT INTO {} VALUES (%s, %s, %s)'.format(dag_config['table_name']),
            parameters=[generate_custom_id(), '{{ task_instance.xcom_pull(task_ids="getting_current_user") }}',
                        get_timestamp_value()],
            dag=dag
        )

        #task to mock table query
        # query_table_task = PythonOperator(
        #     task_id='query_table_task',
        #     python_callable=query_table_and_get_row_count,
        #     dag=dag
        # )


        # Task to retrieve the current user
        getting_current_user = PythonOperator(
            task_id='getting_current_user',
            python_callable=get_current_user,
            provide_context=True,
            dag=dag
        )

        # Add the new task to send message to XCom
        # send_xcom_message_task =PythonOperator(
        #     task_id='send_xcom_message',
        #     python_callable=send_xcom_message,
        #     dag=dag
        # )

        #Task to create table
        create_table_task = PostgresOperator(
            task_id = 'create_table_task',
            postgres_conn_id = 'Postgres_db',
            sql='''CREATE TABLE IF NOT EXISTS {}(
                        custom_id INTEGER NOT NULL,
                        user_name VARCHAR(50) NOT NULL,
                        timestamp TIMESTAMP NOT NULL
                    );'''.format(dag_config['table_name']),
            dag=dag
        )
        # Define dummy and create table tasks
        dummy_task = EmptyOperator(
            task_id='dummy_task',
            trigger_rule=TriggerRule.NONE_FAILED,
            dag=dag
        )
        query_table_task_co = PostgreSQLCountRows(
            task_id='query_table_task_co',
            table_name=dag_config['table_name'],
            dag=dag
        )


        # # Function to print SQL output
        # def print_sql_output(**kwargs):
        #     ti = kwargs['ti']
        #     sql_output = ti.xcom_pull(task_ids='list_all_databases')  # Fetch SQL output from XCom
        #     print("SQL Output:")
        #     for row in sql_output:
        #         print(row[0])
        #
        #
        # # Create a PythonOperator to print SQL output
        # print_sql_output_task = PythonOperator(
        #     task_id='print_sql_output',
        #     python_callable=print_sql_output,
        #     provide_context=True,
        #     dag=dag
        # )



        # Define task dependencies
        print_info_task >>getting_current_user >> check_table_task
        check_table_task>> [create_table_task, dummy_task]
        create_table_task >> insert_new_row>> query_table_task_co
        dummy_task >> insert_new_row >> query_table_task_co


