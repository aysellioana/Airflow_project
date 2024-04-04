from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

dagTaskGroup = DAG(
    'dagTaskGroup',
    description='A DAG with TaskGroup',
    schedule=None,
    start_date=datetime(2018, 3, 12),
)

def get_execution_date(**context):
    """
       Function to retrieve the execution date of the SubDAG.

       Args:
           **context: A dictionary containing context variables, including the execution date.

       Returns:
           The execution date of the SubDAG.
       """
    return context['execution_date']
def print_result(**context):
    #Get message from Xcom
    message = context['ti'].xcom_pull(task_ids='send_xcom_message', key='end_message')
    print(f"Message from Xcom: {message}")

    #Print the entire task context dictionary
    print("Task context dictionary")
    for key, value in context.items():
        print(f"{key}: {value}")

with dagTaskGroup:
    with TaskGroup('subdag') as process:
        # Define a task to get the execution date from the context
        get_execution_date_task = PythonOperator(
            task_id='get_execution_date',
            python_callable=get_execution_date,
            dag=dagTaskGroup
        )

        sensor_triggered_dag = ExternalTaskSensor(
            task_id='sensor_triggered_dag',
            external_dag_id=None,
            allowed_states=['success'],
            mode='poke',
            poke_interval=60,  # check every minute
            timeout=600,  # timeout after 10 minutes
            execution_date_fn=get_execution_date,
            dag=dagTaskGroup
        )

        # Task to print the result
        print_result_task = PythonOperator(
            task_id='print_result_task',
            python_callable=print_result,
            dag=dagTaskGroup
        )

        # Remove ‘run’ file (BashOperator)
        remove_run_file = BashOperator(
            task_id='remove_run_file2',
            bash_command='rm /jobs_dag.py/run',
            dag=dagTaskGroup
        )

        # Create ‘finished_#timestamp’ file (BashOperator)
        create_finished_file = BashOperator(
            task_id='create_finished_file',
            bash_command='touch path/to/finished_{{ ts_nodash }}',
            dag=dagTaskGroup
        )

        # Define the task dependencies within the TaskGroup
        sensor_triggered_dag >> print_result_task
        print_result_task >> remove_run_file
        print_result_task >> create_finished_file
