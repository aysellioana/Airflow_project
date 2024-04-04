import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable, DagModel
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from airflow.utils.session import provide_session
from airflow.utils.task_group import TaskGroup
from slack import WebClient
from slack.errors import SlackApiError
from airflow.models import DagModel
from croniter import croniter
from datetime import datetime


def get_execution_date_of_dependent_dag(external_dag_id):
    @provide_session
    def execution_date_fn(exec_date, session=None, **kwargs):
        external_dag = session.query(DagModel).filter(DagModel.dag_id == external_dag_id).one()
        external_dag_schedule_interval = external_dag.schedule_interval

        current_dag = kwargs['dag']
        current_dag_schedule_interval = current_dag.schedule_interval

        current_run_time = croniter(current_dag_schedule_interval, exec_date).get_next(datetime)
        external_cron_iterator = croniter(external_dag_schedule_interval, current_run_time)

        def check_if_external_dag_has_schedule_at_current_time(cron_iterator, current_time):
            __previous_run_time = cron_iterator.get_prev(datetime)
            current_run_time = cron_iterator.get_next(datetime)
            return True if current_run_time == current_time else False

        if check_if_external_dag_has_schedule_at_current_time(external_cron_iterator, current_run_time):
            external_cron_iterator = croniter(external_dag_schedule_interval, current_run_time)
            return external_cron_iterator.get_prev(datetime)
        else:
            external_cron_iterator = croniter(external_dag_schedule_interval, current_run_time)
            __dag_last_run_time = external_cron_iterator.get_prev(datetime)
            return external_cron_iterator.get_prev(datetime)

    return execution_date_fn


def send_slack_notification(**kwargs):
    slack_conn_id = 'slack_conn'
    slack_token = BaseHook.get_connection(slack_conn_id)
    client = WebClient(token=slack_token)
    dag_id = kwargs['dag'].dag_id
    execution_date = kwargs['execution_date']
    message = f"DAG {dag_id} completed at {execution_date}"
    try:
        response = client.chat_postMessage(
            channel="project",  # channel name
            text=message
        )
    except SlackApiError as e:
        raise e


def send_xcom_message(**context):
    message = f"{context['dag_run'].run_id} ended"
    context['ti'].xcom_push(key='end_message', value=message)
    #ti -> task instance


# Define function to print result
def print_result(**context):
    # Get the XCom value
    run_id = context['ti'].xcom_pull(task_ids=None, key='end_message')
    # Print the read value to the log
    print("XCom Value:", run_id)
    # Print the task's context dictionary
    print("Task Context:", context)

def get_execution_date(**context):
    pass


file_path = Variable.get('trigger_file_path', default_var='/Users/abatcoveanu/airflow/dags/')
schedule_interval = timedelta(days=1)

dag_id = 'trigger_dag'
with DAG(dag_id=dag_id, start_date=datetime(2018, 11, 11), schedule_interval=None) as dag:
    # Wait for the 'run' file to appear
    wait_run_file = FileSensor(
        task_id='wait_run_file',
        filepath=os.path.join(file_path, 'asd'),
        dag=dag
    )

    # Trigger the DAG when the file appears
    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dag_id_3',
        # wait_for_completion=True,
        dag=dag
    )


    with TaskGroup(group_id='process_result') as process_result:

        sensor_trigger_dag = ExternalTaskSensor(
            task_id='sensor_triggered_dag',
            poke_interval=60,
            timeout=180,
            external_dag_id='dag_id_3',
            external_task_id=None,
            execution_date_fn=get_execution_date_of_dependent_dag('dag_id_3'),
            dag=dag
        )



        # # Task to print the result
        # Define the task to print the result
        print_result_task = PythonOperator(
            task_id='print_result_task',
            python_callable=print_result,
            provide_context=True,
        )
        # send_xcom_message_task = PythonOperator(
        #     task_id='send_xcom_message',
        #     python_callable=send_xcom_message,
        #      dag=dag
        # )

        # # Create ‘finished_#timestamp’ file (BashOperator)
        create_finished_file_task = BashOperator(
            task_id='create_finished_file',
            bash_command='touch /Users/abatcoveanu/airflow/dags/finished_{{ ts_nodash }}.txt',
            dag=dag
        )
        # Remove ‘run’ file (BashOperator)
        remove_run_file = BashOperator(
            task_id='remove_run_file2',
            bash_command='rm /Users/abatcoveanu/airflow/dags/asd',
            dag=dag
        )

        sensor_trigger_dag>> print_result_task >> remove_run_file >> create_finished_file_task

    slack_notification_task = PythonOperator(
        task_id='slack_notification_task',
        python_callable=send_slack_notification,
        provide_context=True,
        dag=dag,
    )

    wait_run_file >> trigger_dag_task >> process_result >> slack_notification_task
