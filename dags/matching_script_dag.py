from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.models import Variable
import requests
import json

SLACK_WEBHOOK_CONNECTION_URL = "SLACK_WEBHOOK_CONNECTION_URL"
AIRFLOW_HOST = "AIRFLOW_HOST"
POD_HOST = "http://localhost:8080"


def send_slack_message(message):
    try:
        headers = {'Content-type': 'application/json'}
        data = {'text': message}
        print(message)
        response = requests.post(Variable.get(SLACK_WEBHOOK_CONNECTION_URL), headers=headers, data=json.dumps(data))
        response.raise_for_status()  # Raise an error for bad status codes (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Failed to send message: {e}")


def send_message(context, msg):
    log_url = context.get("task_instance").log_url
    log_url = log_url.replace(
        POD_HOST, Variable.get(AIRFLOW_HOST)
    )

    slack_msg = f"""
    {msg} 
    *Dag*: {context.get('task_instance').dag_id}
    *Execution Time*: {context.get('execution_date')}
    *Log Url*: {log_url}
    """
    send_slack_message(slack_msg)


def task_failure_callback(context):
    send_message(context, ":red_circle: Airflow Task Failed.")


def task_success_callback(context):
    send_message(context, ":large_green_circle: Airflow Task Success.")


default_args = {
    'owner': 'Platform Team',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


dag = DAG(
    'ucin_matching',
    tags=["ucin", "platform"],
    default_args=default_args,
    schedule_interval='30 23 * * *',  # 5 AM IST Every Morning
    catchup=False,
    max_active_runs=1,
    on_failure_callback=task_failure_callback,
    on_success_callback=task_success_callback,
)

um_uuid_backfill = BashOperator(
    task_id='um_uuid_backfill',
    bash_command='python3 /opt/airflow/scripts/um_uuid_backfill.py',
    dag=dag,
)

kyc_expiry_backfill = BashOperator(
    task_id='kyc_expiry_backfill',
    bash_command='python3 /opt/airflow/scripts/kyc_expiry_backfill.py',
    dag=dag,
)

pan_backfill = BashOperator(
    task_id='pan_backfill',
    bash_command='python3 /opt/airflow/scripts/pan_backfill.py',
    dag=dag,
)

matching_logic = BashOperator(
    task_id='matching_logic',
    bash_command='python3 /opt/airflow/scripts/tenant_based_matching_logic.py',
    dag=dag,
)

um_uuid_backfill >> [kyc_expiry_backfill, pan_backfill] >> matching_logic
