from airflow import DAG
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
import calendar
from airflow.operators.python import get_current_context
# from AirflowLocal.script.utils import send_slack_message

default_args = {
    'owner': 'Platform Team',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}


dag = DAG(
    'otp_archival-temp3',
    tags=["otp_archival", "platform"],
    default_args=default_args,
    schedule_interval='30 21 1 * *',  # 3 AM Every 2nd of Month, Airflow uses UTC by default
    catchup=False,
    max_active_runs=1,
    params={
        "date": Param(
            f"{datetime.today().date()}",
            type="string",
            format="date",
            title="Custom Date",
            description="Please select a date, It will archive the data of previous of previous month of selected date.",
        ),
    },
)


# This function will push the required dates and sql strings into xcom to pull it in next task and use it.
def print_data(ti, **kwargs):
    context = get_current_context()
    date_string = datetime.strptime(context["params"]["date"], "%Y-%m-%d")
    print(date_string)
    print("Date_string " + str(date_string))
    execution_timestamp = kwargs['ts']
    print("Execution_timestamp " + str(execution_timestamp))
    start_of_previous_of_previous_month = (date_string - relativedelta(months=2)).replace(day=1)
    end_of_previous_of_previous_month = (start_of_previous_of_previous_month +
            timedelta(days=calendar.monthrange(start_of_previous_of_previous_month.year, start_of_previous_of_previous_month.month)[1] - 1))
    s3_file_path = f"{start_of_previous_of_previous_month.year}/{start_of_previous_of_previous_month.month}/archived-otp-{execution_timestamp}.csv"
    start_of_previous_of_previous_month = start_of_previous_of_previous_month.date()
    end_of_previous_of_previous_month = end_of_previous_of_previous_month.date()
    print(f"Start Date: {start_of_previous_of_previous_month}")
    print(f"End Date: {end_of_previous_of_previous_month}")
    select_sql = f"SELECT * FROM otp WHERE Date(created_at) >= '{start_of_previous_of_previous_month}' AND Date(created_at) <= '{end_of_previous_of_previous_month}'"
    delete_sql = f"DELETE FROM otp WHERE Date(created_at) >= '{start_of_previous_of_previous_month}' AND Date(created_at) <= '{end_of_previous_of_previous_month}'"

    ti.xcom_push('s3_file_path', s3_file_path)
    ti.xcom_push('select_sql', select_sql)
    ti.xcom_push('delete_sql', delete_sql)


print_data = PythonOperator(
    task_id='print_data',
    provide_context=True,
    python_callable=print_data,
    dag=dag
)

print_data
