from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import requests
import json
from datetime import datetime

from scripts.user_properties import UserProperty

ENV_VAR_SLACK_WEBHOOK_CONNECTION_URL = "SLACK_WEBHOOK_CONNECTION_URL"


def get_name_match(name_match_value):
    if name_match_value < 0.7:
        return UserProperty.NAME.NO_MATCH
    elif 0.7 <= name_match_value < 0.85:
        return UserProperty.NAME.PARTIAL_MATCH
    else:
        return UserProperty.NAME.FULL_MATCH


def connect_to_rds(input_connection_id):
    hook = MySqlHook(mysql_conn_id=input_connection_id)
    conn = hook.get_conn()
    return conn


def send_slack_message(message):
    try:
        headers = {'Content-type': 'application/json'}
        data = {'text': message}
        print(message)
        response = requests.post(Variable.get(ENV_VAR_SLACK_WEBHOOK_CONNECTION_URL), headers=headers, data=json.dumps(data))
        response.raise_for_status()  # Raise an error for bad status codes (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Failed to send message: {e}")


def format_message(input_dict):
    message = ""
    for key, value in input_dict.items():
        message += f"{key} - {value}\n"
    return message


def get_last_10_chars(input_string):
    if len(input_string) <= 10:
        return input_string
    else:
        return input_string[-10:]


def convert_to_uppercase(input_string):
    if input_string is None:
        return ""
    else:
        return input_string.upper()


def get_dob_match_type(dob1, dob2):
    date1 = datetime.strptime(dob1, '%Y-%m-%d')
    date2 = datetime.strptime(dob2, '%Y-%m-%d')

    # Full match - all three components (year, month, day) are matching
    if date1 == date2:
        return UserProperty.DOB.FULL_MATCH

    # Partial match - at least 2 components (year, month, day) are matching
    match_count = sum(a == b for a, b in zip(dob1.split('-'), dob2.split('-')))
    if match_count == 2:
        return UserProperty.DOB.PARTIAL_MATCH

    return UserProperty.DOB.NO_MATCH


def execute_sql_query_fetch_one(cursor, query, args=None):
    if args:
        cursor.execute(query, args)
    else:
        cursor.execute(query)

    result = cursor.fetchone()
    cursor.close()
    return result if result else None


def execute_sql_query_fetch_all(cursor, query, args=None):
    if args:
        cursor.execute(query, args)
    else:
        cursor.execute(query)

    result = cursor.fetchall()
    cursor.close()
    return result if result else []


