from airflow.models import Variable
import requests
import json

SLACK_WEBHOOK_CONNECTION_URL = "SLACK_WEBHOOK_CONNECTION_URL"


def send_slack_message(message):
    try:
        headers = {'Content-type': 'application/json'}
        data = {'text': message}
        print(message)
        response = requests.post(Variable.get(SLACK_WEBHOOK_CONNECTION_URL), headers=headers, data=json.dumps(data))
        response.raise_for_status()  # Raise an error for bad status codes (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Failed to send message: {e}")