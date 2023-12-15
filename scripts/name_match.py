import requests
import json
from airflow.models import Variable

ENV_VAR_NAME_MATCH_API = "NAME_MATCH_API"
ENV_VAR_NAME_MATCH_API_TOKEN = "NAME_MATCH_API_TOKEN"


def perform_name_match(source_name, target_name):
    # url = 'http://darwin-maas-orchestrator.internal.payufin.io/api/v1/get_multiple_model_version_scores'
    url = Variable.get(ENV_VAR_NAME_MATCH_API)
    headers = {
        'Content-Type': 'application/json',
        'Token': Variable.get(ENV_VAR_NAME_MATCH_API_TOKEN)
    }

    payload = {
        "models": [
            {"model_name": "name_match", "model_version": 2}
        ],
        "features": {
            "source": source_name,
            "target": target_name
        }
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            data = json.loads(response.json())
            return data["result"]["name_match"]["data"]["score"]
        else:
            raise requests.HttpError
    except requests.RequestException as e:
        raise e
