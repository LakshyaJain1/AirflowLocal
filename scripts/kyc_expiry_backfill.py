import json, requests
import datetime
import time
from mysql.connector import Error

from scripts.constants import UNIVERSAL_ID_DB_CONNECTION, BATCH_SIZE
from utils import connect_to_rds
from airflow.models import Variable

KYC_ENDPOINT = "KYC_ENDPOINT"


class User:
    def __init__(self, um_uuid, kyc_expiry):
        self.um_uuid = um_uuid
        self.kyc_expiry = kyc_expiry


def get_user_list(de_cursor):
    try:
        sql = '''SELECT um_uuid FROM tc_pan_kyc_data where kyc_expiry_date IS NULL'''
        de_cursor.execute(sql)
        records = de_cursor.fetchall()
        return records
    except Error as e:
        print("Error reading data from MySQL table", e)


def insert_kyc_expiry(de_cursor, list_of_users):
    try:
        sql = '''UPDATE tc_pan_kyc_data SET kyc_expiry_date = %s WHERE um_uuid = %s'''
        list_of_tuples = []
        for obj in list_of_users:
            tuple1 = (obj.kyc_expiry, obj.um_uuid)
            list_of_tuples.append(tuple1)
        de_cursor.executemany(sql, list_of_tuples)
        de_cursor.connection.commit()
    except Error as e:
        print("Error reading data from MySQL table", e)


def get_user_kyc_expiry(um_uuid):
    url = Variable.get(KYC_ENDPOINT) + "/api/kycEngine/rekyc/kyc-expiry-status/" + um_uuid
    payload = {}
    headers = {
        'Accept': 'application/json'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code == 200:
        d = json.loads(response.content)
        return d['dateOfExpiry']
    else:
        return None


def get_user_data():
    de_connection = connect_to_rds(UNIVERSAL_ID_DB_CONNECTION)
    de_cursor = de_connection.cursor()
    try:
        data = get_user_list(de_cursor)
        print("Kyc expiry is null for count: {}".format(len(data)))
        list1 = []
        batch_count = 0
        batches = 1
        for row in data:
            if batch_count == BATCH_SIZE:
                insert_kyc_expiry(de_cursor, list1)
                print("Inserted batch: {}".format(batches))
                batch_count = 0
                batches = batches + 1
                list1.clear()
                time.sleep(10)
            um_uuid = row['um_uuid']
            # Paralyse below call in the future, Check with KYC team regarding rate limiting
            response = get_user_kyc_expiry(um_uuid)
            if response is not None:
                date_obj = datetime.datetime.strptime(response, "%d-%b-%Y")
                mysql_datetime = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                t = User(um_uuid, mysql_datetime)
                list1.append(t)
                batch_count = batch_count + 1
        if batch_count > 0:
            insert_kyc_expiry(de_cursor, list1)
            print("Inserted last batch: {}".format(batches))
    except Exception as e:
        print("Error reading data from MySQL table", e)
    finally:
        de_connection.close()
        print("MySQL connection is closed")


if __name__ == '__main__':
    get_user_data()
