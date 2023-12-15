import hashlib
from mysql.connector import Error
import json, requests

from scripts.constants import UNIVERSAL_ID_DB_CONNECTION, BATCH_SIZE
from utils import connect_to_rds
from airflow.models import Variable

DOC_STORE_ENDPOINT = "DOC_STORE_ENDPOINT"


class User:
    def __init__(self, um_uuid, pan, pan_hash):
        self.um_uuid = um_uuid
        self.pan = pan
        self.pan_hash = pan_hash


def insert_pan_in_db(de_cursor, list_of_users):
    try:
        sql = '''UPDATE tc_pan_kyc_data SET pan = %s, pan_hash = %s WHERE um_uuid = %s'''
        list_of_tuples = []
        for obj in list_of_users:
            tuple1 = (obj.pan, obj.pan_hash, obj.um_uuid)
            list_of_tuples.append(tuple1)
        de_cursor.executemany(sql, list_of_tuples)
        de_cursor.connection.commit()
    except Error as e:
        print("Error reading data from MySQL table", e)


def get_user_list(de_cursor):
    try:
        sql = """SELECT tpkd.um_uuid AS um_uuid, tcu.pan_ref AS pan_ref FROM tc_users tcu
        JOIN tc_pan_kyc_data tpkd ON tcu.um_uuid = tpkd.um_uuid
        WHERE tpkd.pan IS NULL"""
        de_cursor.execute(sql)
        records = de_cursor.fetchall()
        return records
    except Error as e:
        print("Error reading data from MySQL table", e)


def get_pan(pan_ref):
    try:
        payload = {"revealSensitiveInfo": "true", "includeDeleted": "true", 'refKey': pan_ref}
        headers = {'content-type': "application/json", 'authToken': "abc"}
        end_point = Variable.get(DOC_STORE_ENDPOINT) + "/sensitiveInfo/retrieveSensitiveInfo"
        response = requests.post(end_point, json=payload,
                                 headers=headers)
        if response.status_code == 200:
            d = json.loads(response.content)
            return d['sensitiveInfoEntryResponses'][0]['sensitiveInfo']
        else:
            return None
    except Exception as Ex:
        return None


def encrypt_pan(input_var):
    sha_value = hashlib.sha256(str(input_var).encode()).hexdigest()
    return sha_value


def update_pan():
    de_connection = connect_to_rds(UNIVERSAL_ID_DB_CONNECTION)
    de_cursor = de_connection.cursor()
    try:
        data = get_user_list(de_cursor)
        print("Pan is null for count: {}".format(len(data)))
        list1 = []
        batch_count = 0
        batches = 1
        for row in data:
            if batch_count == BATCH_SIZE:
                insert_pan_in_db(de_cursor, list1)
                print("Inserted batch: {}".format(batches))
                batch_count = 0
                batches = batches + 1
                list1.clear()
            um_uuid = row['um_uuid']
            pan_ref = row['pan_ref']
            pan = get_pan(pan_ref)
            pan_hash = encrypt_pan(pan)
            print("um_uuid:{}, pan_ref:{}, pan: {}, pan_hash: {}".format(um_uuid, pan_ref, pan, pan_hash))
            t = User(um_uuid, pan, pan_hash)
            list1.append(t)
            batch_count = batch_count + 1
        if batch_count > 0:
            insert_pan_in_db(de_cursor, list1)
            print("Inserted last batch: {}".format(batches))
    except Exception as e:
        print("Error reading data from MySQL table", e)
    finally:
        de_connection.close()
        print("MySQL connection is closed")


if __name__ == '__main__':
    update_pan()
