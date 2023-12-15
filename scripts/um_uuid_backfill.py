import mysql.connector

import sys
from os.path import dirname, abspath

BASE_DIR = dirname(dirname(abspath(__file__)))
print("BASE-DIR")
print(BASE_DIR)
sys.path.append(BASE_DIR)

from scripts.constants import UNIVERSAL_ID_DB_CONNECTION, BATCH_SIZE
from utils import connect_to_rds


def insert_users_in_db(de_cursor, list_of_um_uuid):
    try:
        sql = "INSERT INTO tc_pan_kyc_data (um_uuid) VALUES (%s)"
        data_to_insert = [(um_uuid,) for um_uuid in list_of_um_uuid]
        de_cursor.executemany(sql, data_to_insert)
        de_cursor.connection.commit()
    except mysql.connector.Error as e:
        print("Error reading data from MySQL table", e)


def get_user_list(de_cursor):
    try:
        sql = """SELECT tcu.um_uuid FROM tc_users tcu 
        LEFT JOIN tc_pan_kyc_data tpkd ON tcu.um_uuid = tpkd.um_uuid
        WHERE tcu.is_mitc_signed = 1 and tcu.kyc_completed = 1 
        AND tpkd.um_uuid IS NULL"""
        de_cursor.execute(sql)
        records = de_cursor.fetchall()
        return records
    except mysql.connector.Error as e:
        print("Error reading data from MySQL table", e)


def insert_new_users():
    de_connection = connect_to_rds(UNIVERSAL_ID_DB_CONNECTION)
    de_cursor = de_connection.cursor()
    try:
        data = get_user_list(de_cursor)
        print("New users count to be inserted: {}".format(len(data)))
        list1 = []
        batch_count = 0
        batches = 1
        for row in data:
            if batch_count == BATCH_SIZE:
                insert_users_in_db(de_cursor, list1)
                batch_count = 0
                batches = batches + 1
                list1.clear()
            um_uuid = row['um_uuid']
            list1.append(um_uuid)
            batch_count = batch_count + 1
        if batch_count > 0:
            insert_users_in_db(de_cursor, list1)
            print("Inserted last batch: {}".format(batches))
    except Exception as e:
        print("Error reading data from MySQL table", e)
    finally:
        de_connection.close()
        print("MySQL connection is closed")


if __name__ == '__main__':
    insert_new_users()
