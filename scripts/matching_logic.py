import os
import sys
from datetime import datetime, timedelta
from mysql.connector import Error
import string
import random
from utils import connect_to_rds, send_slack_message, format_message, get_last_10_chars, convert_to_uppercase, get_dob_match_type


not_matching_records_count = 0
invalid_universal_id_mapping = 0


def get_universal_id_mapped(urs_cursor, cl_um_uuid, tc_um_uuid):
    # Prepare a list of um_uuid values from both cl_record and tc_record
    um_uuids = [cl_um_uuid, tc_um_uuid]

    get_universal_id_mapped_query = """
        SELECT um_uuid, is_universal_id_mapped
        FROM user
        WHERE um_uuid IN %s
    """

    # Execute the query with um_uuids as the parameter
    urs_cursor.execute(get_universal_id_mapped_query, (tuple(um_uuids),))
    results = urs_cursor.fetchall()

    # Create a dictionary to store the results for um_uuid
    record_results = {record['um_uuid']: int.from_bytes(record['is_universal_id_mapped'], "big") if record['is_universal_id_mapped'] is not None else 0 for record in results}

    # Retrieve the results for cl_record and tc_record
    is_cl_universal_id_mapped = record_results.get(cl_um_uuid, 0)
    is_tc_universal_id_mapped = record_results.get(tc_um_uuid, 0)

    return is_cl_universal_id_mapped, is_tc_universal_id_mapped


def is_matching_records(cl_record, tc_record):
    # Check if the provided condition holds true
    return (
            cl_record['pan'] is not None and
            tc_record['pan'] is not None and
            convert_to_uppercase(cl_record['pan']) == convert_to_uppercase(tc_record['pan']) and
            cl_record['dob'] is not None and
            tc_record['dob'] is not None and
            get_dob_match_type(cl_record['dob'], tc_record['dob']) < 2 and
            cl_record['mobile'] is not None and
            tc_record['mobile'] is not None and
            cl_record['kyc_completed'] == 1 and
            tc_record['kyc_completed'] == 1 and
            cl_record['kyc_expiry'] is not None and
            tc_record['kyc_expiry'] is not None and
            cl_record['um_uuid'] is not None and
            tc_record['um_uuid'] is not None
    )


def print_log(trace, message):
    print("Trace: {i}, Message: {j}".format(i=trace, j=message))


def get_random_trace():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))


def process_batches(records_batch, batch_updates_user, batch_updates_cl, batch_updates_tc, batch_count,
                    urs_cursor, de_cursor):
    for record in records_batch:
        trace = get_random_trace()
        cl_record = {
            'um_uuid': record['cl_um_uuid'],
            'pan': record['cl_pan'],
            'dob': record['cl_dob'],
            'mobile': record['cl_mobile'],
            'kyc_expiry': record['cl_kyc_expiry'],
            'kyc_completed': record['cl_kyc_completed']
        }
        tc_record = {
            'um_uuid': record['tc_um_uuid'],
            'pan': record['tc_pan'],
            'dob': record['tc_dob'],
            'mobile': record['tc_mobile'],
            'kyc_expiry': record['tc_kyc_expiry'],
            'kyc_completed': record['tc_kyc_completed']
        }

        # Below pan check is added for extra safety
        if is_matching_records(cl_record, tc_record):
            if cl_record['kyc_expiry'] >= tc_record['kyc_expiry']:
                parent_um_uuid = cl_record['um_uuid']
            else:
                parent_um_uuid = tc_record['um_uuid']

            print_log(trace, "Parent um_uuid: {i} for CL um_uuid: {j} and TC um_uuid: {k}".format(
                i=str(parent_um_uuid), j=str(cl_record['um_uuid']), k=str(tc_record['um_uuid'])))

            # Retrieve the results for cl_record and tc_record
            is_cl_universal_id_mapped, is_tc_universal_id_mapped = get_universal_id_mapped(urs_cursor, cl_record['um_uuid'], tc_record['um_uuid'])

            if is_cl_universal_id_mapped == 0 and is_tc_universal_id_mapped == 0:
                # Collect updates for batch commit in batch_updates_user, batch_updates_cl, and batch_updates_tc lists
                batch_updates_user.append((parent_um_uuid, get_dob_match_type(cl_record['dob'], tc_record['dob']), cl_record['um_uuid']))
                batch_updates_user.append((parent_um_uuid, get_dob_match_type(cl_record['dob'], tc_record['dob']), tc_record['um_uuid']))
                print_log(trace, "Successfully Universal Mapping for CL Pan: " + cl_record['pan'] + " TC Pan: " + tc_record['pan'])
                print_log(trace, "Successfully Universal Mapping for CL um_uuid: " + cl_record['um_uuid'] + " TC um_uuid: " + tc_record['um_uuid'])
            elif is_cl_universal_id_mapped == 1 and is_tc_universal_id_mapped == 1:
                print_log(trace, "Universal Id is already mapped for CL um_uuid: {}, TC um_uuid: {}".format(cl_record['um_uuid'], tc_record['um_uuid']))
            else:
                global invalid_universal_id_mapping
                invalid_universal_id_mapping = invalid_universal_id_mapping + 1
                print_log(trace, "Invalid Universal Mapping for CL Pan: " + cl_record['pan'] + " TC Pan: " + tc_record['pan'])
                print_log(trace, "Invalid Universal Mapping for CL um_uuid: " + cl_record['um_uuid'] + " TC um_uuid: " + tc_record['um_uuid'])
        else:
            global not_matching_records_count
            not_matching_records_count = not_matching_records_count + 1
            print_log(trace, "Records are not matching CL Data: Mobile: {}, um_uuid: {}, dob: {}".format(cl_record['mobile'], cl_record['um_uuid'], cl_record['dob']))
            print_log(trace, "Records are not matching TC Data: Mobile: {}, um_uuid: {}, dob: {}".format(tc_record['mobile'], tc_record['um_uuid'], tc_record['dob']))

        batch_updates_cl.append(get_last_10_chars(cl_record['mobile']),)
        batch_updates_tc.append(get_last_10_chars(tc_record['mobile']),)

    try:
        # Update user for cl users table
        if batch_updates_user:
            update_query_user = """
                            UPDATE user
                            SET universal_id = %s, mapping_method = %s, is_universal_id_mapped = 1, updated_at = NOW(), universal_id_mapped_at = NOW()
                            WHERE um_uuid = %s
                            """
            urs_cursor.executemany(update_query_user, batch_updates_user)

        # Update cl_users table
        if batch_updates_cl:
            update_query_cl = "UPDATE cl_users SET record_state = 1, record_updated_at = NOW() WHERE mobile = %s"
            de_cursor.executemany(update_query_cl, batch_updates_cl)

        # Update tc_users table
        if batch_updates_tc:
            update_query_tc = "UPDATE tc_users SET record_state = 1, record_updated_at = NOW() WHERE mobile = %s"
            de_cursor.executemany(update_query_tc, batch_updates_tc)

        # Commit changes for this batch
        urs_cursor.connection.commit()
        de_cursor.connection.commit()
        print(f"Batch {batch_count} committed successfully.")
    except Error as e:
        urs_cursor.connection.rollback()
        de_cursor.connection.rollback()
        print(f"Error during batch {batch_count}: {e}")


def get_timestamp(cursor1, tenant_column):
    timestamp_query = "SELECT today_timestamp FROM timestamp_table WHERE column_name = %s"
    cursor1.execute(timestamp_query, (tenant_column,))
    result = cursor1.fetchone()
    timestamp = result['today_timestamp']
    return timestamp


def update_timestamp(cursor1, script_start_time):
    update_timestamp_query = "UPDATE timestamp_table SET today_timestamp = %s"
    cursor1.execute(update_timestamp_query, (script_start_time,))
    print("Update Last timestamp as: " + str(script_start_time))
    cursor1.connection.commit()


def get_total_mapped_universal_id_count(cursor1, script_start_time):
    rounded_time = script_start_time - timedelta(seconds=script_start_time.second, microseconds=script_start_time.microsecond)
    get_total_mapped_universal_id_count_query = "SELECT COUNT(*) AS total_records FROM user WHERE universal_id_mapped_at >= %s"
    cursor1.execute(get_total_mapped_universal_id_count_query, (rounded_time,))
    result = cursor1.fetchone()
    if result is not None:
        return result['total_records']//2
    else:
        return 0


common_cl_where_clause = """
    cl.record_state = 0
    AND cl.record_updated_at > %s
    AND cl.is_agreement_signed = 1
    AND tc.is_mitc_signed = 1
    AND cl.kyc_completed = 1
    AND tc.kyc_completed = 1
    AND cl.um_uuid IS NOT NULL
    AND tc.um_uuid IS NOT NULL
    AND cl.pan IS NOT NULL
    AND tpkd.pan IS NOT NULL
"""

# Above cl.um_uuid IS NOT NULL check is added because um_uuid can be null
# when data is not properly sync between pscore and user registration


def get_total_cl_records(cursor1, timestamp):
    query = f"""
            SELECT COUNT(*) AS total_records
            FROM cl_users cl
            JOIN tc_pan_kyc_data tpkd ON cl.pan = tpkd.pan
            JOIN tc_users tc on tpkd.um_uuid = tc.um_uuid
            WHERE {common_cl_where_clause}
            """
    cursor1.execute(query, (timestamp,))
    result = cursor1.fetchone()
    if result is not None:
        return result['total_records']
    else:
        return 0


def get_cl_batch(cursor1, batch_size, offset, cl_timestamp):
    query = f"""
            SELECT cl.um_uuid AS cl_um_uuid, cl.mobile AS cl_mobile, cl.kyc_expiry AS cl_kyc_expiry, cl.pan AS cl_pan, cl.dob AS cl_dob, cl.kyc_completed as cl_kyc_completed,
            tc.um_uuid AS tc_um_uuid, tc.mobile AS tc_mobile, tpkd.kyc_expiry_date AS tc_kyc_expiry, tpkd.pan AS tc_pan, tc.dob AS tc_dob, tc.kyc_completed as tc_kyc_completed
            FROM cl_users cl
            JOIN tc_pan_kyc_data tpkd ON cl.pan = tpkd.pan
            JOIN tc_users tc on tpkd.um_uuid = tc.um_uuid  
            WHERE {common_cl_where_clause}
            LIMIT %s OFFSET %s
            """
    cursor1.execute(query, (cl_timestamp, batch_size, offset))
    batch = cursor1.fetchall()
    return batch


common_tc_where_clause = """
    tc.record_state = 0
    AND tc.record_updated_at > %s
    AND cl.is_agreement_signed = 1
    AND tc.is_mitc_signed = 1
    AND cl.kyc_completed = 1
    AND tc.kyc_completed = 1
    AND cl.um_uuid IS NOT NULL
    AND tc.um_uuid IS NOT NULL
    AND cl.pan IS NOT NULL
    AND tpkd.pan IS NOT NULL
"""


def get_total_tc_records(cursor1, timestamp):
    query = f"""
            SELECT COUNT(*) AS total_records
            FROM cl_users cl
            JOIN tc_pan_kyc_data tpkd ON cl.pan = tpkd.pan
            JOIN tc_users tc on tpkd.um_uuid = tc.um_uuid
            WHERE {common_tc_where_clause}
            """
    cursor1.execute(query, (timestamp,))
    result = cursor1.fetchone()
    if result is not None:
        return result['total_records']
    else:
        return 0


def get_tc_batch(cursor1, batch_size, offset, tc_timestamp):
    query = f"""
                SELECT cl.um_uuid AS cl_um_uuid, cl.mobile AS cl_mobile, cl.kyc_expiry AS cl_kyc_expiry, cl.pan AS cl_pan, cl.dob AS cl_dob, cl.kyc_completed as cl_kyc_completed,
                tc.um_uuid AS tc_um_uuid, tc.mobile AS tc_mobile, tpkd.kyc_expiry_date AS tc_kyc_expiry, tpkd.pan AS tc_pan, tc.dob AS tc_dob, tc.kyc_completed as tc_kyc_completed
                FROM cl_users cl
                JOIN tc_pan_kyc_data tpkd ON cl.pan = tpkd.pan
                JOIN tc_users tc on tpkd.um_uuid = tc.um_uuid
                WHERE {common_tc_where_clause}
                LIMIT %s OFFSET %s
            """
    cursor1.execute(query, (tc_timestamp, batch_size, offset))
    batch = cursor1.fetchall()
    return batch


def run_matching_script():
    de_connection = connect_to_rds(UNIVERSAL_ID_DB_CONNECTION)
    urs_connection = connect_to_rds(URS_DB_CONNECTION)

    de_cursor = de_connection.cursor()
    urs_cursor = urs_connection.cursor()
    script_start_time = datetime.now()
    print("Script Start Time: " + str(script_start_time))

    try:
        cl_timestamp = get_timestamp(urs_cursor, 'cl_timestamp')

        total_cl_records_count = get_total_cl_records(de_cursor, cl_timestamp)
        batch_count = 0

        print("Record CL Count: " + str(total_cl_records_count))

        for offset in range(0, total_cl_records_count, BATCH_SIZE):
            records_batch = get_cl_batch(de_cursor, BATCH_SIZE, offset, cl_timestamp)
            if len(records_batch) == 0:
                print("Continuing from CL: " + str(batch_count))
                continue
            batch_updates_user = []
            batch_updates_cl = []
            batch_updates_tc = []
            batch_count += 1
            print("Starting Processing for CL Batch of count" + str(batch_count))
            print("CL Batch Size: " + str(len(records_batch)))
            process_batches(records_batch, batch_updates_user,
                            batch_updates_cl, batch_updates_tc, batch_count, urs_cursor, de_cursor)

        # Get tc_timestamp from timestamp_table
        tc_timestamp = get_timestamp(urs_cursor, 'tc_timestamp')
        total_tc_records_count = get_total_tc_records(de_cursor, tc_timestamp)
        batch_count = 0

        print("Record TC Count: " + str(total_tc_records_count))

        for offset in range(0, total_tc_records_count, BATCH_SIZE):
            records_batch = get_tc_batch(de_cursor, BATCH_SIZE, offset, tc_timestamp)
            if len(records_batch) == 0:
                print("Continuing from TC: " + str(batch_count))
                continue
            batch_updates_user = []
            batch_updates_cl = []
            batch_updates_tc = []
            batch_count += 1
            print("Starting Processing for TC Batch of count" + str(batch_count))
            print("TC Batch Size: " + str(len(records_batch)))
            process_batches(records_batch,  batch_updates_user,
                            batch_updates_cl, batch_updates_tc, batch_count, urs_cursor, de_cursor)

        total_mapped_universal_id_count = get_total_mapped_universal_id_count(urs_cursor, script_start_time)
        print("Total Mapped Universal ID Count: " + str(total_mapped_universal_id_count))

        message_dict = {
            'Total Mapped Universal ID': total_mapped_universal_id_count,
            'Total valid CL initial records': total_cl_records_count,
            'Total valid TC initial records': total_tc_records_count,
            'Total not matching records count': not_matching_records_count,
            'Total invalid universal id mapping records count': invalid_universal_id_mapping
        }
        send_slack_message(format_message(message_dict))
        # This is updating to script_start_time so that record updated in DE DB while we run this matching script gets picked in next run
        update_timestamp(urs_cursor, script_start_time)

    except Error as e:
        print(f"Error: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    finally:
        de_connection.close()
        urs_connection.close()
        print("MySQL connections is closed")


if __name__ == '__main__':
    run_matching_script()
