from base_tenant import BaseTenant
from scripts.constants import BATCH_SIZE, TC_TENANT_ID, TC_TENANT_NAME, TC_TABLE_NAME
from scripts.user import User
from mysql.connector import Error


class TCTenant(BaseTenant):
    def __init__(self):
        super().__init__(TC_TENANT_ID, TC_TENANT_NAME, TC_TABLE_NAME)

    def get_valid_user_count(self, cursor, last_updated_timestamp):
        query = f"SELECT COUNT(*) FROM {self.table_name} WHERE record_updated_at > '{last_updated_timestamp}' AND state = 0 AND is_mitc_signed = 1"
        cursor.execute(query)
        result = cursor.fetchone()
        count = result[0] if result is not None else 0
        return count

    def get_batch(self, cursor, offset, last_updated_timestamp, batch_size=BATCH_SIZE):
        query = f"""
                SELECT * FROM tc_users tc
                JOIN tc_pan_kyc_data tpkd on tpkd.um_uuid = tc.um_uuid
                WHERE record_updated_at > %s AND state = 0 AND is_mitc_signed = 1
                LIMIT %s OFFSET %s
                """
        cursor.execute(query, (last_updated_timestamp, batch_size, offset))
        batch = cursor.fetchall()
        return batch

    def get_user_from_pan(self, cursor, pan):
        query = f"""SELECT tc.nsdl_name, tc.dob, tpkd.pan, tc.um_uuid FROM {self.table_name} tc
        JOIN tc_pan_kyc_data tpkd on tpkd.um_uuid = tc.um_uuid
        WHERE tpkd.pan = {pan}"""
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            name = result['nsdl_name']
            dob = result['dob']
            user_pan = result['pan']
            uuid = result['um_uuid']
            tenant_id = self.tenant_id
            user = User(name, dob, user_pan, uuid, tenant_id)
            return user
        else:
            return None

    def get_user_from_um_uuid(self, cursor, um_uuid):
        query = f"""SELECT tc.nsdl_name, tc.dob, tpkd.pan, tc.mobile, tc.um_uuid FROM {self.table_name} tc
        JOIN tc_pan_kyc_data tpkd on tpkd.um_uuid = tc.um_uuid
        WHERE tc.um_uuid = {um_uuid}"""
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            name = result['nsdl_name']
            dob = result['dob']
            user_pan = result['pan']
            mobile = result['mobile']
            um_uuid = result['um_uuid']
            tenant_id = self.tenant_id
            user = User(name, dob, user_pan, mobile, um_uuid, tenant_id)
            return user
        else:
            return None

    def update_state_and_updated_at_timestamp(self, cursor, batch_update_tenant_user_table):
        if batch_update_tenant_user_table:
            try:
                update_query_tc = f"UPDATE {self.table_name} SET record_state = 1, record_updated_at = NOW() WHERE um_uuid = %s"
                cursor.executemany(update_query_tc, batch_update_tenant_user_table)
                cursor.connection.commit()
            except Error as e:
                cursor.connection.rollback()
                print(f"Error during cl update state: {e}")
