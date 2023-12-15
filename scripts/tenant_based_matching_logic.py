from typing import List, Tuple

from scripts.tenants.base_tenant import BaseTenant
from scripts.constants import UNIVERSAL_ID_DB_CONNECTION, BATCH_SIZE, URS_DB_CONNECTION, MATCHING_ALGO_VERSION
from scripts.rules import DecisionMaker
from scripts.user import User
from utils import connect_to_rds, get_dob_match_type, get_name_match
from name_match import perform_name_match
from tenant_utils import TenantManager, get_tenant_priority, TenantFactory
from datetime import datetime


# Returns high priority user first, 1 is the highest priority
def get_user_basis_priority(current_user: User, tenant_user: User) -> Tuple[User, User]:
    tenant_priority_1 = get_tenant_priority(current_user.tenant_id)
    tenant_priority_2 = get_tenant_priority(tenant_user.tenant_id)

    if tenant_priority_1 < tenant_priority_2:
        return current_user, tenant_user
    else:
        return tenant_user, current_user


def process_no_parent(de_cursor, current_user: User, aux_list: List[Tuple[User, User]], current_tenant: BaseTenant):
    tenant_list = TenantManager.get_ordered_tenant_list()
    for tenant in tenant_list:
        if tenant.tenant_id != current_tenant.tenant_id:
            tenant_user = tenant.get_user_from_um_uuid(de_cursor, current_user.um_uuid)
            if tenant_user is not None:
                aux_list.append(get_user_basis_priority(current_user, tenant_user))
                break


def fetch_entries(urs_cursor, um_uuid_tenant1):
    query = "SELECT tenant_2_id, um_uuid_tenant2 FROM auxiliary_table WHERE um_uuid_tenant1 = %s;"
    urs_cursor.execute(query, (um_uuid_tenant1,))
    entries = urs_cursor.fetchall()
    return entries


def fetch_user_list_having_parent_id(urs_cursor, de_cursor, parent_uuid) -> List[User]:
    entries = fetch_entries(urs_cursor, parent_uuid)
    user_list = []
    for entry in entries:
        tenant_id = entry[0]
        um_uuid_tenant = entry[1]
        tenant_instance = TenantFactory.create_tenant(tenant_id)
        user = tenant_instance.get_user_from_um_uuid(de_cursor, um_uuid_tenant)
        user_list.append(user)
    return user_list


def process_same_parent_and_user(urs_cursor, de_cursor, current_user: User, aux_list: List[Tuple[User, User]]):
    user_list = fetch_user_list_having_parent_id(urs_cursor, de_cursor, current_user.um_uuid)
    for user in user_list:
        aux_list.append((current_user, user))


def upsert_into_aux_table(urs_cursor, decision, parent_user, child_user, dob_match_type, name_match_type, name_match_score):
    dob_remarks = ""
    name_remark = ""
    upsert_query = """
            INSERT INTO auxiliary_table (pan, tenant_1_id, tenant_2_id, um_uuid_tenant1, um_uuid_tenant2, dob_match,
                                         dob_remarks, name_match, name_score, name_remark,
                                         matching_algo_version, decision, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                um_uuid_tenant1 = VALUES(um_uuid_tenant1),
                um_uuid_tenant2 = VALUES(um_uuid_tenant2),
                dob_match = VALUES(dob_match),
                dob_remarks = VALUES(dob_remarks),
                mobile_match = VALUES(mobile_match),
                mobile_remark = VALUES(mobile_remark),
                name_match = VALUES(name_match),
                name_score = VALUES(name_score),
                name_remark = VALUES(name_remark),
                matching_algo_version = VALUES(matching_algo_version),
                decision = VALUES(decision),
                updated_at = NOW()
        """

    urs_cursor.execute(upsert_query, (
        parent_user.pan, parent_user.tenant_id, child_user.tenant_id, parent_user.um_uuid, child_user.um_uuid,
        dob_match_type.name, dob_remarks, name_match_type.name, name_match_score, name_remark,
        MATCHING_ALGO_VERSION, decision))

    urs_cursor.connection.commit()


def process_aux_list(urs_cursor, aux_list: List[Tuple[User, User]]):
    for parent_user, child_user in aux_list:
        dob_match_type = get_dob_match_type(child_user.dob, parent_user.dob)
        name_match_score = perform_name_match(child_user, parent_user)
        name_match_type = get_name_match(name_match_score)
        decision = DecisionMaker.make_decision(dob_match_type, name_match_type)
        upsert_into_aux_table(urs_cursor, decision, parent_user, child_user, dob_match_type, name_match_type, name_match_score)


def process_tenant_batches(de_cursor, urs_cursor, current_tenant: BaseTenant, records_batch: List[User]):
    aux_list = []
    batch_update_tenant_user_table = []
    for current_user in records_batch:
        batch_update_tenant_user_table.append(current_user.um_uuid)
        parent_user = get_parent_user_from_aux_table(de_cursor, urs_cursor, current_user.pan)
        if parent_user is None:
            process_no_parent(de_cursor, current_user, aux_list, current_tenant)
        elif parent_user.tenant_id == current_user.tenant_id:
            process_same_parent_and_user(urs_cursor, de_cursor, current_user, aux_list)
        else:
            aux_list.append((parent_user, current_user))

    process_aux_list(urs_cursor, aux_list)
    current_tenant.update_state_and_updated_at_timestamp(de_cursor, batch_update_tenant_user_table)


def get_parent_user_from_aux_table(de_cursor, urs_cursor, pan) -> User:
    query = "SELECT tenant_1_id, um_uuid_tenant1 FROM auxiliary_table WHERE pan = %s LIMIT 1"
    urs_cursor.execute(query, (pan,))
    result = urs_cursor.fetchone()
    tenant_id = result[0]
    parent_um_uuid = result[1]
    parent_tenant = TenantFactory.create_tenant(tenant_id)
    return parent_tenant.get_user_from_um_uuid(de_cursor, parent_um_uuid)


def process_tenant(current_tenant: BaseTenant):
    de_connection = connect_to_rds(UNIVERSAL_ID_DB_CONNECTION)
    de_cursor = de_connection.cursor()
    urs_connection = connect_to_rds(URS_DB_CONNECTION)
    urs_cursor = urs_connection.cursor()
    timestamp = current_tenant.user_updated_timestamp
    tenant_start_time = datetime.now()
    count = current_tenant.get_valid_user_count(de_cursor, timestamp)
    for offset in range(0, count, BATCH_SIZE):
        records_batch = current_tenant.get_batch(de_cursor, offset, timestamp, BATCH_SIZE)
        process_tenant_batches(de_cursor, urs_cursor, current_tenant, records_batch)

    TenantManager.update_tenant_timestamp(de_cursor, current_tenant.tenant_id, tenant_start_time)


def run_matching_script():
    tenant_list = TenantManager.get_ordered_tenant_list()
    for tenant in tenant_list:
        process_tenant(tenant)


if __name__ == '__main__':
    run_matching_script()
