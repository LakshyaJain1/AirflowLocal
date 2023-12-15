from typing import List

from scripts.tenants.base_tenant import BaseTenant
from scripts.tenants.cl_tenant import CLTenant
from scripts.constants import TC_TENANT_ID, CL_TENANT_ID, SMB_TENANT_ID, UNIVERSAL_ID_DB_CONNECTION
from scripts.tenants.smb_tenant import SMBTenant
from scripts.tenants.tc_tenant import TCTenant
from scripts.utils import connect_to_rds


class TenantFactory:
    @staticmethod
    def create_tenant(tenant_id) -> BaseTenant:
        if tenant_id == CL_TENANT_ID:
            return CLTenant()
        elif tenant_id == SMB_TENANT_ID:
            return SMBTenant()
        elif tenant_id == TC_TENANT_ID:
            return TCTenant()
        else:
            raise ValueError(f"No matching tenant found for tenant_id: {tenant_id}")


class TenantManager:
    _tenant_list = []  # Static variable to store tenant list once fetched

    @staticmethod
    def get_ordered_tenant_list() -> List[BaseTenant]:
        connection = connect_to_rds(UNIVERSAL_ID_DB_CONNECTION)
        cursor = connection.cursor()
        if TenantManager._tenant_list:
            return TenantManager._tenant_list

        # If tenant list is empty or not present in memory, fetch it from the database
        # Perform a database query to fetch tenant data ordered by priority
        # 1 is the highest priority

        query = "SELECT tenant_id, name, table_name, priority, user_updated_timestamp FROM tenant ORDER BY priority ASC"
        cursor.execute(query)

        # Fetch all rows as tuples and create instances using the TenantFactory
        rows = cursor.fetchall()
        tenant_list = []
        for row in rows:
            tenant_id, name, table_name, priority, user_updated_timestamp = row
            # Create instances using the TenantFactory
            tenant = TenantFactory.create_tenant(tenant_id)
            tenant.priority = priority
            tenant.user_updated_timestamp = user_updated_timestamp  # Assuming you assign this attribute
            tenant_list.append(tenant)

        cursor.close()

        # Store fetched tenant list in the static variable
        TenantManager._tenant_list = tenant_list
        return TenantManager._tenant_list

    @staticmethod
    def update_tenant_timestamp(cursor, tenant_id, tenant_start_time):
        update_query = "UPDATE tenant SET user_updated_timestamp = %s WHERE tenant_id = %s"
        cursor.execute(update_query, (tenant_start_time, tenant_id,))
        cursor.connection.commit()


def get_tenant_priority(tenant_id):
    for tenant in TenantManager.get_ordered_tenant_list():
        if tenant.tenant_id == tenant_id:
            return tenant.priority  # Return the priority when matching tenant_id is found

