from scripts.constants import BATCH_SIZE
from scripts.user import User
from typing import List


class BaseTenant:
    """This class is base class for all the tenants and any new tenant should extend to this class and implements its methods"""

    def __init__(self, tenant_id, name, table_name):
        self.tenant_id = tenant_id
        self.name = name
        self.table_name = table_name
        self.priority = None
        self.user_updated_timestamp = None

    def get_valid_user_count(self, cursor, timestamp):
        """This method should return valid users count which updated after given timestamp from respective tenants"""
        raise NotImplementedError

    def get_batch(self, cursor, offset, timestamp, batch_size=BATCH_SIZE) -> List[User]:
        """This method returns valid users batch which updated after given timestamp from respective tenants"""
        raise NotImplementedError

    def get_user_from_pan(self, cursor, pan) -> User:
        """This method returns user from the table"""
        raise NotImplementedError

    def get_user_from_um_uuid(self, cursor, um_uuid) -> User:
        """This method returns user from the table"""
        raise NotImplementedError

    def update_state_and_updated_at_timestamp(self, cursor, batch_update_tenant_user_table):
        """This method updates the user state and updated timestamp for the given list of users"""
        raise NotImplementedError

