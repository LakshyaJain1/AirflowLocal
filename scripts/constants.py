import sys
from os.path import dirname, abspath

BASE_DIR = dirname(dirname(dirname(abspath(__file__))))
print(BASE_DIR)
sys.path.append(BASE_DIR)


BATCH_SIZE = 1000
UNIVERSAL_ID_DB_CONNECTION = "UNIVERSAL_ID_DB_CONNECTION"
URS_DB_CONNECTION = "URS_DB_CONNECTION"

TC_TENANT_ID = "ec35b493"
TC_TENANT_NAME = "TC"
TC_TABLE_NAME = "tc_users"

CL_TENANT_ID = "53ff160e"
CL_TENANT_NAME = "CL"
CL_TABLE_NAME = "cl_users"

SMB_TENANT_ID = "gra2c371"
SMB_TENANT_NAME = "SMB"
SMB_TABLE_NAME = "smb_users"

"""
Increment this version whenever you make any changes in algorithm or decision tree
"""
MATCHING_ALGO_VERSION = 1
