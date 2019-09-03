# $Id: filemgmt_defs.py 46644 2018-03-12 19:54:58Z friedel $
# $Rev:: 46644                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-03-12 14:54:58 #$:  # Date of last commit.

""" Constants used in file management code

"""

# lower case because appears as wcl section and wcl sections are converted to lowercase
META_HEADERS = 'h'
META_COMPUTE = 'c'
META_WCL = 'w'
META_COPY = 'p'
META_REQUIRED = 'r'
META_OPTIONAL = 'o'

FILETYPE_METADATA = 'filetype_metadata'
FILE_HEADER_INFO = 'file_header'

USE_HOME_ARCHIVE_INPUT = 'use_home_archive_input'
USE_HOME_ARCHIVE_OUTPUT = 'use_home_archive_output'

FM_PREFER_UNCOMPRESSED = [None, '.fz', '.gz']
FM_PREFER_COMPRESSED = ['.fz', '.gz', None]
FM_UNCOMPRESSED_ONLY = [None]
FM_COMPRESSED_ONLY = ['.fz', '.gz']

FM_EXIT_SUCCESS = 0
FM_EXIT_FAILURE = 1
FW_MSG_ERROR = 3
FW_MSG_WARN = 2
FW_MSG_INFO = 1

PROV_USED_TABLE = "OPM_USED"
#PROV_WGB_TABLE  = "OPM_WAS_GENERATED_BY"
PROV_WDF_TABLE = "OPM_WAS_DERIVED_FROM"
PROV_TASK_ID = "TASK_ID"
PROV_FILE_ID = "DESFILE_ID"
PROV_PARENT_ID = "PARENT_DESFILE_ID"
PROV_CHILD_ID = "CHILD_DESFILE_ID"

# 1024**4
TB = 1099511627776.0
# 1024**3
GB = 1073741824.0
# 1024**2
MB = 1048576.0
# 1024**1
KB = 1024.0
