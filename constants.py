import os

# ARCU Database Connection String
ARCU_CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=VSARCU02;"
    "Database=ARCUSYM000;"
    "Trusted_Connection=yes;"
    "MARS_Connection=yes;"
)

# KRAP Database Connection String
KRAP_CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=VSARCU02;"
    "Database=kRAP;"
    "Trusted_Connection=yes;"
)

isTest = False  # Set to True for testing, False for production
# Paths

SOURCE_FOLDER = r"\\vsarcu02\k$\ARCUFTP_ARCHIVE\SYM000"

STAGING_FOLDER = os.path.join(os.path.dirname(__file__), "staging")
NUM_EXTRACTS_TO_KEEP = 7
if isTest:
    DESTINATION_FOLDER = os.path.join(os.path.dirname(__file__), "test_destination")
else:
    DESTINATION_FOLDER = r"\\kfcu\share\PR\MIS\ASD Area\HubSpot"
