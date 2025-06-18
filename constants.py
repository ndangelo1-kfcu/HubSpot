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

source_folder = r"\\vsarcu02\k$\ARCUFTP_ARCHIVE\SYM000"

staging_folder = os.path.join(os.path.dirname(__file__), "staging")
num_extracts_to_keep = 7
if isTest:
    destination_folder = os.path.join(os.path.dirname(__file__), "test_destination")
else:
    destination_folder = r"\\kfcu\share\PR\MIS\ASD Area\HubSpot"
