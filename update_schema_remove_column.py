# 1) run this script
# 2) update incremental load script to remove column from relevant soql
# 3) commit to repo

from dotenv import load_dotenv
from query_helpers import motherduck_auth

table = "Account"
column = "Pure_Commods_Account__c"

load_dotenv()
con = motherduck_auth()

con.execute(f"""
alter table {table}
drop column {column}
""")