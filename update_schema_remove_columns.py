# 1) run this script
# 2) update incremental load script to remove column from relevant soql
# 3) commit to repo

from dotenv import load_dotenv
from query_helpers import motherduck_auth

table = "opportunity"
columns = [
    "Conga_Addendum_TemplateID__c",
    "Conga_HSC_Addendum__c",
    "Conga_MSA_TemplateID__c",
    "Created_Closed_Same_Day__c",
    "Max_Trial_End_Roll_Up_Helper__c",

    "LastActivityDate",
    "Last_Activity_Assignee__c",
    "Last_Activity_Date__c",
    "Last_Activity_Type__c",
]

load_dotenv()
con = motherduck_auth()

for column in columns:
    con.execute(f"""
    alter table {table}
    drop column {column}
    """)