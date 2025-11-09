# 1) run this script
# 2) update incremental load script to use updated_soql
# 3) commit to repo

from dotenv import load_dotenv
from query_helpers import salesforce_auth, motherduck_auth, run_sf_query

updated_soql = """
    select Id, SystemModstamp, IsDeleted, LastModifiedById, ParentId, Parent_Account__c, Name, Parent_Account_Consolidated__c, CreatedDate, Region__c, ShippingCountry, Buy_Sell_Side__c, Buy_Side_Type__c, Customer_Type__c, Active_People_Subs__c, Active_Trials__c, Ex_Subscribers__c, Ex_Trialists__c, Subscription_Amount_cc__c, Amount_Cancelling_cc__c, Precautionary_Cancel_cc__c, Net_Cancel_cc__c, Cancel_Type__c, CreatedById, CreatedBy.Name, OwnerId, Owner.Name, Pure_Commods_Account__c
    from account
    where Parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
    and recordtype.name <> 'Vendor'
    ORDER BY SystemModstamp DESC
"""

table = "account"
new_field = "Pure_Commods_Account__c"
datatype = "VARCHAR"

load_dotenv()
sf = salesforce_auth()
con = motherduck_auth()

df = run_sf_query(sf, updated_soql)

# make updates to table in Motherduck
con.execute("BEGIN TRANSACTION;")

try:
    con.execute(f"""
    ALTER TABLE {table}
    ADD COLUMN {new_field} {datatype}
    """)

    con.execute(f"""
    CREATE OR REPLACE TABLE {table} AS
    SELECT * EXCLUDE (ingested_at, {new_field}), {new_field}, ingested_at
    FROM {table}
    """)
    
    con.execute(f"""
    TRUNCATE TABLE {table}
    """)
    
    con.execute(f"""
    INSERT INTO {table} BY NAME
    SELECT * FROM df
    """)

    con.execute("COMMIT;")
except Exception as e:
    con.execute("ROLLBACK;")
    raise