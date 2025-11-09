from dotenv import load_dotenv
from query_helpers import salesforce_auth, motherduck_auth, soql_to_ddl, run_sf_query

soql = """
SELECT Id,SystemModstamp,IsDeleted,LastModifiedById,ContactId,CreatedById,CreatedDate,Ex_Trialist_Rating__c,IsPrimary,OpportunityId,Role
FROM OpportunityContactRole
WHERE Opportunity.Account.Parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
AND Opportunity.account.recordtype.name <> 'Vendor'
"""

load_dotenv()

sf = salesforce_auth()
con = motherduck_auth()

ddl, base_object = soql_to_ddl(sf, soql)

con.sql(ddl)

df = run_sf_query(sf, soql, use_api='bulk')

print(df.head())

con.register("df", df)
con.sql(f"""
insert into "{base_object}" by name
select * from df
"""
)