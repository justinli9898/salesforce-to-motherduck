from dotenv import load_dotenv
from query_helpers import salesforce_auth, motherduck_auth, soql_to_ddl, run_sf_query

soql = """
SELECT Account__c,Active__c,Comment__c,Contact__c,Content__c,CreatedById,CreatedDate,Distribution_Channel__c,Edge_Region__c,Email_Rendering__c,Email__c,End_Date__c,Id,IsDeleted,LastModifiedById,LastModifiedDate,Name,Opportunity__c,OwnerId,Product2__c,Product_Name_text__c,Rebelmouse_User_ID__c,Start_Date__c,SystemModstamp,Turn_Off_Email_Access__c
FROM Free_of_Cost_Items__c
WHERE account__r.parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
AND account__r.recordtype.name <> 'Vendor'
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