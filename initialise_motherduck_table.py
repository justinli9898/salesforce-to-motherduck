from dotenv import load_dotenv
from query_helpers import salesforce_auth, motherduck_auth, soql_to_ddl, run_sf_query

soql = """
SELECT Id,SystemModstamp,LastModifiedById,Activate_by_Default__c,Billable__c,Bloomberg_Sub_Channel__c,CreatedById,CreatedDate,CurrencyIsoCode,Description,Distribution_Channel__c,Family,IsActive,IsArchived,IsDeleted,LastModifiedDate,Main_Package__c,Name,Name_for_Rollups__c,ParentProduct__c,ProductCode,Revenue_Stream__c,Select_By_Default_From_Parent__c,Types__c,Website_Offering__c
FROM Product2
"""

load_dotenv()

sf = salesforce_auth()
con = motherduck_auth()

ddl, base_object = soql_to_ddl(sf, soql)

con.sql(ddl)

df = run_sf_query(sf, soql, use_api='rest')

print(df.head())

con.register("df", df)
con.sql(f"""
insert into "{base_object}" by name
select * from df
"""
)