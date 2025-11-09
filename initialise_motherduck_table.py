from dotenv import load_dotenv
from query_helpers import salesforce_auth, motherduck_auth, soql_to_ddl, run_sf_query

soql = """
select Id,SystemModstamp,IsDeleted,LastModifiedById,Account__c,Contractual_Relationship__c,Contract__c,CreatedById,CreatedDate,Name
from contract_account_junction__c
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