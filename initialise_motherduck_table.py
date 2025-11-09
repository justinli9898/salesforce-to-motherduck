from dotenv import load_dotenv
from query_helpers import salesforce_auth, motherduck_auth, soql_to_ddl, run_sf_query

soql = """
select Id, SystemModstamp, IsDeleted, LastModifiedById, AccountId, Active_Account_Lookup__c, Anniversary_Date__c, Auto_Renew__c, Base_MSA__c, Cancellation_Terms_days__c, ContractNumber, ContractTerm, Contractual_MRR_cc__c, Contract_End_Date__c, Contract_Status__c, Contract_Type__c, CreatedById, CreatedDate, Current_Contract_Renewed__c, Description, EndDate, Exit_2__c, Exit__c, Expiry_Date__c, Original_MSA_Commencement_Date__c, Price_Increase_Notice_Period__c, RecordTypeId, Roll_Date__c, SpecialTerms, StartDate, Subcontract_Type__c
from contract
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