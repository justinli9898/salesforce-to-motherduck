from dotenv import load_dotenv
from query_helpers import salesforce_auth, motherduck_auth, soql_to_ddl, run_sf_query

soql = """
select AccountId,Adjustment_Effective_Date__c,AlphaFlash_rev_cc__c,AlphaFlash_rev__c,Billing_Schedule_Cancelled__c,Billing_Status__c,Cancellation_Amount__c,Cancellation_Effective_Date__c,Cancellation_Processed_Date__c,Cancellation_Reason_Notes__c,Cancellation_Reason__c,Cancellation_Type__c,Cancelled_in_System__c,Cancel_Amount_CC__c,Cancel_Owner__c,Cancel_Reason__c,CaseNumber,Chicago_PMI_rev_cc__c,Chicago_PMI_rev__c,Comments,Commodities_rev_cc__c,Connect_rev_cc__c,Connect_Rev__c,Contract__c,CPI_Subcomponent_rev_cc__c,CPI_Subcomponent_rev__c,CreatedById,CreatedDate,Credit_revs_cc__c,CurrencyIsoCode,Data_Revs_cc__c,Description,DM_rev_cc__c,DM_rev__c,EM_Credit_rev_cc__c,EM_Credit_rev__c,EM_Policy_rev_cc__c,EM_Policy_rev__c,EM_rev_cc__c,EM_rev__c,EU_Credit_rev_cc__c,EU_Credit_rev__c,Global_Macro_rev_cc__c,Global_Macro_rev__c,HSC_rev_cc__c,HSC_rev__c,Id,IsClosed,IsDeleted,LastModifiedById,Macro_rev_cc__c,Macro_rev__c,Oil_Gas_rev_cc__c,Oil_Gas_rev__c,Other_Data_rev_cc__c,Other_Data_rev__c,Other_rev_cc__c,Other_rev__c,OwnerId,Policy_rev_cc__c,Policy_rev__c,PowerBI_Exclude__c,Power_rev_cc__c,Power_rev__c,Precautionary_Cancellation__c,Product_rev_Cancellation_amount__c,RecordTypeId,Recurring_Billing__c,Sales_Ops_Status__c,Status,Status_Sales__c,Subject,SystemModstamp,Type,Users_on_Assets__c,User_End_Date_Specified__c,US_Credit_rev_cc__c,US_Credit_rev__c,US_Oil_Gas_rev_cc__c,US_Oil_Gas_rev__c
from case
where account.parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
and account.recordtype.name <> 'Vendor'
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