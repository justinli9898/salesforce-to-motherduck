from dotenv import load_dotenv
from query_helpers import salesforce_auth, motherduck_auth, incremental_refresh_table

tables = {
    "content__c":
    """
    select Id, SystemModstamp, IsDeleted, LastModifiedById, Account__c, Contact__c, Opportunity__c, Product__c, Case__c, Recurring_Billing_Line__c, Account__r.Name, Contact__r.Name, CreatedDate, CreatedById, Start_Date__c, End_Date__c, Type__c, Active__c, CurrencyIsoCode, Amount__c, Amount_CC__c, Product_Name_text__c, Imputed_conversion_date__c, Original_Converted_Amount_CC__c, Converted_from_a_Trial__c, Lead_Source_New__c, True_Commods_Trial__c, True_Credit_Trial__c, Role_Type_at_Trial_Creation_Date__c, Created_from_weblead__c
    from content__c
    where Account__r.Parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
    and Account__r.RecordType.Name <> 'Vendor'
    and SystemModstamp >= {lower_bound}
    order by SystemModstamp DESC
    """,

    "account":
    """
    select Id, SystemModstamp, IsDeleted, LastModifiedById, ParentId, Parent_Account__c, Name, Parent_Account_Consolidated__c, CreatedDate, Region__c, ShippingCountry, Buy_Sell_Side__c, Buy_Side_Type__c, Customer_Type__c, Active_People_Subs__c, Active_Trials__c, Ex_Subscribers__c, Ex_Trialists__c, Subscription_Amount_cc__c, Amount_Cancelling_cc__c, Precautionary_Cancel_cc__c, Net_Cancel_cc__c, Cancel_Type__c, CreatedById, CreatedBy.Name, OwnerId, Owner.Name, Pure_Commods_Account__c
    from account
    where Parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
    and recordtype.name <> 'Vendor'
    and SystemModstamp >= {lower_bound}
    order by SystemModstamp DESC
    """,

    "contact":
    """
    select Id, SystemModstamp, IsDeleted, LastModifiedById, AccountId, Auth_0_Id__c, CreatedDate, CreatedById, CreatedBy.name, Name, Email, Job_Title__c, Primary_Product_Target__c, Subscription_Status__c, Active_Contents_Current_Employer__c, All_Active_Trials__c, All_Inactive_Contents__c, Inactive_Trials__c, Last_Subscribed_Content_End_Date__c, Last_Content_End_Date__c, Contents_at_risk__c, Last_Rep_Outreach_Date__c, Last_SDR_Touchpoint_Date__c, HasOptedOutOfEmail, DO_NOT_CONTACT__c
    from contact
    where account.Parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
    and account.recordtype.name <> 'Vendor'
    and SystemModstamp >= {lower_bound}
    order by SystemModstamp DESC
    """,

    "user":
    """
    select Id, SystemModstamp, LastModifiedById, Name, Email, IsActive, Promotion_Date__c, UserRoleId, UserRole.Name, Role_Team__c, Role_Type__c, ProfileId, Profile.Name
    from User
    where SystemModstamp >= {lower_bound}
    order by SystemModstamp DESC
    """,

    "userrole":
    """
    select Id,SystemModstamp,LastModifiedById,Name,ParentRoleId
    from UserRole
    where SystemModstamp >= {lower_bound}
    order by SystemModstamp desc
    """,

    "product2":
    """
    select Id,SystemModstamp,LastModifiedById,Activate_by_Default__c,Billable__c,Bloomberg_Sub_Channel__c,CreatedById,CreatedDate,CurrencyIsoCode,Description,Distribution_Channel__c,Family,IsActive,IsArchived,IsDeleted,LastModifiedDate,Main_Package__c,Name,Name_for_Rollups__c,ParentProduct__c,ProductCode,Revenue_Stream__c,Select_By_Default_From_Parent__c,Types__c,Website_Offering__c
    from Product2
    where SystemModstamp >= {lower_bound}
    order by SystemModstamp desc
    """,

    "opportunity":
    """
    select AccountId, AlphaFlash_rev_cc__c, AlphaFlash_rev__c, Amount, Amount_Lockdown__c, Buy_Sell_Side__c, Chicago_PMI_rev_cc__c, Chicago_PMI_rev__c, CloseDate, Close_Date_Lockdown__c, Comment__c, Commodities_rev_cc__c, Conga_Addendum_TemplateID__c, Conga_HSC_Addendum__c, Conga_MSA_TemplateID__c, Connect_Pipeline__c, Connect_rev_cc__c, Connect_Rev__c, CPI_Subcomponent_rev_cc__c, CPI_Subcomponent_rev__c, CreatedById, CreatedDate, Created_Closed_Same_Day__c, Credit_PipelineWeighted__c, Credit_Pipeline__c, Credit_rev_cc__c, Credit_rev__c, CurrencyIsoCode, Data_rev_cc__c, Date_Budget_Approved__c, Date_Good_Feedback__c, Date_Negotiation__c, Date_of_Original_Agreement__c, Date_Proposal_Sent__c, Days_Since_Budget_Approved_90__c, Days_Since_Good_Feedback_25__c, Days_Since_Negotiation_75__c, Days_Since_Proposal_Sent_50__c, Days_Since_Trial_Created__c, Days_until_90_day_deadline__c, Days_until_Trial_Deadline__c, Description, Desk_License__c, DM_rev_cc__c, DM_rev__c, Edited_Using_Flow__c, EM_Credit_pipeline__c, EM_Credit_rev_cc__c, EM_Credit_rev__c, EM_Policy_Pipeline__c, EM_Policy_rev_cc__c, EM_Policy_rev__c, EM_rev_cc__c, EM_rev__c, EU_Credit_rev_cc__c, EU_Credit_rev__c, Finance_Notes__c, Forecasted_Close_Date__c, Global_Macro_rev_cc__c, Global_Macro_rev__c, Highest_Probability_Reached__c, Hold_off_on_Billing__c, HSC_Pipeline__c, HSC_rev_cc__c, HSC_rev__c, Id, IsClosed, IsDeleted, IsSplit, Issuance_Data_Pipeline__c, Issuance_Data_Sent__c, IsWon, Key_Deal__c, LastActivityDate, LastAmountChangedHistoryId, LastCloseDateChangedHistoryId, LastModifiedById, LastModifiedDate, LastStageChangeDate, LastViewedDate, Last_Activity_Assignee__c, Last_Activity_Date__c, Last_Activity_Type__c, LeadSource, Likelihood_to_Close__c, Loss_Reason_Description__c, Loss_Reason__c, Macro_Pipeline__c, Macro_rev_cc__c, Macro_Rev__c, Max_Trial_End_Roll_Up_Helper__c, MEDDICC_Metrics_Notes__c, MEDDIC_Champion_Notes__c, MEDDIC_Decision_Criteria_Notes__c, MEDDIC_Decision_Process_Notes__c, MEDDIC_Economic_Buyer_Notes__c, MEDDIC_Identified_Pain_Value_Notes__c, Name, NB_Closing_Window_Override__c, NB_Closing_Window__c, NB_Trial_Window_Override__c, NB_Trial_Window__c, NB_Window_End_Date_Group__c, New_Upgrade_Amount__c, NextStep, Non_Renewal_Auto_Roll_30__c, Not_Standard_Terms__c, Oil_Gas_PipelineWeighted__c, Oil_gas_rev_cc__c, Oil_gas_rev__c, Oil_x_Gas_Pipeline__c, Other_Data_rev_cc__c, Other_Data_rev__c, Other_rev_cc__c, Other_rev__c, OwnerId, Policy_rev_cc__c, Policy_rev__c, Power_Pipeline__c, Power_rev_cc__c, Power_rev__c, Price_Increase_Amount_CC__c, Price_Increase_Amount__c, Price_Increase_Opp__c, Probability, Probability_Read_Only__c, Product_rev_Amount__c, Prod_rev_allocation_used__c, Prospected_by__c, RecordTypeId, Recurring_Billing_Frequency__c, Recurring_Billing_Next_Bill_Date__c, Recurring_Billing_r__c, Region__c, Rejected_Reason_Type__c, Rejection_Notes__c, Renewal_is_Automatic__c, Renewal_Period_in_Months__c, Renewal_Period_in_Years__c, Rep_Closed_Amount_CC__c, Rep_Closed_Price_Increase_Amount_CC__c, Rep_Closed_Price_Increase__c, RFB_Waiting_on_Sales__c, Sales_Ops_Price_Increase_Amount_cc__c, SDR_Commission_Rollover__c, StageName, Stage__c, Start_Date_of_Additional_Services__c, SystemModstamp, Termination_Language_Template__c, Termination_Notice_Period_Days__c, Termination__c, Term_Co_Term_Until__c, Term_Type__c, Total_New_Upgrade_Amount_CC__c, Trial_Stage__c, Type, Upgrade_Type__c, US_Credit_Pipeline__c, US_Credit_rev_cc__c, US_Credit_rev__c, US_Oil_Gas_Pipeline__c, US_Oil_Gas_rev_cc__c, US_Oil_Gas_rev__c, Web_User_Type__c, Weighted_Amount_cc__c, Weighted_Amount__c, X1st_Stage_Feedback_Call_Booked_Timesta__c, X1st_Stage_Feedback_Call_Complete_Times__c, X2nd_Stage_Feedback_Call_Booked_Timesta__c, X2nd_Stage_Feedback_Call_Complete_Times__c
    from Opportunity
    where account.Parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
    and account.recordtype.name <> 'Vendor'
    and SystemModstamp >= {lower_bound}
    order by SystemModstamp desc
    """,

    "opportunitycontactrole":
    """
    select Id,SystemModstamp,IsDeleted,LastModifiedById,ContactId,CreatedById,CreatedDate,Ex_Trialist_Rating__c,IsPrimary,OpportunityId,Role
    from opportunitycontactrole
    where Opportunity.Account.Parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
    and Opportunity.account.recordtype.name <> 'Vendor'
    and SystemModstamp >= {lower_bound}
    order by SystemModstamp desc
    """,

    "contract":
    """
    select Id, SystemModstamp, IsDeleted, LastModifiedById, AccountId, Active_Account_Lookup__c, Anniversary_Date__c, Auto_Renew__c, Base_MSA__c, Cancellation_Terms_days__c, ContractNumber, ContractTerm, Contractual_MRR_cc__c, Contract_End_Date__c, Contract_Status__c, Contract_Type__c, CreatedById, CreatedDate, Current_Contract_Renewed__c, Description, EndDate, Exit_2__c, Exit__c, Expiry_Date__c, Original_MSA_Commencement_Date__c, Price_Increase_Notice_Period__c, RecordTypeId, Roll_Date__c, SpecialTerms, StartDate, Subcontract_Type__c
    from contract
    where SystemModstamp >= {lower_bound}
    order by SystemModstamp desc 
    """,

    "contract_account_junction__c":
    """
    select Id,SystemModstamp,IsDeleted,LastModifiedById,Account__c,Contractual_Relationship__c,Contract__c,CreatedById,CreatedDate,Name
    from contract_account_junction__c
    where SystemModstamp >= {lower_bound}
    order by SystemModstamp desc     
    """
}

load_dotenv()

sf = salesforce_auth()
con = motherduck_auth()

for table, soql in tables.items():
    incremental_refresh_table(sf, con, table=table, soql_template=soql)