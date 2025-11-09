from dotenv import load_dotenv
from query_helpers import salesforce_auth, motherduck_auth, incremental_refresh_table

tables = {
    "content__c":
    """
    SELECT Id, SystemModstamp, IsDeleted, LastModifiedById, Account__c, Contact__c, Opportunity__c, Product__c, Case__c, Recurring_Billing_Line__c, Account__r.Name, Contact__r.Name, CreatedDate, CreatedById, Start_Date__c, End_Date__c, Type__c, Active__c, CurrencyIsoCode, Amount__c, Amount_CC__c, Product_Name_text__c, Imputed_conversion_date__c, Original_Converted_Amount_CC__c, Converted_from_a_Trial__c, Lead_Source_New__c, True_Commods_Trial__c, True_Credit_Trial__c, Role_Type_at_Trial_Creation_Date__c, Created_from_weblead__c
    FROM content__c
    WHERE Account__r.Parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
    AND Account__r.RecordType.Name <> 'Vendor'
    AND SystemModstamp >= {lower_bound}
    ORDER BY SystemModstamp DESC
    """,

    "account":
    """
    select Id, SystemModstamp, IsDeleted, LastModifiedById, ParentId, Parent_Account__c, Name, Parent_Account_Consolidated__c, CreatedDate, Region__c, ShippingCountry, Buy_Sell_Side__c, Buy_Side_Type__c, Customer_Type__c, Active_People_Subs__c, Active_Trials__c, Ex_Subscribers__c, Ex_Trialists__c, Subscription_Amount_cc__c, Amount_Cancelling_cc__c, Precautionary_Cancel_cc__c, Net_Cancel_cc__c, Cancel_Type__c, CreatedById, CreatedBy.Name, OwnerId, Owner.Name, Pure_Commods_Account__c
    from account
    where Parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
    and recordtype.name <> 'Vendor'
    AND SystemModstamp >= {lower_bound}
    ORDER BY SystemModstamp DESC
    """,

    "contact":
    """
    select Id, SystemModstamp, IsDeleted, LastModifiedById, AccountId, Auth_0_Id__c, CreatedDate, CreatedById, CreatedBy.name, Name, Email, Job_Title__c, Primary_Product_Target__c, Subscription_Status__c, Active_Contents_Current_Employer__c, All_Active_Trials__c, All_Inactive_Contents__c, Inactive_Trials__c, Last_Subscribed_Content_End_Date__c, Last_Content_End_Date__c, Contents_at_risk__c, Last_Rep_Outreach_Date__c, Last_SDR_Touchpoint_Date__c, HasOptedOutOfEmail, DO_NOT_CONTACT__c
    from contact
    where account.Parent_account_consolidated__c <> 'TOP GLOBAL - MARKET NEWS (INTERNAL)'
    and account.recordtype.name <> 'Vendor'
    AND SystemModstamp >= {lower_bound}
    ORDER BY SystemModstamp DESC
    """,

    "user":
    """
    select Id, SystemModstamp, LastModifiedById, Name, Email, IsActive, Promotion_Date__c, UserRoleId, UserRole.Name, Role_Team__c, Role_Type__c, ProfileId, Profile.Name
    from User
    WHERE SystemModstamp >= {lower_bound}
    ORDER BY SystemModstamp DESC
    """,
    "userrole":
    """
    SELECT Id,SystemModstamp,LastModifiedById,Name,ParentRoleId
    FROM UserRole
    WHERE SystemModstamp >= {lower_bound}
    ORDER BY SystemModstamp DESC
    """,
    "product2":
    """
    SELECT Id,SystemModstamp,LastModifiedById,Activate_by_Default__c,Billable__c,Bloomberg_Sub_Channel__c,CreatedById,CreatedDate,CurrencyIsoCode,Description,Distribution_Channel__c,Family,IsActive,IsArchived,IsDeleted,LastModifiedDate,Main_Package__c,Name,Name_for_Rollups__c,ParentProduct__c,ProductCode,Revenue_Stream__c,Select_By_Default_From_Parent__c,Types__c,Website_Offering__c
    FROM Product2
    WHERE SystemModstamp >= {lower_bound}
    ORDER BY SystemModstamp DESC
    """
}

load_dotenv()

sf = salesforce_auth()
con = motherduck_auth()

for table, soql in tables.items():
    incremental_refresh_table(sf, con, table=table, soql_template=soql)