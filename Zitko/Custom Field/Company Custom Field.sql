--CreatedBy
select a.Id, b.Username from Account a left join [User] b on a.CreatedById = b.Id

--Parent Company
select distinct a.Id,a.Name from Account a, Account b where a.ID = b.ParentId

--Company Registration Number
select Id, Company_Registration_Number__c from Account where Company_Registration_Number__c <> ''

----------------------------------------------------------------------------
--Annual Revenue
select 'add_com_info' as 'Additional_type',
Id as 'External_id',
AnnualRevenue as 'CustomValue',
'Annual Revenue' as 'lookup_name',
getdate() as insert_timestamp
from Account where AnnualRevenue <> ''


--Business Focus
select 'add_com_info' as 'Additional_type',
Id as 'External_id',
Business_Focus__c as 'CustomValue',
'Business Focus' as 'lookup_name',
getdate() as insert_timestamp
from Account where Business_Focus__c <> ''

--Company Benefits
select 'add_com_info' as 'Additional_type',
Id as 'External_id',
Company_Benefits__c as 'CustomValue',
'Company Benefits' as 'lookup_name',
getdate() as insert_timestamp
from Account where Company_Benefits__c <> ''


--Current Agencies
select 'add_com_info' as 'Additional_type',
Id as 'External_id',
Current_Agencies__c as 'CustomValue',
'Current Agencies' as 'lookup_name',
getdate() as insert_timestamp
from Account where Current_Agencies__c <> ''

--Do Not Contact
select 'add_com_info' as 'Additional_type',
Id as 'External_id',
Do_Not_Contact__c as 'CustomValue',
'Do Not Contact' as 'lookup_name',
getdate() as insert_timestamp
from Account

--Key Problems
select 'add_com_info' as 'Additional_type',
Id as 'External_id',
Key_Problems__c as 'CustomValue',
'Key Problems' as 'lookup_name',
getdate() as insert_timestamp
from Account where Key_Problems__c <> ''


--Number of Roles
select 'add_com_info' as 'Additional_type',
Id as 'External_id',
Number_of_Roles__c as 'CustomValue',
'Number of Roles' as 'lookup_name',
getdate() as insert_timestamp
from Account where Number_of_Roles__c <> ''

--Product
select 'add_com_info' as 'Additional_type',
Id as 'External_id',
Product__c as 'CustomValue',
'Product' as 'lookup_name',
getdate() as insert_timestamp
from Account


--Sector
select 'add_com_info' as 'Additional_type',
Id as 'External_id',
Sector__c as 'CustomValue',
'Sector' as 'lookup_name',
getdate() as insert_timestamp
from Account


--Typical Roles
select 'add_com_info' as 'Additional_type',
Id as 'External_id',
Typical_Roles__c as 'CustomValue',
'Typical Roles' as 'lookup_name',
getdate() as insert_timestamp
from Account where Typical_Roles__c <> ''