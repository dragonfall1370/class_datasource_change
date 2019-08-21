--Do Not Call
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
DoNotCall as 'CustomValue',
'Do Not Call' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate'

--Availability Date
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Availability_Date__c as 'CustomValue',
'Availability Date' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Availability_Date__c <> ''

--Candidate Registration Sent
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Candidate_Registration_Sent__c as 'CustomValue',
'Candidate Registration Sent' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Candidate_Registration_Sent__c <> ''

--Candidate Source ( Core Field )
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
b.["Name"] as 'CustomValue',
'Candidate Source' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
left join ts2__Source__c b on ts2__Candidate_Source__c = b."""Id""" ---------
where name='Candidate' and ts2__Candidate_Source__c <> ''

--Clear Criminal Record
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Clear_Criminal_Record__c as 'CustomValue',
'Clear Criminal Record' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Clear_Criminal_Record__c <> ''

--Companies Not Of Interest
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Companies_Not_Of_Interest__c as 'CustomValue',
'Companies Not Of Interest' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Companies_Not_Of_Interest__c <> ''

--Companies Of Interest
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Companies_Of_Interest__c as 'CustomValue',
'Companies Of Interest' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Companies_Of_Interest__c <> ''

--Contract
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Contract__c as 'CustomValue',
'Contract' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Contract__c <> ''

--Current Benefits
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Current_Benefits__c as 'CustomValue',
'Current Benefits' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Current_Benefits__c <> ''

--Current Employer ( Core Fields )
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Current_Employer__c as 'CustomValue',
'Current Employer' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Current_Employer__c <> ''

--Current Salary ( Core Fields )
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
ts2__Current_Salary__c as 'CustomValue',
'Current Salary' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and ts2__Current_Salary__c <> ''

--Degree Subject
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Degree_Subject__c as 'CustomValue',
'Degree Subject' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Degree_Subject__c <> ''

--Desired Benefits
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Desired_Benefits__c as 'CustomValue',
'Desired Benefits' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Desired_Benefits__c <> ''

--Desired Salary
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
ts2__Desired_Salary__c as 'CustomValue',
'Desired Salary' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and ts2__Desired_Salary__c <> ''

--Driving Licence
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Driving_License__c as 'CustomValue',
'Driving Licence' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Driving_License__c <> ''

--Financial History
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Financial_History__c as 'CustomValue',
'Financial History' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Financial_History__c <> ''

--Good Health
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Good_Health__c as 'CustomValue',
'Good Health' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Good_Health__c <> ''


--Graduation Year
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Graduation_Year__c as 'CustomValue',
'Graduation Year' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Graduation_Year__c <> ''

--ID Confirmed
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
ID_Confirmed__c as 'CustomValue',
'ID Confirmed' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and ID_Confirmed__c <> ''


--Key Priority 1
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Key_Priority_1__c as 'CustomValue',
'Key Priority 1' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Key_Priority_1__c <> ''

--Key Priority 2
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Key_Priority_2__c as 'CustomValue',
'Key Priority 2' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Key_Priority_2__c <> ''

--Key Priority 3
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Key_Priority_3__c as 'CustomValue',
'Key Priority 3' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Key_Priority_3__c <> ''

--Notice Period
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Notice_Period__c as 'CustomValue',
'Notice Period' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Notice_Period__c <> ''

--Other Agencies
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Other_Agencies__c as 'CustomValue',
'Other Agencies' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Other_Agencies__c <> ''

--Other I/Vs & Applications
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Other_i_vs_applications__c as 'CustomValue',
'Other I/Vs & Applications' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Other_i_vs_applications__c <> ''

--Permanent
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
iif(Permanent__c = 1,0,2) as 'CustomValue',
'Permanent' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Permanent__c <> ''

--Predicted/Actual Grade
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Predicted_Actual_Grade__c as 'CustomValue',
'Predicted/Actual Grade' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Predicted_Actual_Grade__c <> ''

--Reason for looking
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Reason_for_looking__c as 'CustomValue',
'Reason for looking' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Reason_for_looking__c <> ''

--Referrals
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Referrals__c as 'CustomValue',
'Referrals' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Referrals__c <> ''

--Relocation
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Relocation__c as 'CustomValue',
'Relocation' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Relocation__c <> ''

--Student
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Student__c as 'CustomValue',
'Student' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Student__c <> ''

--Time Looking
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Time_Looking__c as 'CustomValue',
'Time Looking' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Time_Looking__c <> ''

--Travel - Flexibility
select 'add_cand_info' as 'Additional_type',
contact.Id as 'External_id', 
Travel_Flexibility__c as 'CustomValue',
'Travel - Flexibility' as 'lookup_name',
getdate() as insert_timestamp
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
where name='Candidate' and Travel_Flexibility__c <> ''



--Nationality (core fields)
select Nationality__c, 

case when (b.nationality = Nationality__c) then b.alpha_2_code
when Nationality__c = 'British' then 'GB'
when Nationality__c = 'Dutch' then 'NL'
when Nationality__c = 'Ecuadorean' then 'EC'
when Nationality__c = 'Filipino' then 'PH'
when Nationality__c = 'Greek' then 'GR'
when Nationality__c = 'Hungarian' then 'HU'
when Nationality__c = 'Iranian' then 'IR'
when Nationality__c = 'Nepalese' then 'NP'
when Nationality__c = 'Scottish' then 'GB'
when Nationality__c = 'Slovakian' then 'SK'
when Nationality__c = 'New Zealander' then 'NZ'
when Nationality__c = 'Taiwanese' then 'TW'
when Nationality__c = 'Welsh' then 'GB'
when Nationality__c = 'Yemenite' then 'YE'
else '' end as 'countrycode' 
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
left join countries b on contact.Nationality__c = b.nationality
where name='Candidate' and Nationality__c <> ''



