with 
loc as (
	select Unique_ID, Address_Hse_No,Address_Line_1,Address_Line_2,Address_Line_3
			,Address_Line_4,Address_Line_5,Address_Line_6,PostCode
			, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(Address_Hse_No, ''), '')
			+ Coalesce(', ' + NULLIF(Address_Line_1, ''), '')
			+ Coalesce(', ' + NULLIF(Address_Line_2, ''), '')
			+ Coalesce(', ' + NULLIF(Address_Line_3, ''), '')
			+ Coalesce(', ' + NULLIF(Address_Line_4, ''), '')
			+ Coalesce(', ' + NULLIF(Address_Line_5, ''), '')
			+ Coalesce(', ' + NULLIF(Address_Line_6, ''), '')
			+ Coalesce(', ' + NULLIF(PostCode, ''), '')
			, 1, 1, '')) as 'locationName'
	from Candidates)

-------------------------------use this to link the candidate with contact
,  dupContacts as (select *
, ROW_NUMBER() OVER(PARTITION BY Contact_Unique_ID ORDER BY Site_Unique ASC) AS rn
from ContactManagementContacts)
--select Contact_Unique_ID,Site_Unique,rn from dupContacts where rn>1 order by Contact_Unique_ID

, Contact as (
select *
, iif(rn=1,Contact_Unique_ID,concat(Contact_Unique_ID,'_',Site_Unique))as contactID
from dupContacts)

--CANDIDATE DUPLICATE MAIL REGCONITION
, tempEmail as (select Unique_ID, Email,Email_Address_2, coalesce(nullif(Email,''),Email_Address_2) as Email1 from candidates
where Email <> '' or Email_Address_2 <> '')

, tempEmail1 as (select Unique_ID,
iif(CHARINDEX(';',Email1)<>0,left(Email1,CHARINDEX(';',Email1)-1),Email1) as Email
from tempEmail)

--check email format
, EmailDupRegconition as (SELECT Unique_ID,Email,
 ROW_NUMBER() OVER(PARTITION BY Email ORDER BY Unique_ID ASC) AS rn 
from tempEmail1
where Email like '%_@_%.__%')

--edit duplicating emails
, CandidateEmail as (select Unique_ID, 
case 
when rn=1 then Email
else concat('DUP',rn,'-',Email)
end as CandidateEmail
, rn
from EmailDupRegconition)
--select * from CandidateEmail where rn >1

----------------Users
, Users as (select concat(Users_Name,STMP_Account) as UserName, concat(iif(User_Email like '%@%',User_Email,''),Sync_Mailbox_Name) as UserEmail
from UserProfiles)

-------------------------------------------------------------MAIN SCRIPT
select concat('GP', c.Unique_ID) as 'candidate-externalId'
, iif(c.Forename = '' or c.Forename = '`',concat('NoFirstName-ID',c.Unique_ID), rtrim(ltrim(c.Forename))) as 'candidate-firstName'
, iif(c.Surname = '' or c.Surname = '0',concat('NoLastName-ID',c.Unique_ID), rtrim(ltrim(c.Surname))) as 'candidate-Lastname'
, rtrim(ltrim(loc.locationName)) as 'candidate-address'
--, cand_town as 'candidate-city'
--, cand_county as 'candidate-state'
, c.PostCode as 'candidate-zipCode'
, case 
		when loc.locationName like '%South Africa%' then 'ZA' 
		when loc.locationName like '%Gauteng%' then 'ZA'
		when loc.locationName like '%Cape%' then 'ZA'
		when loc.locationName like '%Durban%' then 'ZA'
		when loc.locationName like '%burg%' then 'ZA'
		when loc.locationName like '%UK%' then 'GB'
		when loc.locationName like '%London%' then 'GB'
		when loc.locationName like '%Cayman%' then 'KY'
		when loc.locationName like '%USA%' then 'US'
		else '' end as 'candidate-country'
, u.UserEmail as 'candidate-owners'
, upper(c.sex) as 'candidate-gender'
--, c.Date_of_Birth
--, iif(c.Date_of_Birth = ' ','',iif(CHARINDEX(':',c.Date_of_Birth)<>0,concat(substring(c.Date_of_Birth,9,2),'/',substring(c.Date_of_Birth,6,2),'/',left(c.Date_of_Birth,4)),concat(substring(c.Date_of_Birth,4,2),'/',left(c.Date_of_Birth,2),'/',right(c.Date_of_Birth,4)))) as 'candidate-dob'--mm/dd/yyyy
, iif(c.Date_of_Birth = ' ' or c.Date_of_Birth like '%2019%','',iif(CHARINDEX(':',c.Date_of_Birth)<>0,concat(left(c.Date_of_Birth,4),'-',substring(c.Date_of_Birth,9,2),'-',substring(c.Date_of_Birth,6,2)),concat(right(c.Date_of_Birth,4),'-',substring(c.Date_of_Birth,4,2),'-',left(c.Date_of_Birth,2)))) as 'candidate-dob'--yyyy-mm-dd
, case c.Title
		when 'Sir' then 'MR' 
		when 'Mrs' then 'MRS'
		when 'Miss' then 'MISS'
		when 'Ms' then 'MS'
		when 'Mr' then 'MR'
		else '' end as 'candidate-title'
, case c.Nationality
		when 'Greece' then 'GR' 
		when 'Hungary' then 'HU'
		when 'Pakistan' then 'PK'
		when 'Russia' then 'RU'
		when 'South Africa' then 'ZA'
		when 'Zimbabwe' then 'ZW'
		else '' end as 'candidate-citizenship'
--, case Currency
--				when 'Euros' then 'EUR'
--				when 'Rand' then 'ZAR'
--				when 'US Dollars' then 'USD'
--				else 'GBP' end as 'candidate-currency'
, 'THB' as 'candidate-currency'
, nullif(Basic_Salary,0) as 'candidate-currentSalary'
, iif(c.job_description in ('+',' ','0') or c.job_description like '%£%','',c.job_description) as test
, iif(c.job_description in ('+',' ','0') or c.job_description like '%£%',iif(job_code_001 = '','',job_code_001),c.job_description) as 'candidate-jobTitle1'
, iif(c.Current_Employer in ('0','',' '),'',c.Current_Employer) as 'candidate-Employer1'
, coalesce(nullif(c.Telephone,' '), nullif(c.Mobile,' '), nullif(c.Work_Telephone,' ')) as 'candidate-phone'
, Stuff(Coalesce(' ' + NULLIF(c.Work_Telephone, ''), '')
      + Coalesce(',' + NULLIF(c.Work_Telephone,NULLIF(c.DDI,'')), '')
      , 1, 1, '') as 'candidate-workPhone'
--, c.Work_Telephone as 'candidate-workPhone'
, c.Mobile as 'candidate-mobile'
, Stuff(Coalesce(' ' + NULLIF(cast(c.Documents_Names_001 as varchar(max)), ''), '')
      + Coalesce(',' + NULLIF(cast(c.Documents_Names_002 as varchar(max)), ''), '')
      + Coalesce(',' + NULLIF(cast(c.Documents_Names_003 as varchar(max)), ''), '')
      + Coalesce(',' + NULLIF(cast(c.Documents_Names_004 as varchar(max)), ''), '')
      , 1, 1, '') as 'candidate-resume'
, Digital_ID_Filename as 'candidate-photo'
, case
	when c.Email is not NULL and ce.Unique_ID is not null then ce.CandidateEmail
	else concat('CandidateID-',c.Unique_ID,'@noemail.com') end as 'candidate-email'
, iif(c.Social_Net_Site_1 like '%linkedin.com%',c.Social_Net_Site_1,'') as 'candidate-linkedin'
--, c.SecondaryEmail as 'candidate-workEmail'
, left(concat('Candidate External ID: GP',c.Unique_ID, char(10)
	, iif(c.Email_Address_2 in ('',' ','0'),'',concat(char(10),'Email Address 2: ',c.Email_Address_2,char(10)))
	, iif(c.Email_Verified = '','',concat(char(10),'Email Verified? ',replace(c.Email_Verified,'Y','Yes'),char(10)))
	, iif(c.Marital_Status = '','',concat(char(10),'Marital Status: ',c.Marital_Status,char(10)))
	, iif(c.Fax = '','',concat(char(10),'Fax: ',c.Fax,char(10)))
	, iif(c.Job_Code_001 = '','',concat(char(10),'Job Code: ',c.Job_Code_001,char(10)))
	, iif(c.Position_Code = '','',concat(char(10),'Position Code: ',c.Position_Code,char(10)))
	, iif(Area_Code_001 = '' and Area_Code_002 = '' and Area_Code_003 = '' and Area_Code_004 = '',''
		,concat(char(10),'Area Code: ',ltrim(Stuff(
			Coalesce(' ' + NULLIF(Area_Code_001, ''), '')
			+ Coalesce(', ' + NULLIF(Area_Code_002, ''), '')
			+ Coalesce(', ' + NULLIF(Area_Code_003, ''), '')
			+ Coalesce(', ' + NULLIF(Area_Code_003, ''), '')
			, 1, 1, '')),char(10)))
	, iif(c.Availability_Type = '','',concat(char(10),'Availability Type: ',c.Availability_Type,char(10)))
	, iif(c.Bank_Acc_Name = '','',concat(char(10),'Bank Account Name: ',c.Bank_Acc_Name,char(10)))
	, iif(c.Business_Area = '','',concat(char(10),'Business Area: ',c.Business_Area,char(10)))
	, iif(c.Passport_Type = '','',concat(char(10),'Passport Type: ',c.Passport_Type,char(10)))
	, iif(c.Social_Net_Site_1 = '','',concat(char(10),'Social Net Site: ',c.Social_Net_Site_1,char(10)))
	, iif(c.On_Target_Earnings in ('',0),'',concat(char(10),'On Target Earnings: ',c.On_Target_Earnings,char(10)))
	, iif(c.Reference = '','',concat(char(10),'Reference: ',c.Reference,char(10)))
	, iif(CHARINDEX(':',c.Professional_Expiry)<>0,concat(char(10), 'Professional Expiry: ', substring(c.Professional_Expiry,6,2),'/',substring(c.Professional_Expiry,9,2),'/',left(c.Professional_Expiry,4),char(10)),iif(c.Professional_Expiry = '','',concat(char(10), 'Professional Expiry: ',c.Professional_Expiry,char(10))))
	, iif(c.CV_Last_Sent_By_001 = '','',concat(char(10),'CV Last Sent By: ',c.CV_Last_Sent_By_001,char(10)))
	, iif(CHARINDEX(':',c.CV_Last_Sent_Date_001)<>0,concat(char(10), 'CV Last Sent Date: ', substring(c.CV_Last_Sent_Date_001,6,2),'/',substring(c.CV_Last_Sent_Date_001,9,2),'/',left(c.CV_Last_Sent_Date_001,4),char(10)),iif(c.CV_Last_Sent_Date_001 = '','',concat(char(10), 'CV Last Sent Date: ',c.CV_Last_Sent_Date_001,char(10))))
	, iif(c.CV_Updated_By = '','',concat(char(10),'CV Updated By: ',c.CV_Updated_By,char(10)))
	, iif(CHARINDEX(':',c.CV_Updated_Date)<>0,concat(char(10), 'CV Updated Date: ', substring(c.CV_Updated_Date,6,2),'/',substring(c.CV_Updated_Date,9,2),'/',left(c.CV_Updated_Date,4),char(10)),iif(c.CV_Updated_Date = '','',concat(char(10), 'CV Updated Date: ',c.CV_Updated_Date,char(10))))
	, iif(c.Safe_CV_Gen_By_001 = '','',concat(char(10),'Safe CV Gen By: ',c.Safe_CV_Gen_By_001,char(10)))
	, iif(CHARINDEX(':',c.Safe_CV_Gen_Date_001)<>0,concat(char(10), 'Safe_CV_Gen_Date_001: ', substring(c.Safe_CV_Gen_Date_001,6,2),'/',substring(c.Safe_CV_Gen_Date_001,9,2),'/',left(c.Safe_CV_Gen_Date_001,4),char(10)),iif(c.Safe_CV_Gen_Date_001 = '','',concat(char(10), 'Safe CV Gen Date: ',c.Safe_CV_Gen_Date_001,char(10))))
	, iif(c.Contact_Uniq_ID = '' or c.Contact_Uniq_ID is null,'',concat(char(10),'Link to Contact: ',concat(cc.forename, ' ',cc.surname),char(10)))
	, iif(c.Contract_Rate_Type = '','',concat(char(10),'Contract Rate Type: ',c.Contract_Rate_Type,char(10)))
	, iif(c.Contract_Work_YN = '','',concat(char(10),'Contract Work?: ',replace(c.Contract_Work_YN,'N','No'),char(10)))
	, iif(c.Perm_Work_YN = '','',concat(char(10),'Permanent Work?: ',replace(replace(c.Perm_Work_YN,'N','No'),'Y','Yes'),char(10)))
	, iif(c.Enquiry_Source = '','',concat(char(10),'Enquiry Source: ',c.Enquiry_Source,char(10)))
	, iif(c.Ethnic_Origin = '','',concat(char(10),'Ethnic Origin: ',c.Ethnic_Origin,char(10)))
	, iif(c.Extension = '','',concat(char(10),'Extension: ',c.Extension,char(10)))
	, iif(c.Geog_Restriction = '','',concat(char(10),'Geog Restriction: ',c.Geog_Restriction,char(10)))
	, iif(c.Holiday_Allocation in ('',0),'',concat(char(10),'Holiday Allocation: ',c.Holiday_Allocation,char(10)))
	, iif(c.Importance_in_Org = '','',concat(char(10),'Importance in Org: ',c.Importance_in_Org,char(10)))
	, iif(c.Initials in ('','.','-.'),'',concat(char(10),'Initials: ',c.Initials,char(10)))
	, iif(c.Registered in ('','.','-.'),'',concat(char(10),'Registered: ',replace(replace(c.Registered,'N','No'),'Y','Yes'),char(10)))
	, iif(CHARINDEX(':',c.Registered_Date)<>0,concat(char(10), 'Registered Date: ', substring(c.Registered_Date,6,2),'/',substring(c.Registered_Date,9,2),'/',left(c.Registered_Date,4),char(10)),iif(c.Registered_Date = '','',concat(char(10), 'Registered Date: ',c.Registered_Date,char(10))))
	, iif(CHARINDEX(':',c.Start_Date)<>0,concat(char(10), 'Start Date: ', substring(c.Start_Date,6,2),'/',substring(c.Start_Date,9,2),'/',left(c.Start_Date,4),char(10)),iif(c.Start_Date = '','',concat(char(10), 'Start Date: ',c.Start_Date,char(10))))
	, iif(c.Status = '','',concat(char(10),'Status: ',c.Status,char(10)))
	, iif(c.Disabled = '','',concat(char(10),'Disabled?: ',replace(replace(Disabled,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Main_Consultant = '' or c.Main_Consultant is null,'',concat(char(10),'Main Consultant: ',c.Main_Consultant,char(10)))
	, iif(c.Current_OTE in ('',0),'',concat(char(10),'Current OTE: ',convert(float,c.Current_OTE),char(10)))
	, iif(CHARINDEX(':',c.Dup_Added_Date)<>0,concat(char(10), 'Dup Added Date: ', substring(c.Dup_Added_Date,6,2),'/',substring(c.Dup_Added_Date,9,2),'/',left(c.Dup_Added_Date,4),char(10)),iif(c.Dup_Added_Date = '','',concat(char(10), 'Dup Added Date: ',c.Dup_Added_Date,char(10))))
	, iif(c.Creating_User = '','',Concat(char(10), 'Creating User: ', c.Creating_User, char(10)))
	, iif(CHARINDEX(':',c.Creation_Date)<>0,concat(char(10), 'Creation Date: ', substring(c.Creation_Date,6,2),'/',substring(c.Creation_Date,9,2),'/',left(c.Creation_Date,4),char(10)),concat(char(10), 'Creation Date: ',c.Creation_Date,char(10)))
	, iif(c.Amending_User = '','',Concat(char(10), 'Amending User: ', c.Amending_User, char(10)))
	, iif(CHARINDEX(':',c.Amendment_Date)<>0,concat(char(10), 'Amendment Date: ', substring(c.Amendment_Date,6,2),'/',substring(c.Amendment_Date,9,2),'/',left(c.Amendment_Date,4),char(10)),concat(char(10), 'Amendment Date: ',c.Amendment_Date,char(10)))
	, iif(c.Notice = '' or c.Notice = ' ','',Concat(char(10),'Notice: ',c.Notice,char(10)))
	, iif(c.SummaryText = '','',Concat(char(10), 'Summary Text: ',char(10), c.SummaryText, char(10)))
	, iif(c.CVText = '','',Concat(char(10), 'CV Text: ',char(10), c.CVText, char(10)))
	, iif(c.Cont_Usr_Chk_Box_001 = '','',concat(char(10),'Cont Usr Check Box 001: ',replace(replace(c.Cont_Usr_Chk_Box_001,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_002 = '','',concat(char(10),'Cont Usr Check Box 002: ',replace(replace(c.Cont_Usr_Chk_Box_002,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_003 = '','',concat(char(10),'Cont Usr Check Box 003: ',replace(replace(c.Cont_Usr_Chk_Box_003,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_004 = '','',concat(char(10),'Cont Usr Check Box 004: ',replace(replace(c.Cont_Usr_Chk_Box_004,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_005 = '','',concat(char(10),'Cont Usr Check Box 005: ',replace(replace(c.Cont_Usr_Chk_Box_005,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_006 = '','',concat(char(10),'Cont Usr Check Box 006: ',replace(replace(c.Cont_Usr_Chk_Box_006,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_007 = '','',concat(char(10),'Cont Usr Check Box 007: ',replace(replace(c.Cont_Usr_Chk_Box_007,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_008 = '','',concat(char(10),'Cont Usr Check Box 008: ',replace(replace(c.Cont_Usr_Chk_Box_008,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_009 = '','',concat(char(10),'Cont Usr Check Box 009: ',replace(replace(c.Cont_Usr_Chk_Box_009,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_010 = '','',concat(char(10),'Cont Usr Check Box 010: ',replace(replace(c.Cont_Usr_Chk_Box_010,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_011 = '','',concat(char(10),'Cont Usr Check Box 011: ',replace(replace(c.Cont_Usr_Chk_Box_011,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_012 = '','',concat(char(10),'Cont Usr Check Box 012: ',replace(replace(c.Cont_Usr_Chk_Box_012,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_013 = '','',concat(char(10),'Cont Usr Check Box 013: ',replace(replace(c.Cont_Usr_Chk_Box_013,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_014 = '','',concat(char(10),'Cont Usr Check Box 014: ',replace(replace(c.Cont_Usr_Chk_Box_014,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_015 = '','',concat(char(10),'Cont Usr Check Box 015: ',replace(replace(c.Cont_Usr_Chk_Box_015,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_016 = '','',concat(char(10),'Cont Usr Check Box 016: ',replace(replace(c.Cont_Usr_Chk_Box_016,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_017 = '','',concat(char(10),'Cont Usr Check Box 017: ',replace(replace(c.Cont_Usr_Chk_Box_017,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_018 = '','',concat(char(10),'Cont Usr Check Box 018: ',replace(replace(c.Cont_Usr_Chk_Box_018,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_019 = '','',concat(char(10),'Cont Usr Check Box 019: ',replace(replace(c.Cont_Usr_Chk_Box_019,'Y','Yes'),'N','No'),char(10)))
	, iif(c.Cont_Usr_Chk_Box_020 = '','',concat(char(10),'Cont Usr Check Box 020: ',replace(replace(c.Cont_Usr_Chk_Box_020,'Y','Yes'),'N','No')))
	),32000) as 'candidate-note'
from candidates c left join CandidateEmail ce on c.Unique_ID = ce.Unique_ID
				 left join loc on c.Unique_ID = loc.Unique_ID
				 left join Users u on c.Main_Consultant = u.UserName
				 left join Contact cc on c.Contact_Uniq_ID = cc.ContactID	 
--where c.unique_ID = 20944
--where Digital_ID_Filename like 'PIC15859.jpg'
order by convert(int,c.Unique_ID)