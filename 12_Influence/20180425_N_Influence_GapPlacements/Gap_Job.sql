with
 dupContacts as (select *
, ROW_NUMBER() OVER(PARTITION BY Contact_Unique_ID ORDER BY Site_Unique ASC) AS rn
from ContactManagementContacts)
--select Contact_Unique_ID,Site_Unique,rn from dupContacts where rn>1 order by Contact_Unique_ID

, Contact as (
select *
, iif(rn=1,Contact_Unique_ID,concat(Contact_Unique_ID,'_',Site_Unique))as contactID
from dupContacts)
--select * from contact

, ContactMaxID as (select 
case when Site_Unique is NULL then '9999999'
else Site_Unique end as CompanyID
, max(contactID) as ContactMaxID 
from Contact
group by Site_Unique)
--select * from ContactMaxID
----------------Users
, Users as (select concat(Users_Name,STMP_Account) as UserName, concat(iif(User_Email like '%@%',User_Email,''),Sync_Mailbox_Name) as UserEmail
from UserProfiles)

--DUPLICATION REGCONITION
, dup as (SELECT Unique_ID, Role_Description, ROW_NUMBER() OVER(PARTITION BY Role_Description ORDER BY Unique_ID ASC) AS rn 
from Vacancies)

--MAIN SCRIPT
select
case 
	when (j.Report_To_Unique_ID = '' or j.Report_To_Unique_ID = '0') and j.Site_Uniq_ID in (select CompanyID from ContactMaxID) then concat('GP',CM.ContactMaxID)
	when (j.Report_To_Unique_ID = '' or j.Report_To_Unique_ID = '0') and j.Site_Uniq_ID not in (select CompanyID from ContactMaxID) then 'GP9999999'
	when j.Report_To_Unique_ID is NULL and j.Site_Uniq_ID is NULL then 'GP9999999'
	else concat('GP',j.Report_To_Unique_ID) end as 'position-contactId'
--concat('GP',j.cont_ref) as 'position-contactId'
, j.Site_Uniq_ID as 'CompanyID'
, cms.Organisation
, concat('GP',j.Unique_ID) as 'position-externalId'
, j.Role_Description as 'position-title(old)'
, iif(j.Unique_ID in (select Unique_ID from dup where dup.rn > 1)
	, iif(dup.Role_Description = '' or dup.Role_Description is NULL,concat('No job title - ',dup.Unique_ID),concat(dup.Role_Description,'-',dup.Unique_ID))
	, iif(j.Role_Description = '' or j.Role_Description is null,concat('No job title - ',j.Unique_ID),j.Role_Description)) as 'position-title'
--, iif(CHARINDEX(':',job_recdate)<>0,concat(substring(job_recdate,6,2),'/',substring(job_recdate,9,2),'/',left(job_recdate,4)),job_recdate) as 'position-startDate1'--dd/mm/yyyy -> not suppoted to import
, iif(CHARINDEX(':',j.Creation_Date)<>0,concat(substring(j.Creation_Date,9,2),'/',substring(j.Creation_Date,6,2),'/',left(j.Creation_Date,4)),concat(substring(j.Creation_Date,4,2),'/',left(j.Creation_Date,2),'/',right(j.Creation_Date,4))) as 'position-startDate'--mm/dd/yyyy
, iif(Date_Placed = '','',iif(CHARINDEX(':',Date_Placed)<>0,concat(substring(Date_Placed,9,2),'/',substring(Date_Placed,6,2),'/',left(Date_Placed,4)),concat(substring(Date_Placed,4,2),'/',left(Date_Placed,2),'/',right(Date_Placed,4)))) as 'position-endDate'--mm/dd/yyyy
, u.UserEmail as 'position-owners'
, j.No_of_Positions as 'position-headcount'
, j.Documents_Names_001 as 'position-document'
--, case Currency
--				when 'Euros' then 'EUR'
--				when 'Rand' then 'ZAR'
--				when 'US Dollars' then 'USD'
--				else 'GBP' end as 'position-currency'
, 'THB' as 'position-currency'
--, job_salary as 'position-actualSalary'
, case
		when Vacancy_Type = 'Permanent Role' then 'PERMANENT'
		when Vacancy_Type = 'Temp' then 'TEMPORARY'
	else 'CONTRACT' end as 'position-type'
--, js.skill
, left(
	concat('Job External ID: GP',j.Unique_ID,char(10),char(10)
	, iif(Job_Code = '','', concat('Job Code: ',Job_Code,char(10),char(10)))
	, iif(j.Area_Code = '','', concat('Area Code: ',j.Area_Code,char(10),char(10)))
	, iif(Work_Location = '' or Work_Location is null,'', concat('Work Location: ',replace(replace(Work_Location,',,',','),', ,',','),char(10),char(10)))
	, iif(j.Assigned_Consultants_001 = '','', concat('Assigned Consultants: ',j.Assigned_Consultants_001,char(10),char(10)))
	, iif(j.Benefits_001 = '' and j.Benefits_002 = '' and j.Benefits_003 = '',''
		,concat('Benefits: ',ltrim(Stuff(
			Coalesce(' ' + NULLIF(j.Benefits_001, ''), '')
			+ Coalesce(', ' + NULLIF(j.Benefits_002, ''), '')
			+ Coalesce(', ' + NULLIF(j.Benefits_003, ''), '')
			, 1, 1, '')),char(10),char(10)))
	, iif(j.Other_Benefit = '','',concat('Other Benefit: ',j.Other_Benefit,char(10),char(10)))
	, iif(Business_Area = '','', concat('Business Area: ',Business_Area,char(10),char(10)))
	, iif(Business_Type = '','', concat('Business Type: ',Business_Type,char(10),char(10)))
	, iif(j.Charge_Rate_1 in ('','0'),'', concat('Charge Rate 1: ',j.Charge_Rate_1,char(10),char(10)))
	, iif(j.Charge_Rate_2 in ('','0'),'', concat('Charge Rate 2: ',j.Charge_Rate_2,char(10),char(10)))
	, iif(j.Charge_Rate_10 in ('','0'),'', concat('Charge Rate 10: ',j.Charge_Rate_10,char(10),char(10)))
	, iif(j.Default_Match_Status = '','', concat('Default Match Status: ',Default_Match_Status,char(10),char(10)))
	, iif(Date_Placed = '','', iif(CHARINDEX(':',Date_Placed)<>0,concat('Date Placed: ', substring(Date_Placed,6,2),'/',substring(Date_Placed,9,2),'/',left(Date_Placed,4),char(10),char(10)),concat('Date Placed: ',Date_Placed,char(10),char(10))))
	, iif(Earliest_Start_Date = '','', iif(CHARINDEX(':',Earliest_Start_Date)<>0,concat('Earliest Start Date: ', substring(Earliest_Start_Date,6,2),'/',substring(Earliest_Start_Date,9,2),'/',left(Earliest_Start_Date,4),char(10),char(10)),concat('Earliest Start Date: ',Earliest_Start_Date,char(10),char(10))))
	, iif(Latest_Start_Date = '','', iif(CHARINDEX(':',Latest_Start_Date)<>0,concat('Latest Start Date: ', substring(Latest_Start_Date,6,2),'/',substring(Latest_Start_Date,9,2),'/',left(Latest_Start_Date,4),char(10),char(10)),concat('Latest Start Date: ',Latest_Start_Date,char(10),char(10))))
	, iif(From_OTE = '' or From_OTE = '0','', concat('OTE From: ',From_OTE,char(10),char(10)))
	, iif(To_OTE = '' or To_OTE = '0','', concat('OTE To: ',To_OTE,char(10),char(10)))
	, iif(From_Salary = '' or From_Salary = '0','', concat('Salary From : ',From_Salary,char(10),char(10)))
	, iif(To_Salary = '' or To_Salary = '0','', concat('Salary To: ',To_Salary,char(10),char(10)))
	, concat('Currency: ',Currency,char(10),char(10))
	, iif(j.Generated_Reference = '','', concat('Generated Reference: ',j.Generated_Reference,char(10),char(10)))
	, iif(j.Key_Word_Codes_001 = '' and j.Key_Word_Codes_002 = '' and j.Key_Word_Codes_003 = '' and j.Key_Word_Codes_004 = '' and j.Key_Word_Codes_005 = '' and j.Key_Word_Codes_006 = '',''
		,concat(char(10),'Key Word Codes: '
			,iif(j.Key_Word_Codes_001 = '','',iif(Key_Word_Level_001<>'',concat(Key_Word_Codes_001, ' (Level: ',Key_Word_Level_001,')'),Key_Word_Codes_001))
			,iif(j.Key_Word_Codes_002 = '','',iif(Key_Word_Level_002<>'',concat(', ',Key_Word_Codes_002, ' (Level: ',Key_Word_Level_002,')'),concat(', ',Key_Word_Codes_002)))
			,iif(j.Key_Word_Codes_003 = '','',iif(Key_Word_Level_003<>'',concat(', ',Key_Word_Codes_003, ' (Level: ',Key_Word_Level_003,')'),concat(', ',Key_Word_Codes_003)))
			,iif(j.Key_Word_Codes_004 = '','',iif(Key_Word_Level_004<>'',concat(', ',Key_Word_Codes_004, ' (Level: ',Key_Word_Level_004,')'),concat(', ',Key_Word_Codes_004)))
			,iif(j.Key_Word_Codes_005 = '','',iif(Key_Word_Level_005<>'',concat(', ',Key_Word_Codes_005, ' (Level: ',Key_Word_Level_005,')'),concat(', ',Key_Word_Codes_005)))
			,iif(j.Key_Word_Codes_006 = '','',iif(Key_Word_Level_006<>'',concat(', ',Key_Word_Codes_006, ' (Level: ',Key_Word_Level_006,')'),concat(', ',Key_Word_Codes_006)))
			,concat(char(10),char(10))))
		--,concat(char(10),'Key Word Codes: ',ltrim(Stuff(
		--	Coalesce(' ' + NULLIF(j.Key_Word_Codes_001, ''), '')
		--	+ Coalesce(', ' + NULLIF(j.Key_Word_Codes_002, ''), '')
		--	+ Coalesce(', ' + NULLIF(j.Key_Word_Codes_003, ''), '')
		--	+ Coalesce(', ' + NULLIF(j.Key_Word_Codes_004, ''), '')
		--	+ Coalesce(', ' + NULLIF(j.Key_Word_Codes_005, ''), '')
		--	+ Coalesce(', ' + NULLIF(j.Key_Word_Codes_006, ''), '')
		--	, 1, 1, '')),char(10)))
	, iif(j.Source = '','', concat('Source: ',j.Source,char(10),char(10)))
	, iif(j.Stage_Reached = '' or j.Stage_Reached is NULL,'',Concat('Stage Reached: ',j.Stage_Reached,char(10),char(10)))
	, iif(Status = '' or Status is null,'', concat('Status: ',Status,char(10),char(10)))
	, iif(j.Terms_Fxd_or_Perc = '','', concat('Terms Fxd or Perc: ',j.Terms_Fxd_or_Perc,char(10),char(10)))
	, iif(j.Terms_Value = '','', concat('Terms Value: ',j.Terms_Value,char(10),char(10)))
	, iif(j.To_Age = '','', concat('To Age: ',j.To_Age,char(10),char(10)))
	, iif(j.Creating_User = '','', concat('Creating User: ',j.Creating_User,char(10),char(10)))
	, iif(CHARINDEX(':',j.Creation_Date)<>0,concat('Creation Date: ', substring(j.Creation_Date,6,2),'/',substring(j.Creation_Date,9,2),'/',left(j.Creation_Date,4),char(10),char(10)),concat('Creation Date: ',j.Creation_Date,char(10),char(10)))
	, iif(j.Amending_User = '','',Concat('Amending User: ', j.Amending_User,char(10),char(10)))
	, iif(CHARINDEX(':',j.Amendment_Date)<>0,concat('Amendment Date: ', substring(j.Amendment_Date,6,2),'/',substring(j.Amendment_Date,9,2),'/',left(j.Amendment_Date,4)),concat('Amendment Date: ',j.Amendment_Date))
	),32000) as 'position-note'
from Vacancies j left join ContactMaxID CM on j.Site_Uniq_ID = CM.CompanyID
				left join dup on j.Unique_ID = dup.Unique_ID
				left join Users u on j.Main_Consultant = u.UserName
				left join ContactManagementSites cms on j.Site_Uniq_ID = cms.Site_Unique_Id
--where j.job_ref = 517	
order by convert(int,j.Unique_ID)