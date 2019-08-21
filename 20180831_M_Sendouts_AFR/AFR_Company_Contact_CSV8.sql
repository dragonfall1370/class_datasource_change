-->> COMPANY <<--
--DUPLICATION RECOGNITION
with dup as (
	SELECT Company, Status, CompanyOwner, RegDate, ContactName, ContactSurname, JobTitle, Note_Comment, Location, PrimaryPhone, PrimaryEmailAddress 
	, ROW_NUMBER() OVER(ORDER BY Company ASC) AS CompanyID, ROW_NUMBER() OVER(PARTITION BY Company ORDER BY Company ASC) AS duprn
	FROM AFR_CSV8_Potential)

--MAIN SCRIPT
select concat('AFR9999999',CompanyID) as 'company-externalId'
, case when duprn > 1 then concat(Company, ' - ', duprn) 
	else Company end as 'company-name'
, CompanyOwner as 'company-owners'
, Location as 'company-locationName'
, Location as 'company-locationAddress'
, concat_ws(char(10), '[This is company imported from CSV8 Potential Clients and Contacts]'
	, coalesce('Status: ' + nullif(Status,''),'')
	, coalesce('Reg Date: ' + nullif(RegDate,''),'')
	, coalesce('Contact Name: ' + ContactName + ' ' + ContactSurname,NULL)
	, coalesce('Contact Job Title: ' + nullif(JobTitle,''),NULL)
	, coalesce('Primary Phone: ' + nullif(PrimaryPhone,''),NULL)
	, coalesce('Primary Email Address: ' + nullif(PrimaryEmailAddress,''),NULL)
	, coalesce('Note Comment: ' + nullif(Note_Comment,''),NULL)
	) as 'company-note'
from dup

-->> CONTACT <<--
--DUPLICATION RECOGNITION
with dupComp as (
	SELECT Company, Status, CompanyOwner, RegDate, ContactName, ContactSurname, JobTitle, Note_Comment, Location, PrimaryPhone, PrimaryEmailAddress 
	, ROW_NUMBER() OVER(ORDER BY Company ASC) AS CompanyID, ROW_NUMBER() OVER(PARTITION BY Company ORDER BY Company ASC) AS duprn
	FROM AFR_CSV8_Potential)

--MAIL CONTACT DUPLICATION | (Result: no duplicate email)
, dupContactMail as (select CompanyID, ContactName, ContactSurname, PrimaryEmailAddress
	, ROW_NUMBER() OVER(PARTITION BY lower(PrimaryEmailAddress) ORDER BY CompanyID ASC) AS Emailrn
	from dupComp
	where PrimaryEmailAddress is not NULL
	and PrimaryEmailAddress <> ''
	and PrimaryEmailAddress like '%_@_%.__%')

--MAIN SCRIPT
select concat('AFR9999999', dc.CompanyID) as 'contact-companyId'
, concat('AFR9999999', dc.CompanyID) as 'company-externalId'
, coalesce(dc.ContactName,'Firstname') as 'contact-firstName'
, coalesce(dc.ContactSurname,concat('Lastname - ',dc.CompanyID)) as 'contact-lastName'
, case when Emailrn > 1 then concat(dup.Emailrn,'_',dup.PrimaryEmailAddress)
	else dup.PrimaryEmailAddress end as 'contact-email'
, dc.JobTitle as 'contact-jobTitle'
, dc.PrimaryPhone as 'contact-phone'
, concat_ws(char(10), '[This is contact imported from CSV8 Potential Clients and Contacts]'
	, coalesce('Company: ' + nullif(dc.Company,''),'')
	, coalesce('Status: ' + nullif(dc.Status,''),'')
	, coalesce('Reg Date: ' + nullif(dc.RegDate,''),'')
	, coalesce('Location: ' + nullif(dc.Location,''),'')
	, coalesce('Note Comment: ' + nullif(dc.Note_Comment,''),NULL)
	) as 'company-note'
from dupComp dc
left join dupContactMail dup on dup.CompanyID = dc.CompanyID
where dc.ContactName is not NULL and dc.ContactSurname is not NULL

--total: 232