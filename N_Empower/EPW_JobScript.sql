with 

--DUPLICATION REGCONITION
dup as (SELECT Requirement_RequirementId ID, ltrim(rtrim(Requirement_JobTitle)) JobTitle, ROW_NUMBER() OVER(PARTITION BY rtrim(rtrim(Requirement_JobTitle)) ORDER BY Requirement_RequirementId ASC) AS rn 
FROM v_Requirement_AllFields)

--MAIN SCRIPT
select 
--case 
--	when (j.ClientcontactId = '' or j.ClientContactId is NULL) and j.ClientId in (select CompanyID from ContactMaxID) then concat('MP',CM.ContactMaxID)
--	when (j.ClientcontactId = '' or j.ClientcontactId is NULL) and j.ClientId not in (select CompanyID from ContactMaxID) then 'MP9999999'
--	when j.ClientcontactId is NULL and j.ClientId is NULL then 'MP9999999'
--	else concat('MP',j.ClientContactId) end as 'position-contactId'
--, j.ClientId as 'CompanyID'
--, j.ClientContactId as 'ContactID'
concat('EPW',j.RequirementContact_ContactId) as 'position-contactId'
, concat('EPW',j.Requirement_RequirementId) as 'position-externalId'
, j.Requirement_JobTitle as 'position-title(old)'
, iif(j.Requirement_RequirementId in (select ID from dup where dup.rn > 1)
	, iif(dup.Jobtitle = '' or dup.Jobtitle is NULL,concat('No job title-',dup.ID),concat(dup.Jobtitle,'_',dup.ID))
	, iif(j.Requirement_Jobtitle = '' or j.Requirement_Jobtitle is null,concat('No job title -',j.Requirement_RequirementId),j.Requirement_Jobtitle)) as 'position-title'
, iif(rcf.WebText like '%<p>%' or rcf.WebText is null,'',rcf.WebText) as 'position-publicDescription'
, upper(j.Requirement_RequirementEntityType) as 'position-type' --PERMANENT only
, left(rcf.NumberRequired,1) as 'position-headcount'
, right(rcf.JobDescription,(CHARINDEX('\',Reverse(rcf.JobDescription))-1)) as 'position-document'
, convert(varchar(10),j.Requirement_EarliestStartDate,120) as 'position-startDate'
, left(
	concat('Job External ID: EPW',j.Requirement_RequirementId,char(10),char(10)
	, iif(j.RequirementCompany_Name = '' or j.RequirementCompany_Name is null,'', concat('Company: ',j.RequirementCompany_Name,char(10),char(10)))
	, iif(j.RequirementContact_FullName = '','', concat('Contact: ',j.RequirementContact_FullName,char(10),char(10)))
	, iif(j.Requirement_SiteAddress = '' or j.Requirement_SiteAddress is null,'', concat('Address: ',j.Requirement_SiteAddress,char(10),char(10)))
	, iif(j.Requirement_RequirementStatus = '' or j.Requirement_RequirementStatus is null,'', concat('Status: ',j.Requirement_RequirementStatus,char(10),char(10)))
	, iif(j.Requirement_Fee = '' or j.Requirement_Fee is null,'', concat('Fee: ',j.Requirement_Fee,char(10),char(10)))
	, iif(j.Requirement_MaximumSalary = '' or j.Requirement_MaximumSalary is null,'', concat('Maximum Salary: ',j.Requirement_MaximumSalary,char(10),char(10)))
	, iif(j.Requirement_MaximumRate = '' or j.Requirement_MaximumRate is null,'', concat('Maximum Rate: ',j.Requirement_MaximumRate,char(10),char(10)))
	, iif(j.Requirement_Comments = '' or j.Requirement_Comments is NULL,'',Concat(char(10),'Comments: ',char(10),j.Requirement_Comments))),32000)
	--, coalesce (char(10) + 'Other Notes: ' + j.Notes, '')),32000)
	 as 'position-note'
from v_Requirement_AllFields j 
				left join RequirementConfigFields rcf on j.Requirement_RequirementId = rcf.EntityId
				left join JobTitle jt on rcf.JobTitle = jt.JobTitleId
				left join dup on j.Requirement_RequirementId = dup.ID
			
			--left join Locations loc on j.LocationId = loc.LocationId