with ContactMaxID as (select 
case when ClientId is NULL then '9999999'
else ClientId end as CompanyID
, max(ClientContactId) as ContactMaxID 
from ClientContacts
group by ClientId)

, permanentSalary as (select JobId, convert(numeric(19,1),Salary) as Salary 
from Jobs where EmploymentTypeId in (4,7))
, PayRate as (select JobId, convert(numeric(19,1),Salary) as Salary 
from Jobs where EmploymentTypeId in (5,6))

,ParentCompany as (select JobId, ClientHirerLegalEntityId as ParentId, c.Company
from Jobs j left join Clients c on j.ClientHirerLegalEntityId = c.ClientId
where j.ClientHirerLegalEntityId is not null)

-----------------Job Owners
, tempJobOwner as (select jc.JobConsultantId, jc.JobId, jc.UserId, u.EmailAddress
from JobConsultants jc left join Users u on jc.UserId = u.UserId
where u.EmailAddress like '%@%')
--------
, JobOwners as (SELECT JobId, 
     STUFF(
         (SELECT ',' + EmailAddress
          from  tempJobOwner
          WHERE JobId = tjo.JobId
    order by JobId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'jobOwner'
FROM tempJobOwner as tjo
GROUP BY tjo.JobId)

--DUPLICATION REGCONITION
, dup as (SELECT JobId, JobTitle, ROW_NUMBER() OVER(PARTITION BY JobTitle ORDER BY JobId ASC) AS rn 
FROM Jobs)

--MAIN SCRIPT
select 
case 
	when (j.ClientcontactId = '' or j.ClientContactId is NULL) and j.ClientId in (select CompanyID from ContactMaxID) then concat('MP',CM.ContactMaxID)
	when (j.ClientcontactId = '' or j.ClientcontactId is NULL) and j.ClientId not in (select CompanyID from ContactMaxID) then 'MP9999999'
	when j.ClientcontactId is NULL and j.ClientId is NULL then 'MP9999999'
	else concat('MP',j.ClientContactId) end as 'position-contactId'
, j.ClientId as 'CompanyID'
, j.ClientContactId as 'ContactID'
, concat('MP',j.JobId) as 'position-externalId'
, j.JobTitle as 'position-title(old)'
, iif(j.JobId in (select JobId from dup where dup.rn > 1)
	, iif(dup.JobTitle = '' or dup.JobTitle is NULL,concat('No job title-',dup.JobId),concat(dup.JobTitle,'-',dup.JobId))
	, iif(j.JobTitle = '' or j.JobTitle is null,concat('No job title -',j.JobId),j.JobTitle)) as 'position-title'
, case 
when j.EmploymentTypeId = 6 then 'CONTRACT'
when j.EmploymentTypeId = 5 then 'TEMPORARY'
else 'PERMANENT' end as 'position-type'
, j.NoOfPlaces as 'position-headcount'
, case
when j.CurrencyId = 10 then 'GBP'
when j.CurrencyId = 11 then 'USD'
when j.CurrencyId = 12 then 'EUR'
else NULL end as 'position-currency'
, ps.Salary as 'position-actualSalary'
, pr.Salary as 'position-payRate'
, jo.jobOwner as 'position-owners'
, convert(varchar(10),j.StartDate,120) as 'position-startDate'
, left(
	concat('Job External ID: MP',j.JobId,char(10),char(10)
	, iif(j.ClientHirerLegalEntityId = '' or j.ClientHirerLegalEntityId is NULL,'', concat('Hirer Legal Entity: ',pc.Company,char(10),char(10)))
	, iif(j.PositionAttributeId = '' or j.PositionAttributeId is NULL,'',concat('Position Attribute: ',a.Description,char(10),char(10)))
	, iif(j.SectorId = '' or j.SectorId is NULL,'',concat('Sector: ',st.SectorName,char(10),char(10)))
	, iif(j.ClientContactId = '' or j.ClientContactId is NULL,'',concat('Contact: ',p.PersonName,' ', p.SurName,char(10),char(10)))
	, iif(j.ClientID = '' or j.ClientID is NULL,'',concat('Company: ',c.Company,char(10),char(10)))
	, iif(j.JobRefNo = '' or j.JobRefNo is NULL,'',concat('Job Ref No.: ',j.JobRefNo,char(10),char(10)))
	, iif(j.NoOfPlaces = '' or j.NoOfPlaces is NULL,'',concat('No of Places: ',j.NoOfPlaces,char(10),char(10)))
	, iif(j.StatusId = '' or j.StatusId is NULL,'',concat('Status: ',js.Description,char(10),char(10)))
	, iif(j.CurrencyId = '' or j.CurrencyId is NULL,'',concat('Currency: ',cr.CurrencyName,', Conversion Rate: ',cr.ConversionRate,char(10),char(10)))
	, iif(j.WorkAddress = '' or j.WorkAddress is NULL,'',concat('Work Address: ',j.WorkAddress,char(10),char(10)))
	--, iif(ltrim(rtrim(j.LocationId)) = '' or j.LocationId is NULL,'',concat('Location: ',loc.Description,char(10)))
	, iif(j.MinBasic is NULL,'',concat('Salary From: ',convert(numeric(19,2),j.MinBasic),char(10),char(10)))
	, iif(MaxBasic is NULL,'',concat('Salary To: ',convert(numeric(19,2),j.MaxBasic),char(10),char(10)))
	, iif(jgv.Interviews = '' or jgv.Interviews is NULL,'',concat('Interviews: ',jgv.Interviews,char(10),char(10)))
	, iif(j.CommissionPerc is NULL,'',concat('Commission %: ',j.CommissionPerc,char(10),char(10)))
	, iif(j.Notes = '' or j.Notes is NULL,'',concat(char(10),'Notes: ',j.Notes))),32000)
	--, coalesce (char(10) + 'Other Notes: ' + j.Notes, '')),32000)
	 as 'position-note'
from Jobs j left join ContactMaxID CM on j.ClientId = CM.CompanyID
			left join dup on j.JobId = dup.JobId
			left join permanentSalary ps on j.JobId = ps.JobId
			left join PayRate pr on j.JobId = pr.JobId
			left join Clients c on j.ClientId = c.ClientID
			left join Attributes a on j.PositionAttributeId = a.AttributeId
			left join Sectors st on j.SectorId = st.SectorId
			left join ClientContacts cc on j.ClientContactId = cc.ClientContactId
			left join Person p on cc.ContactPersonId = p.PersonID
			left join JobStatus js on j.StatusId = js.JobStatusId
			left join Currency cr on j.CurrencyId = cr.CurrencyId
			left join ParentCompany pc on j.JobId = pc.JobId
			left join VW_JOB_GRID_VIEW jgv on j.JobId = jgv.JobId
			left join JobOwners jo on j.JobId = jo.JobId
			--left join Locations loc on j.LocationId = loc.LocationId