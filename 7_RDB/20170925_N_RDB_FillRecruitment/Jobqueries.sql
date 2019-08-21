select * from Jobs
select * from Sectors
select * from Clients
select JobId, j.SectorId, st.SectorName
from Jobs j left join Sectors st on j.SectorId = st.SectorId

select JobId, ClientHirerLegalEntityId, c.Company
from Jobs j left join Clients c on j.ClientHirerLegalEntityId = c.ClientID
where ClientHirerLegalEntityId is not null

select JobId, ClientHirerLegalEntityId
from Jobs
where ClientHirerLegalEntityId is not null

select JobId, PositionAttributeId, a.Description
from Jobs j left join Attributes a on j.PositionAttributeId = a.AttributeId
where PositionAttributeId is not null

select JobId, j.EmploymentTypeId, et.SystemCode, et.Description
from Jobs j left join EmploymentTypes et on j.EmploymentTypeId = et.EmploymentTypeId
where j.EmploymentTypeId = 7

select * from Jobs
select * from EmploymentTypes
select * from AwrStatus
select * from JobStatus
select * from JobCategories
select * from JobAttributes
select * from Attributes
select * from ApplicantActionStatus
select * from ApplicantAction_AuditStatus
select * from JobCategories
select * from JobAttributes
select * from ApplicantActionStatus
select * from INFORMATION_SCHEMA.COLUMNS 
where COLUMN_NAME like '%StatusId%' 
order by TABLE_NAME

select JobId, j.CurrencyId, c.CurrencyName, c.IsoCode
from Jobs j left join Currency c on j.CurrencyId = c.CurrencyId
where j.currencyId is not null

select * from Jobs where JobTitle is null

select JobId, convert(numeric(19,1),Salary) from Jobs where Salary > 0
select JobId, Notes from Jobs where CommissionPerc > 0

select * from Jobs where StartDate is not null
select * from Interviews
select * from ApplicantActions

select JobId, convert(varchar(10),StartDate,120) as Startdate
from Jobs where StartDate is not null

