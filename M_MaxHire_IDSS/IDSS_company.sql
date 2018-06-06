with
--DUPLICATION REGCONITION
dup as (SELECT id, compname, ROW_NUMBER() OVER(PARTITION BY lower(compname) ORDER BY id ASC) AS rn
		FROM company
		where DeleteFlag = 0)

----COMPANY ID LINKS FROM EMPLOYMENT
, EmploymentComp as (select distinct CompanyName as CompanyName
	from Employment
	where (id is NULL or id = 0)
	and CompanyName <> ''
	and Employment_id in (select Employment_id from People where RoleType = 1 and DeleteFlag = 0))

	, ContactComp as (select 10000 + row_number() over (order by CompanyName asc) as rn, CompanyName
	from EmploymentComp
	where ltrim(CompanyName) <> ''
	and lower(CompanyName) not in (select distinct lower(compname) from company where DeleteFlag = 0))
	
--MAIN SCRIPT
select concat('IDSS',c.id) as 'company-externalId'
	, iif(c.id in (select id from dup where dup.rn > 1)
		, iif(dup.compname = '' or dup.compname is NULL,concat('Company name -',dup.ID),concat(dup.compname,' - ',dup.ID))
		, iif(c.compname = '' or c.compname is null,concat('Company name -',dup.ID),c.compname)) as 'company-name'
	, left(web,100) as 'company-website'
	, concat('Company External ID: ',c.id) as 'company-note'
from company c
left join dup on dup.id = c.id
where DeleteFlag = 0

UNION ALL

select 'IDSS9999999','Default company',NULL,'This is default company'

UNION ALL

select concat('IDSS',rn)
, CompanyName
, NULL
,'This is company imported from Contact Employment'
from ContactComp