-------------------------
--PART 1: MAIN SCRIPT
-------------------------
--DUPLICATION REGCONITION
with dup as (SELECT ID, NAME, ROW_NUMBER() OVER(PARTITION BY NAME ORDER BY ID ASC) AS rn 
	FROM Jobs)

--MAX JOB ADS - 1 job may have multiple job ads, get the latest job ads name
, MaxJobAds as (select PEOPLECLOUD1__VACANCY__C, max(NAME) as LatestAds
from Ads where PEOPLECLOUD1__VACANCY__C is not NULL
group by PEOPLECLOUD1__VACANCY__C)

, JobAds as (select ma.PEOPLECLOUD1__VACANCY__C, ma.LatestAds, a.PEOPLECLOUD1__JOB_CONTENT__C
from MaxJobAds ma
left join Ads a on a.NAME = ma.LatestAds)

, ContactNotFound as (select CLIENT_CONTACT__C from Jobs
where CLIENT_CONTACT__C not in (select ID from Contact))

--MAIN SCRIPT
select case 
	when j.CLIENT_CONTACT__C = '' or j.CLIENT_CONTACT__C is NULL or j.CLIENT_CONTACT__C in (select CLIENT_CONTACT__C from ContactNotFound) then 'SP999999999' 
	else j.CLIENT_CONTACT__C end as 'position-contactId'
, j.ID as 'position-externalId'
, iif(j.ID in (select ID from dup where dup.rn > 1)
	, iif(dup.NAME = '' or dup.NAME is NULL,concat('No job title - ',dup.ID),concat(dup.NAME,' - ',dup.ID))
	, iif(j.NAME = '' or j.NAME is null,concat('No job title -',j.ID),j.NAME)) as 'position-title'
, j.NUMBER_OF_POSITIONS__C as 'position-headcount'
, convert(varchar(20),j.CREATEDDATE,120) as 'position-startDate'
, case when j.VACANCY_STATUS__C = 'Closed' then convert(varchar(20),getdate() - 1,120)
	else convert(varchar(20),j.PEOPLECLOUD1__END_DATE__C,120) end as 'position-endDate' --should be updated from PROD
, j.PEOPLECLOUD1__BASE_SALARY__C as 'position-actualSalary'
, concat(coalesce(ltrim(su.EMAIL),''), coalesce(', ' + ltrim(su2.EMAIL),'')) as 'position-owners'
, ja.PEOPLECLOUD1__JOB_CONTENT__C as 'position-publicDescription'
, case when j.JOB_TYPE__C = 'Full-Time' then 'FULL_TIME'
	when j.JOB_TYPE__C = 'Part-Time' then 'PART_TIME'
	else '' end as 'position-employmentType'
, case when j.RECORD_TYPE_NAME__C in ('Advertisement (Permanent)','Internal Vacancy','Permanent Vacancy','Replacement Vacancy') then 'PERMANENT'
	when j.RECORD_TYPE_NAME__C in ('Unqualified Vacancy','Temporary Vacancy','Advertisement (Temporary)','Temporary Vacancy Over 3 Months') then 'TEMPORARY'
	when j.RECORD_TYPE_NAME__C in ('Advertisement (Contract)','Contract Vacancy','Fixed Term Contract Vacancy') then 'CONTRACT'
	when j.RECORD_TYPE_NAME__C in ('Temp to Perm Vacancy') then 'TEMPORARY_TO_PERMANENT'
	else 'PERMANENT' end as 'position-type'
, concat('Job External ID: ',j.ID,char(10)
	, coalesce('Job status: ' + j.VACANCY_STATUS__C + char(10),'')
	, coalesce('Company: ' + com.NAME + ' - ' + j.PEOPLECLOUD1__COMPANY__C + char(10),'')
	, coalesce('Placed Candidate: ' + c.LASTNAME + ' ' + c.FIRSTNAME + ' - ' + j.PEOPLECLOUD1__PLACED_CANDIDATE__C + char(10),'')
	, coalesce('Contact: ' + con.LASTNAME + ' ' + con.FIRSTNAME + ' - ' + j.CLIENT_CONTACT__C + char(10),'')
	, coalesce('Flat Fee: ' + j.PEOPLECLOUD1__FLAT_FEE__C + char(10),'')
	, coalesce('Super: ' + j.PEOPLECLOUD1__SUPER__C + char(10),'')
	, coalesce('Total Package: ' + j.PEOPLECLOUD1__TOTAL_PACKAGE__C + char(10),'')
	, coalesce('Division: ' + j.DIVISION__C + char(10),'')
	, coalesce('Resourcer: ' + j.RESOURCER__C + char(10),'')
	, coalesce('Hours of Work: ' + j.HOURS_OF_WORK__C + char(10),'')
	, coalesce('Days: ' + j.DAYS__C + char(10),'')
	, coalesce('Fee Based On: ' + j.FEE_BASED_ON__C + char(10),'')
	, coalesce('Estimated Vacancy Value: ' + j.ESTIMATED_VACANCY_VALUE__C + char(10),'')
	, coalesce('Pro Rate Months: ' + j.PRO_RATA_MONTHS__C + char(10),'')
	, coalesce('On Cost: ' + j.ONCOST__C + char(10),'')
	, coalesce('On Cost Value: ' + j.ONCOST_VALUE__C + char(10),'')
	, coalesce('Margin: ' + j.MARGIN__C + char(10),'')
	, coalesce('Margin Percentage: ' + j.MARGIN_PERCENTAGE__C + char(10),'')
	, coalesce('Expected Close Date: ' + convert(varchar(20),j.EXPECTED_CLOSE_DATE__C,120) + char(10),'')
	, coalesce('Job Number: ' + j.JOB_NUMBER__C + char(10),'')
	, coalesce('Rate Type: ' + j.RATE_TYPE__C + char(10),'')
	, coalesce('Hours per Day: ' + j.HOURS_PER_DAY__C + char(10),'')
	, coalesce('Days per Week: ' + convert(varchar(max),j.DAYS_PER_WEEK__C) + char(10),'')
	, coalesce('Weekly Margin: ' + j.WEEKLY_MARGIN__C + char(10),'')
	, coalesce('Estimated Contract Value: ' + j.ESTIMATED_CONTRACT_VALUE__C + char(10),'')
	, coalesce('Estimated Temp Value: ' + j.ESTIMATED_TEMP_VALUE__C + char(10),'')
	, coalesce('Job Picked Up Passed: ' + j.JOB_PICKED_UP_PASSED__C + char(10),'')
	, coalesce('Total Package: ' + j.TOTAL_PACKAGE_1__C + char(10),'')
	, coalesce('Calculated Total Package: ' + convert(varchar(max),j.CALCULATED_TOTAL_PACKAGE__C) + char(10),'')
	, coalesce('Consultant Forecast Percentage: ' + j.CONSULTANT_FORECAST_PERCENTAGE__C + char(10),'')
	, coalesce('Forecast Value Consultant: ' + j.FORECAST_VALUE_CONSULTANT__C + char(10),'')
	, coalesce('Replacement Vacancy: ' + j.REPLACEMENT_VACANCY__C + char(10),'')
	, coalesce('Placement Being Replaced: ' + j.PLACEMENT_BEING_REPLACED__C + char(10),'')
) as 'position-note'
from Jobs j
left join JobAds ja on ja.PEOPLECLOUD1__VACANCY__C = j.ID
left join dup on dup.ID = j.ID
left join SiriusUsers su on su.ID = j.OWNERID
left join SiriusUsers su2 on su2.ID = j.CONSULTANT__C
left join Company com on com.ID = j.PEOPLECLOUD1__COMPANY__C
left join Candidate c on c.ID = j.PEOPLECLOUD1__PLACED_CANDIDATE__C
left join Contact con on con.ID = j.CLIENT_CONTACT__C

-------------
--PART 2: CUSTOM FIELDS
-------------

/* 1. PAY RATE for CONTRACT AND TEMP JOB */
select * from compensation

select PEOPLECLOUD1__CANDIDATE_CHARGE_RATE__C from Job

/* 2. Client Charge rate */

select PEOPLECLOUD1__CLIENT_CHARGE_RATE__C from Job -- Client charge rate

/* 3. JOB > FUNCTIONAL EXPERTISE */

select DESK__C from Job

/* 4. JOB CF > Reason For Difficulty */

select FORECAST_NOTES__C from Job