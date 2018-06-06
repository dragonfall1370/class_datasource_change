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

--CONTACT WITHOUT BEING LISTED IN CONTACT TABLE
, ContactNotFound as (select CLIENT_CONTACT__C 
    from Jobs
    where CLIENT_CONTACT__C not in (select ID from Contact))

--COMPARISON BW JOB COMPANY AND CONTACT COMPANY -> if different, get DEFAULT CONTACT within JOB COMPANY
, JobCompContactComp as (select j.ID as JobID
    , j.PEOPLECLOUD1__COMPANY__C as JobCompany
    , j.CLIENT_CONTACT__C as JobContact
    , c.ACCOUNTID as ContactCompany
    from Jobs j
	left join Contact c on c.ID = j.CLIENT_CONTACT__C)

, JobCompanyDiff as (select JobID
    , JobCompany
    , JobContact
    , ContactCompany
    from JobCompContactComp
    where JobCompany <> ContactCompany)

--MAIN SCRIPT
select
case 
	when j.ID in (select JobID from JobCompanyDiff) and j.PEOPLECLOUD1__COMPANY__C is not NULL then concat(j.PEOPLECLOUD1__COMPANY__C,'-DC')
	when j.CLIENT_CONTACT__C in (select CLIENT_CONTACT__C from ContactNotFound) and j.PEOPLECLOUD1__COMPANY__C is not NULL then concat(j.PEOPLECLOUD1__COMPANY__C,'-DC')
	when j.CLIENT_CONTACT__C = '' and j.PEOPLECLOUD1__COMPANY__C is not NULL then concat(j.PEOPLECLOUD1__COMPANY__C,'-DC')
	when j.CLIENT_CONTACT__C is NULL and j.PEOPLECLOUD1__COMPANY__C is not NULL then concat(j.PEOPLECLOUD1__COMPANY__C,'-DC')
    when j.PEOPLECLOUD1__COMPANY__C is NULL and j.CLIENT_CONTACT__C is NULL then 'SP999999999'
	else j.CLIENT_CONTACT__C end as 'position-contactId'

, j.ID as 'position-externalId'
, case when exists (select ID from dup where dup.rn > 1 and j.ID = ID) AND exists (select JobID from JobCompanyDiff where j.ID = JobID) AND (dup.NAME is not NULL) then concat(dup.NAME,' - ',con.LASTNAME,' ',con.FIRSTNAME,' - ',dup.ID)
    when exists (select ID from dup where dup.rn > 1 and j.ID = ID) AND exists (select JobID from JobCompanyDiff where j.ID = JobID) AND (dup.NAME = '' or dup.NAME is NULL) then concat('No job title - ',con.LASTNAME,' ',con.FIRSTNAME,' - ',dup.ID)
    when exists (select JobID from JobCompanyDiff where j.ID = JobID) AND j.NAME is not NULL then concat(j.NAME,' - ',con.LASTNAME,' ',con.FIRSTNAME)
    when j.NAME = '' or j.NAME is null then concat('No job title -',j.ID)
    else j.NAME end as 'position-title'
, j.NUMBER_OF_POSITIONS__C as 'position-headcount'
, convert(varchar(20),j.CREATEDDATE,120) as 'position-startDate'

--END DATE JOB SHOULD BE USED IN 3 CASES
, case when j.CLOSED_DATE__C is NULL and j.PEOPLECLOUD1__END_DATE__C is not NULL then j.CLOSED_DATE__C
	when j.EXPECTED_CLOSE_DATE__C is NULL and j.PEOPLECLOUD1__END_DATE__C is not NULL then j.PEOPLECLOUD1__END_DATE__C
	when j.PEOPLECLOUD1__END_DATE__C is NULL and  j.EXPECTED_CLOSE_DATE__C is not NULL then j.EXPECTED_CLOSE_DATE__C
    when j.EXPECTED_CLOSE_DATE__C is NULL and j.PEOPLECLOUD1__END_DATE__C is NULL and j.VACANCY_STATUS__C = 'Closed' then convert(varchar(20),getdate() - 1,120)
    else convert(varchar(20),j.PEOPLECLOUD1__END_DATE__C,120) end as 'position-endDate'

, j.PEOPLECLOUD1__BASE_SALARY__C as 'position-actualSalary'
, 'AUD' as 'position-currency'
, concat(coalesce(ltrim(su.EMAIL),''), coalesce(',' + ltrim(su2.EMAIL),'')) as 'position-owners'
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
    , coalesce('Candidate charge rate: ' + j.PEOPLECLOUD1__CANDIDATE_CHARGE_RATE__C + char(10),'')
    , coalesce('Client charge rate: ' + j.PEOPLECLOUD1__CLIENT_CHARGE_RATE__C + char(10),'')
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
	, coalesce('Fore cast notes: ' + j.FORECAST_NOTES__C + char(10),'')
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

--Table Input
select ID as Sirius_jobExtID
, DESK__C as Sirisu_FExp
from Jobs
where DESK__C is not NULL

--MAPPING (from select * from position_description_functional_expertise )
--position_id
--functional_expertise_id
--sub_functional_expertise_id

--PROCESS

/* 4. JOB CF > Reason For Difficulty */
--TABLE INPUT
select ID, FORECAST_NOTES__C
from Job

--MAPPING
-->> Insert/Update >> FORECAST_NOTES__C = reason_for_difficulty (from position_description)

-------------
--PART 3: INJECT JOB ACTIVITIES by CANDIDATE PLACEMENT with PLACEMENT INFO
-------------
select cm.PEOPLECLOUD1__PLACEMENT__C as Sirius_jobExtID
	, -10 as Sirius_user_account_id
	, concat('Job Application ID: ',cm.ID,char(10)
		, 'Candidate name: ',c.FIRSTNAME, ' ', c.LASTNAME,char(10)
		, 'Created date: ',c.CREATEDDATE,char(10)
		, coalesce('Fee based on: ' + cm.FEE_BASED_ON__C + char(10),'')
		, coalesce('Pro rata months: ' + convert(varchar(max),cm.PRO_RATA_MONTHS__C) + char(10),'')
		, coalesce('Base salary: ' + convert(varchar(max),cm.BASE_SALARY__C) + char(10),'')
		, coalesce('Super: ' + cm.SUPER__C + char(10),'')
		, coalesce('Fee: ' + cm.FEE__C + char(10),'')
		, coalesce('Flat fee: ' + cm.FLAT_FEE__C + char(10),'')
		, coalesce('Actual placement value: ' + cm.ACTUAL_PLACEMENT_VALUE__C + char(10),'')
		, coalesce('Pay rate custom: ' + cm.PAY_RATE_CUSTOM__C + char(10),'')
		, coalesce('Change rate custom: ' + cm.CHARGE_RATE_CUSTOM__C + char(10),'')
		, coalesce('Oncost: ' + cm.ONCOST__C + char(10),'')
		, coalesce('Oncost value: ' + cm.ONCOST_VALUE__C + char(10),'')
		, coalesce('Margin value: ' + cm.MARGIN_VALUE__C + char(10),'')
		, coalesce('Margin: ' + cm.MARGIN__C + char(10),'')
		, coalesce('Hours: ' + cm.HOURS__C + char(10),'')
		, coalesce('Client charge: ' + cm.CLIENT_CHARGE__C + char(10),'')
		, coalesce('Candidate pay: ' + cm.CANDIDATE_PAY__C + char(10),'')
		, coalesce('Hours of work: ' + cm.HOURS_OF_WORK__C + char(10),'')
		, coalesce('Assignment location: ' + cm.ASSIGNMENT_LOCATION__C + char(10),'')
		, coalesce('Guarantee Period: ' + cm.GUARANTEE_PERIOD__C + char(10),'')
		, coalesce('Guarantee date: ' + cm.GUARANTEE_DATE__C + char(10),'')
		, coalesce('Placement type: ' + cm.PLACEMENT_TYPE__C + char(10),'')
		, coalesce('PO Number: ' + cm.PO_NUMBER__C + char(10),'')
		, coalesce('Invoice Number: ' + cm.INVOICE_NUMBER__C + char(10),'')
		, coalesce('Due date: ' + convert(varchar(20),cm.DUE_DATE__C,120) + char(10),'')
		, coalesce('Placed date: ' + convert(varchar(20),cm.PLACED_DATE__C,120) + char(10),'')
		, coalesce('Weekly margin Sirius: ' + cm.WEEKLY_MARGIN_SIRIUS__C + char(10),'')
		, coalesce('Total package: ' + convert(varchar(max),cm.TOTAL_PACKAGE_C__C) + char(10),'')
		, coalesce('Weekly margin IND SBS: ' + cm.WEEKLY_MARGIN_IND_SBS__C + char(10),'')
		, coalesce('Calculated total package: ' + cm.CALCULATED_TOTAL_PACKAGE__C + char(10),'')
		, coalesce('Candidate payment type 1: ' + cm.CANDIDATE_PAYMENT_TYPE1__C + char(10),'')
		, coalesce('ABN ACN Company Enterprise: ' + cm.ABN_ACN_COMPANY_ENTERPRISE__C + char(10),'')
		, coalesce('Address Company Enterprise: ' + cm.ADDRESS_COMPANY_ENTERPRISE__C + char(10),'')
		, coalesce('Company Enterprise name: ' + cm.COMPANY_ENTERPRISE_NAME__C + char(10),'')
		, coalesce('Payment terms: ' + cm.PAYMENT_TERMS__C + char(10),'')
		, coalesce('ABN ACN: ' + cm.ABN_ACN__C + char(10),'')
		, coalesce('Client signatory email: ' + cm.CLIENT_SIGNATORY_EMAIL__C + char(10),'')
		, coalesce('Company address: ' + cm.COMPANY_ADDRESS__C + char(10),'')
		, coalesce('Notice period: ' + cm.NOTICE_PERIOD__C + char(10),'')
		, coalesce('Invoice recipient Email address: ' + cm.INVOICE_RECIPIENT_EMAIL_ADDRESS__C + char(10),'')
		, coalesce('Status candidate progress: ' + cm.STATUS_CANDIDATE_PROGRESS__C + char(10),'')
		, coalesce('Total fee: ' + cm.TOTAL_FEE__C + char(10),'')
		, coalesce('Record type name: ' + cm.RECORD_TYPE_NAME__C + char(10),'')
		, coalesce('Monthly drip fee: ' + cm.MONTHLY_DRIP_FEE__C + char(10),'')
		, coalesce('Sales value: ' + cm.SALES_VALUE__C,'')
		) as Sirius_content
	, convert(datetime, convert(varchar(30), replace(replace(cm.CREATEDDATE,'T',' '),'.000Z','')), 101) as Sirius_insert_timestamp
	, 'comment' as Sirius_category
	, 'job' as Sirius_type
from CandidateManagement cm
left join Candidate c on c.ID = cm.PEOPLECLOUD1__CANDIDATE__C
where STATUS_CANDIDATE_PROGRESS__C like '%Placed%'