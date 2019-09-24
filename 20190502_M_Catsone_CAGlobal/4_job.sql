with 
--DUPLICATION REGCONITION
dup as (select id, title, row_number() over(partition by lower(title) order by id) as rn
	from jobs)

, NewJobTitle as (select id
	, case when rn > 1 then concat(title, ' - ', id)
	else title end as NewJobTitle
	from dup)


--JOB FINAL DOCUMENTS (after creating companyDocument table)
, Documents as (select data_item_id, string_agg(cast(concat(id
					, right(filename, charindex('.',reverse(filename)))) as nvarchar(max)),',') as jobDocuments
	from attachments
	where data_item_type = 'job'
	group by data_item_id)

--JOB TYPE
, JobType as (select cv.jobs_id
	, cv.cf_value
	, c1.label as jobtype
	from jobs_custom_fields_value cv
	left join jobs_custom_fields_153614 c1 on c1.id = cv.cf_value
	where cv.cf_id = 153614) --Job Type

--COMPANY MAX CONTACT ID
, maxContactID as (select companyid, max(id) as maxContactID
	from contacts
	where companyid > 0
	group by companyid)

--JOB ADDRESS
, JobAddress as (select id, nullif(concat_ws(', '
	, nullif(nullif(locationcity,''),'0')
	, nullif(nullif(locationstate,''),'0')
	, nullif(nullif(locationpostalcode,''),'0')
	, nullif(nullif(countrycode,''),'0')),', ') as JobAddress
	from jobs)

--JOBS COUNTRIES -Custom Field #154108
, JobCountries as (select cv.jobs_id
	, cv.cf_value
	, c1.label as countries
	from jobs_custom_fields_value cv
	left join [jobs_custom_fields_154108] c1 on c1.id = cv.cf_value
	where cv.cf_id = 154108) --job countries

--JOB CURRENCY
, JobCurrency as (select cv.jobs_id
	, case when cf_value = '$' then 'USD'
		when cf_value = '$ USD' then 'USD'
		when cf_value = '$US Dollar' then 'USD'
		when cf_value = '$US Dollars' then 'USD'
		when cf_value = '$USDollars' then 'USD'
		when cf_value = 'AED' then 'AED'
		when cf_value = 'AED (3.18886 Exchange Rate to Rand)' then 'AED'
		when cf_value = 'AUD' then 'AUD'
		when cf_value = 'C$' then 'CAD'
		when cf_value = 'CAD' then 'CAD'
		when cf_value = 'CDF' then 'CDF'
		when cf_value = 'CHF' then 'CHF'
		when cf_value = 'EGP' then 'EGP'
		when cf_value = 'Egypt' then 'EGP'
		when cf_value = 'Egyptian Pound' then 'EGP'
		when cf_value = 'Egyptian pounds' then 'EGP'
		when cf_value = 'EUR' then 'EUR'
		when cf_value = 'Euro' then 'EUR'
		when cf_value = 'Euros' then 'EUR'
		when cf_value = 'GBP' then 'GBP'
		when cf_value = 'IDR' then 'IDR'
		when cf_value = 'KES' then 'KES'
		when cf_value = 'MAD' then 'MAD'
		when cf_value = 'MXN' then 'MXN'
		when cf_value = 'NGN' then 'NGN'
		when cf_value = 'QAR' then 'QAR'
		when cf_value = 'RWF' then 'RWF'
		when cf_value = 'SAR' then 'SAR'
		when cf_value = 'TZS' then 'TZS'
		when cf_value = 'UGX' then 'UGX'
		when cf_value = 'US' then 'USD'
		when cf_value = 'US $' then 'USD'
		when cf_value = 'US Dollar' then 'USD'
		when cf_value = 'US Dollars' then 'USD'
		when cf_value = 'US$' then 'USD'
		when cf_value = 'USD' then 'USD'
		when cf_value = 'USD $' then 'USD'
		when cf_value = 'USD & SSP' then 'USD'
		when cf_value = 'USD / KES' then 'USD'
		when cf_value = 'USD / ZAR' then 'USD'
		when cf_value = 'USD 3000' then 'USD'
		when cf_value = 'USD`' then 'USD'
		when cf_value = 'USSD' then 'USD'
		when cf_value = 'ZAR' then 'ZAR'
		when cf_value = 'ZAR / Swazi currency' then 'ZAR'
		when cf_value = 'ZAR R' then 'ZAR'
		when cf_value = 'ZAR Rand' then 'ZAR'
		else NULL end as currency
	from jobs_custom_fields_value cv
	where cv.cf_id = 175621
	and cf_value <> '') --job currency

--GLOBAL REGION
, GlobalRegion as (select cv.jobs_id
	, cv.cf_value
	, c1.label as globalregions
	from jobs_custom_fields_value cv
	left join [jobs_custom_fields_154093] c1 on c1.id = cv.cf_value
	where cv.cf_id = 154093) --global regions | no multiple values

--Candidate Previous Employer
, PreviousEmployer as (select jobs_id
	, cf_value
	from jobs_custom_fields_value
	where cf_id = 202918
	and cf_value <> '')

--TCTC Offered
, TCTCOffered as (select jobs_id
	, cf_value
	from jobs_custom_fields_value
	where cf_id = 175729
	and cf_value <> '')

--Placement Fee (%)
, PlacementFee as (select jobs_id
	, cf_value
	from jobs_custom_fields_value
	where cf_id = 175624
	and cf_value <> '')

--Placement Value
, PlacementValue as (select jobs_id
	, cf_value
	from jobs_custom_fields_value
	where cf_id = 175627
	and cf_value <> '')

--Contractor Salary Rate
, ContractorSalarySplit as (select jobs_id
	, cf_value
	, value as contractorid
	, row_number() over(partition by jobs_id order by value) as rn
	from jobs_custom_fields_value
	cross apply string_split(replace(replace(cf_value,'[',''),']',''),',')
	where cf_id = 175723
	and cf_value <> '[]')

, ContractorSalary as (select cv.jobs_id
	, string_agg(c1.label,', ') as ContractorSalary
	from ContractorSalarySplit cv
	left join [jobs_custom_fields_175723] c1 on c1.id = cv.contractorid
	group by jobs_id
	)

--MAIN SCRIPT
select concat('CG',j.id) as [position-externalId]
	, case when j.contactid is not NULL then concat('CG',j.contactid)
		when j.companyid = 0 or mc.maxContactID is NULL then concat('CG9999999',j.companyid)
		else concat('CG', mc.maxContactID) end as [position-contactId]
	, j.companyid
	, njt.NewJobTitle as [position-title]
	, case when j.startdate is not NULL and j.startdate not in ('') then convert(varchar(10),j.startdate,120)
		else convert(varchar(10),datecreated,120) end as [position-startDate]
	, case when j.ownerid in (0) then NULL
		else u.username end as [contact-owners]
	, jcc.currency as [position-currency]
	, j.description as [position-publicDescription]
	, d.jobDocuments as [position-document]
	, case 
		when jt.JobType = 'FIFO' then 'TEMPORARY'
		when jt.JobType = 'Contract Position' then 'CONTRACT'
		when jt.JobType = 'Permanent Position' then 'PERMANENT'
		when jt.JobType = 'Residential' then 'PERMANENT'
		when jt.JobType = 'Contractor Payroll' then 'CONTRACT'
		else 'PERMANENT' end as [position-type]
	, concat_ws(char(10)
		, concat('Job External ID: ', j.id)
		, coalesce('Job Order #: ' + nullif(j.externalid,''),NULL)
		, coalesce('Status: ' + nullif(j.embeddedstatustitle,''),NULL)
		, coalesce('TCTC Offered: ' + nullif(tco.cf_value,''),NULL)
		, coalesce('Placement Value: ' + nullif(pv.cf_value,''),NULL)
		, coalesce('Placement Fee %: ' + nullif(pf.cf_value,''),NULL)
		, coalesce('Contractor Salary Rate: ' + nullif(cs.ContractorSalary,''),NULL)
		, coalesce('PreviousEmployer: ' + nullif(pe.cf_value,''),NULL)
		, coalesce('Company ID: ' + nullif(convert(varchar(max),j.companyid),''),NULL)
		, coalesce('Company name: ' + nullif(j.embeddedcompanyname,''),NULL)
		, coalesce('Company department: ' + nullif(cd.name,''),NULL)
		, coalesce('Country code: ' + nullif(j.countrycode,''),NULL)
		, coalesce('Date created: ' + nullif(nullif(convert(varchar(10),j.datecreated,120),''),'NA'),NULL)
		, coalesce('Date modified: ' + nullif(nullif(convert(varchar(10),j.datemodified,120),''),'NA'),NULL)
		, coalesce('Is Published: ' + nullif(nullif(case when j.ispublished = 1 then 'YES' 
				when j.ispublished = 0 then 'NO' else convert(varchar(max),j.ispublished) end,''),'0'),NULL)
		--, coalesce('Is Published: ' + nullif(j.ispublished,''), NULL)
		, coalesce('Job Countries: ' + nullif(jc.countries,''),NULL)
		, coalesce('Job Location: ' + nullif(ja.JobAddress,''),NULL)
		, coalesce('Job Owner: ' + nullif(concat(u.firstname,' ',u.lastname),''),NULL)
		, coalesce('Job Recruiter: ' + nullif(concat(u2.firstname,' ',u2.lastname),''),NULL)
		, coalesce('Job Global Region: ' + nullif(gr.globalregions,''),NULL)
		) as [position-note]
from jobs j
left join NewJobTitle njt on njt.id = j.id
left join Documents d on d.data_item_id = j.id
left join maxContactID mc on mc.companyid = j.companyid
left join users u on u.id = j.ownerid
left join users u2 on u2.id = j.recruiterid
left join JobAddress ja on ja.id = j.id
left join JobType jt on jt.jobs_id = j.id
left join JobCountries jc on jc.jobs_id = j.id
left join JobCurrency jcc on jcc.jobs_id = j.id
left join GlobalRegion gr on gr.jobs_id = j.id
left join PreviousEmployer pe on pe.jobs_id = j.id
left join TCTCOffered tco on tco.jobs_id = j.id
left join PlacementValue pv on pv.jobs_id = j.id
left join PlacementFee pf on pf.jobs_id = j.id
left join ContractorSalary cs on cs.jobs_id = j.id
left join companies_departments cd on cd.id = j.departmentid
where tco.cf_value is not NULL
order by j.id