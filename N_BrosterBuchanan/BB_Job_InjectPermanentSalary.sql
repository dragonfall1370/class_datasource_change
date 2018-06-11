select
	concat('BB',cj.JOB) as jobexternalId
	, case
			when jg.JOB_TYPE in (6509063,8255744) then 'PERMANENT'
			when jg.JOB_TYPE in (8255745,8249164) then 'CONTRACT'
			else 'TEMPORARY' end as 'position-type'--refer from: select * from MD_MULTI_NAMES where id in (select distinct JOB_TYPE from PROP_JOB_GEN) and LANGUAGE = 1
	--, as 'position-employmentType'	
	, sal.SALARY_FROM as SalaryFrom
	, sal.SALARY_TO as SalaryTo
from PROP_X_CLIENT_JOB cj --3095 rows
left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE
left join PROP_JOB_JOBBOARD sal on cj.JOB = sal.PRIMREF
where jg.JOB_TYPE in (6509063,8255744) and (sal.SALARY_FROM is not null or sal.SALARY_TO is not null)