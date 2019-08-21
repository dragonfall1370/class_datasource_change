with loc as (
		select job_ref, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(job_address, ''), '')
			+ Coalesce(', ' + NULLIF(job_address2, ''), '')
			+ Coalesce(', ' + NULLIF(job_town, ''), '')
			+ Coalesce(', ' + NULLIF(job_county, ''), '')
			+ Coalesce(', ' + NULLIF(job_pcode, ''), '')
			+ iif(job_country = '231', ', United Arab Emirates', iif(job_country = '232', ', United Kingdom (Great Britain)',', Belgium'))
			, 1, 1, '')) as 'locationName'
	from jobs)

, skill1 as (
select j.job_ref, s.description
from jobs j left join jobskill js on j.job_ref = js.job_ref
left join skill s on js.skill_ref = s.skill_ref
where js.skill_ref <> 0)

, job_skill as (SELECT job_ref,
     STUFF(
         (SELECT ', ' + description
          from  skill1
          WHERE job_ref = s.job_ref
    order by job_ref asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS skill
FROM skill1 as s
GROUP BY s.job_ref)

, owners as (select consult_ref, consult_name
	, case consult_name 
		when 'Support' then 'zed@rebelrecruiters.co.uk'
		when 'Azar Hussain' then 'azar@rebelrecruiters.co.uk'
		when 'Mica Bell' then 'mica@rebelrecruiters.co.uk'
		when 'Faisal Faik' then 'fess@rebelrecruiters.co.uk'
		when 'Yas Mahtab' then 'yas@rebelrecruiters.co.uk'
		when 'Loukia Poutziouris' then 'loukia@rebelrecruiters.co.uk'
		when 'Hamzah Ikram' then 'hamzah@rebelrecruiters.co.uk'
		when 'Julija Lipnickaja' then 'julija@rebelrecruiters.co.uk'
		when 'Ben Williamson' then 'ben@rebelrecruiters.co.uk'
		when 'Hayley McGowan' then 'hayley@rebelrecruiters.co.uk'
		else '' end as consult_email
from consultant where consult_inits <> '')

--DUPLICATION REGCONITION
, dup as (SELECT job_ref, job_title, ROW_NUMBER() OVER(PARTITION BY job_title ORDER BY job_ref ASC) AS rn 
from jobs)

--MAIN SCRIPT
select 
concat('REBEL',j.cont_ref) as 'position-contactId'
, j.client_ref as 'CompanyID'
, concat('REBEL',j.job_ref) as 'position-externalId'
, j.job_title as 'position-title(old)'
, iif(j.job_ref in (select job_ref from dup where dup.rn > 1)
	, iif(dup.job_title = '' or dup.job_title is NULL,concat('No job title-',dup.job_ref),concat(dup.job_title,'-',dup.job_ref))
	, iif(j.job_title = '' or j.job_title is null,concat('No job title -',j.job_ref),j.job_title)) as 'position-title'
, iif(j.job_desc <> 'null' and j.job_desc <> '',j.job_desc,'') as 'position-publicDescription'
--, iif(CHARINDEX(':',job_recdate)<>0,concat(substring(job_recdate,6,2),'/',substring(job_recdate,9,2),'/',left(job_recdate,4)),job_recdate) as 'position-startDate1'--dd/mm/yyyy -> not suppoted to import
, iif(CHARINDEX(':',job_recdate)<>0,concat(substring(job_recdate,9,2),'/',substring(job_recdate,6,2),'/',left(job_recdate,4)),concat(substring(job_recdate,4,2),'/',left(job_recdate,2),'/',right(job_recdate,4))) as 'position-startDate'--mm/dd/yyyy
, job_recdate as recdate
, como.consult_email as 'position-owners'
, job_sal_curr_code as 'position-currency'
, job_salary as 'position-actualSalary'
, case
		when job_type = 'P' then 'PERMANENT'
		when job_type = 'T' then 'TEMPORARY'
	else 'CONTRACT' end as 'position-type'
--, js.skill
, left(
	concat('Job External ID: REBEL',j.job_ref,char(10),char(10)
	, iif(loc.locationName = '' or loc.locationName is null,'', concat('Address: ',replace(replace(loc.locationName,',,',','),', ,',','),char(10),char(10)))
	, iif(j.job_location = 0,'', iif(loc1.parent_ref = 0, concat('Location: ',loc1.description,char(10),char(10)), concat('Location: ',loc1.description, ', ', loc2.description,char(10),char(10))))
	, iif(js.skill = '' or js.skill is null,'', concat('Skills: ',js.skill,char(10),char(10)))
	, iif(job_rate = '' or job_rate = 'null','', concat('Rate: ',job_rate,char(10),char(10)))
	, iif(job_benefits = '' or job_benefits = 'null','', concat('Benefits: ',job_benefits,char(10),char(10)))
	, concat('Status: ',status_type,char(10),char(10))
	, iif(j.job_notes = '' or j.job_notes is NULL,'',Concat(char(10),'Notes: ',char(10),j.job_notes))),32000)
	 as 'position-note'
from jobs j left join dup on j.job_ref = dup.job_ref
				left join owners como on j.job_consult = como.consult_ref
				left join loc on j.job_ref = loc.job_ref
				left join status s on j.job_status = s.status_ref
				left join location loc1 on j.job_location = loc1.loc_ref
				left join location loc2 on loc1.parent_ref = loc2.loc_ref	
				left join job_skill js on j.job_ref = js.job_ref
--where j.job_ref = 517	
order by convert(int, j.job_ref)