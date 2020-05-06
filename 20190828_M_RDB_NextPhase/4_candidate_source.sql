--CANDIDATE SOURCE BASED ON MAPPING
select concat('NP',a.ApplicantId) as cand_ext_id
, s.SourceId, s.SystemCode
, s.Description
, case s.Description
	when 'CV - Library' then 'CV Library'
	when 'CV-Library CV' then 'CV Library CV search'
	when 'Emed' then 'Emed'
	when 'Emed CV' then 'Emed CV search'
	when 'Facebook' then 'Facebook'
	when 'Glassdoor' then 'Glassdoor'
	when 'Indeed' then 'Indeed' --existing
	when 'Indeed CV' then 'Indeed CV search'
	when 'Jobsite' then 'Jobsite'
	when 'Jobsite CV' then 'Jobsite CV search'
	when 'LinkedIn Advert' then 'LinkedIn' --existing
	when 'LinkedIn Profile' then 'LinkedIn' --existing
	when 'LinkedIn Recruiter' then 'LinkedIn' --existing
	when 'Next Phase Website' then 'Next Phase Website'
	when 'Prof Passport' then 'Prof Passport'
	when 'Reed' then 'Reed'
	when 'Reed CV' then 'Reed CV search'
	when 'Referral' then 'Referral' --existing
	when 'Shane' then 'Shane'
	when 'Total Jobs' then 'Totaljobs'
	when 'Totaljobs CV' then 'Totaljobs CV search'
	else 'Legacy' end as candidate_source
from Applicants a
left join dbo.Sources s on s.SourceId=a.SourceId