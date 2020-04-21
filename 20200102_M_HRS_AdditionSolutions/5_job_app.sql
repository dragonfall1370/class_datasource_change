with jobapp as (select e.__pk as job_app_id
		, e._fk_candidate_list
		, e._fk_job
		, e.colour_code
		, case when j.contract_type = 'Permanent' then 301
				when j.contract_type = 'Temp' then 302
				else 301 end job_type
		, convert(datetime, e.stamp_created, 103) as actioned_date
		, row_number() over(partition by e._fk_candidate_list, e._fk_job 
			order by case e.colour_code
				when 'Agency Interview' then '3'
				when 'Candidate Rejected' then '0'
				when 'Candidate Withdrew' then '0'
				when 'Closed' then '1'
				when 'CV Sent' then '2'
				when 'External Interview 1' then '3'
				when 'External Interview 2' then '4'
				when 'Hold/Rejected' then '1'
				when 'Interview Other' then '3'
				when 'Offer Accepted' then '5'
				when 'Offer Made' then '5'
				when 'Offer Rejected' then '5'
				when 'Offer Sent' then '5'
				when 'Other' then '0'
				when 'Placed' then '6'
				when 'Telephone Interview' then '3'
				else 0 end desc, e.stamp_created desc) as rn
		from [20191030_155243_events] e
		inner join [20191030_155620_jobs] j on j.__pk = e._fk_job
		where e._fk_candidate_list is not NULL AND e._fk_job is not NULL
		and e._fk_candidate_list in (select __pk from [20191030_153350_contacts] where type = 'Candidate')
		)

--MAIN SCRIPT
select job_app_id
	, concat('AS', _fk_candidate_list) as cand_ext_id
	, concat('AS', _fk_job) as job_ext_id
	, colour_code as sub_stage
	, job_type
	, case colour_code
		when 'Agency Interview' then 'FIRST_INTERVIEW'
		when 'Closed' then 'SHORTLISTED'
		when 'CV Sent' then 'SENT'
		when 'External Interview 1' then 'FIRST_INTERVIEW'
		when 'External Interview 2' then 'SECOND_INTERVIEW'
		when 'Hold/Rejected' then 'SHORTLISTED'
		when 'Interview Other' then 'FIRST_INTERVIEW'
		when 'Offer Accepted' then 'OFFERED'
		when 'Offer Made' then 'OFFERED'
		when 'Offer Rejected' then 'OFFERED'
		when 'Offer Sent' then 'OFFERED'
		when 'Placed' then 'OFFERED'
		when 'Telephone Interview' then 'FIRST_INTERVIEW'
		else NULL end as stage
	, convert(date, actioned_date, 120) as actioned_date
from jobapp ja
where rn = 1
and colour_code not in ('Candidate Rejected', 'Candidate Withdrew', 'Other')