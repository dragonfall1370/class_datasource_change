--FINAL JOB APPLICATION
---Organize in row_number, with highest stage, latest date
with jobapp as (select cand_ext_id
, job_ext_id
, app_status
, action_date
, rejected_date
, concat(cand_ext_id, job_ext_id) as jobapp_rn
, row_number() over(partition by cand_ext_id, job_ext_id
		order by action_date desc,
				case app_status
				when 'Shortlisted - Pending' then 1
				when 'Shortlisted - Rejected' then 1
				when 'Sent - Pending' then 2
				when 'Sent - Rejected' then 2
				when '1st Interview - Pending' then 3
				when '1st Interview - Rejected' then 3
				when '2nd+ Interview - Pending' then 4
				when '2nd+ Interview - Rejected' then 4
				when 'Offered - received' then 5
				when 'Placed - Starting' then 6
				when 'Placed - Active' then 7
				end desc) as rn
, rank() over(partition by cand_ext_id, job_ext_id
		order by --action_date desc,
				case app_status
				when 'Shortlisted - Pending' then 1
				when 'Shortlisted - Rejected' then 1
				when 'Sent - Pending' then 2
				when 'Sent - Rejected' then 2
				when '1st Interview - Pending' then 3
				when '1st Interview - Rejected' then 3
				when '2nd+ Interview - Pending' then 4
				when '2nd+ Interview - Rejected' then 4
				when 'Offered - received' then 5
				when 'Placed - Starting' then 6
				when 'Placed - Active' then 6
				end desc) as rank_rn
, sub_status
from pa_final_jobapp
where cand_ext_id in (select [PANO ] from csv_can)
and job_ext_id in (select [PANO ] from csv_job)
)

/* Check reference
select *
from jobapp --CDT183041	JOB117535
*/

, jobappfilter as (select *
	, case 
		when app_status in ('Shortlisted - Pending', 'Shortlisted - Rejected') then 'SHORTLISTED'
		when app_status in ('Sent - Pending', 'Sent - Rejected') then 'SENT'
		when app_status in ('1st Interview - Pending', '1st Interview - Rejected') then 'FIRST_INTERVIEW'
		when app_status in ('2nd+ Interview - Pending', '2nd+ Interview - Rejected') then 'SECOND_INTERVIEW'
		when app_status in ('Offered - received') then 'OFFERED'
		when app_status in ('Placed - Starting', 'Placed - Active') then 'PLACED'
		else NULL end as stage
	from jobapp
	where rn = rank_rn
	--and cand_ext_id = 'CDT183041' and job_ext_id = 'JOB117535' --check conditions if ok
)

select ja.cand_ext_id
, ja.job_ext_id
, ja.app_status as original_stage
, case 
		when ja.app_status in ('Shortlisted - Pending', 'Shortlisted - Rejected') then 'SHORTLISTED'
		when ja.app_status in ('Sent - Pending', 'Sent - Rejected') then 'SENT'
		when ja.app_status in ('1st Interview - Pending', '1st Interview - Rejected') then 'FIRST_INTERVIEW'
		when ja.app_status in ('2nd+ Interview - Pending', '2nd+ Interview - Rejected') then 'SECOND_INTERVIEW'
		when ja.app_status in ('Offered - received') then 'OFFERED'
		when ja.app_status in ('Placed - Starting', 'Placed - Active') then 'OFFERED'
		else NULL end as stage
, ja.sub_status
, ja.action_date
, jaf.stage as stage1
, jaf.action_date as shortlisted_date
, jaf.sub_status as sub_status1
, jaf2.stage as stage2
, jaf2.action_date as sent_date
, jaf2.sub_status as sub_status2
, jaf3.stage as stage3
, jaf3.action_date as first_int_date
, jaf3.sub_status as sub_status3
, jaf4.stage as stage4
, jaf4.action_date as second_int_date
, jaf4.sub_status as sub_status4
, jaf5.stage as stage5
, jaf5.action_date as offer_date
, jaf5.sub_status as sub_status5
, jaf6.stage as stage6
, jaf6.action_date as placed_date
, jaf6.sub_status as sub_status6
--vc_final_jobapp --temp table for placement details
from jobapp ja
left join jobappfilter jaf on jaf.jobapp_rn = ja.jobapp_rn and jaf.stage = 'SHORTLISTED'
left join jobappfilter jaf2 on jaf2.jobapp_rn = ja.jobapp_rn and jaf2.stage = 'SENT'
left join jobappfilter jaf3 on jaf3.jobapp_rn = ja.jobapp_rn and jaf3.stage = 'FIRST_INTERVIEW'
left join jobappfilter jaf4 on jaf4.jobapp_rn = ja.jobapp_rn and jaf4.stage = 'SECOND_INTERVIEW'
left join jobappfilter jaf5 on jaf5.jobapp_rn = ja.jobapp_rn and jaf5.stage = 'OFFERED'
left join jobappfilter jaf6 on jaf6.jobapp_rn = ja.jobapp_rn and jaf6.stage = 'PLACED'
where ja.rn = 1 --791749 rows
--and jaf.stage = 'SHORTLISTED'
--and jaf2.stage = 'SENT'
--and jaf3.stage = 'FIRST_INTERVIEW'
--and jaf4.stage = 'SECOND_INTERVIEW'
--and jaf5.stage = 'OFFERED'
--and jaf6.stage = 'PLACED'
--and ja.cand_ext_id = 'CDT183041' and ja.job_ext_id = 'JOB117535' --check sample | 544410