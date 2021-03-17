--MAIN SCRIPT
with jobapp as (select --distinct c94.description --11 mapping
	--distinct c68.description
	uniqueid
	, "4 candidate xref"
	, "6 job id xref"
	, case c94.description
			when 'Shorlist' then 'SHORTLISTED'
			when 'Shortlist' then 'SHORTLISTED'
			when 'Acc/Reject' then 'SHORTLISTED'
			when 'Book Shift' then 'SHORTLISTED'
			when 'CV Sent' then 'SENT'
			when 'Fill Bkg' then 'PLACED' --checking mapping each client | changed from PROD > 'SHORTLISTED'
			when 'Int Feedbk' then 'FIRST_INTERVIEW'
			when 'Interview' then 'FIRST_INTERVIEW'
			when 'Perm Place' then 'PLACED'
			when 'Reject' then 'SHORTLISTED'
			when 'Replace' then 'SHORTLISTED'
			end as stage
	, coalesce(to_date("16 lastactnda date", 'DD/MM/YY'), to_date("11 created date", 'DD/MM/YY')) actioned_date
	, c94.description as sub_status
	, case when c94.description in ('Acc/Reject', 'Reject') then 'rejected'
			else NULL end as rejected_status
	, c94.description as last_stage
	, c68.description as stage_status
	, to_date(f13."31 rejectdte date", 'DD/MM/YY') as rejected_date
	from f13
	left join (select * from codes where codegroup = '94') c94 on c94.code = "15 last actio codegroup  94"
	left join (select * from codes where codegroup = '68') c68 on c68.code = "19 status codegroup  68"
	where 1=1
	and "4 candidate xref" in (select uniqueid from f01 where "101 candidate codegroup  23" = 'Y')
	and "6 job id xref" in (select uniqueid from f03)
	--and c68.description = 'PLACED' --may include different stages
	--and c94.description = 'Fill Bkg' --change mapping to PLACED
	--order by c94.description
	--total 85861
	)

, final_jobapp as (select "4 candidate xref" as cand_ext_id
	, "6 job id xref" as job_ext_id
	, stage
	, actioned_date
	, sub_status
	, stage_status
	, rejected_status
	, rejected_date
	, row_number() over(partition by "4 candidate xref", "6 job id xref"
							order by case stage
								WHEN 'PLACED' THEN 6
								WHEN 'OFFERED' THEN 5
								WHEN 'SECOND_INTERVIEW' THEN 4
								WHEN 'FIRST_INTERVIEW' THEN 3
								WHEN 'SENT' THEN 2
								WHEN 'SHORTLISTED' THEN 1 end desc
							, actioned_date desc) as rn
	from jobapp)
	
select cand_ext_id as "application-candidateExternalId"
, job_ext_id as "application-positionExternalId"
, case when stage = 'PLACED' then 'OFFERED'
	else stage end as "application-stage"
, actioned_date as "application-date"
from final_jobapp
where 1=1
and rn=1
--and stage = 'PLACED'