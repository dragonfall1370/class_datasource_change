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
	and "6 job id xref" in (select uniqueid from f03 where "1 job ref numeric" = '6612')
	--and c68.description = 'PLACED' --may include different stages
	--and c94.description = 'Fill Bkg' --change mapping to PLACED
	--order by c94.description
	--total 85861
	)
	
	select ja.uniqueid
	, f01."1 name alphanumeric" as candidate_name
	, f03."3 position alphanumeric" as job_title
	, ja.stage as VC_stage
	, ja.actioned_date
	, ja.sub_status
	, ja.last_stage
	from jobapp ja
	left join f01 on f01.uniqueid = ja."4 candidate xref"
	left join f03 on f03.uniqueid = ja."6 job id xref"
	
	
--CHECK JOB APP MAPPING STATUS
select distinct c94.description
, c68.description
, c30.description
from f13
	left join (select * from codes where codegroup = '94') c94 on c94.code = "15 last actio codegroup  94"
	left join (select * from codes where codegroup = '68') c68 on c68.code = "19 status codegroup  68"
	left join (select * from codes where codegroup = '30') c30 on c30.code = "28 inttype codegroup  30"
	order by c94.description, c68.description, c30.description