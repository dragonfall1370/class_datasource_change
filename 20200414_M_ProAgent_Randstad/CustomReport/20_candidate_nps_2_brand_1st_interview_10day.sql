with cand_brand as (select tgc.candidate_id
	, tgc.team_group_id
	, tg.name as brand
	from team_group_candidate tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	where team_group_id in (1125, 1123)
	)

/* 
ACTIVE VALUE: active=0-passive 1-active 2-donotcontact 3-blacklist 
MET-NOT MET: status=1-met 2-notmet
*/


--Candidate with >1 job app	
, cand_job_app as (select candidate_id
		, count(id) as job_app_count
		from position_candidate
		group by candidate_id
		having count(*) > 1)
		
--Candidate with >1 job app and setting 1st interview date
, cand_1st_int_date as (select pc.id as job_app_id
		, pc.candidate_id
		, pc.interview1_date
		, row_number() over(partition by pc.candidate_id order by pc.interview1_date desc) as rn --get latest 1st int date
		, pd.name as job_title
		, pd.company_id
		, c.name as company_name
		, pd.contact_id
		, con.first_name
		, con.last_name
		, con.email as contact_primary_email
		from position_candidate pc
		left join position_description pd on pd.id = pc.position_description_id
		left join company c on c.id = pd.company_id
		left join contact con on con.id = pd.contact_id
		where 1=1
		--and pc.candidate_id in (select candidate_id from cand_job_app) --the filter will not be applied
		and pc.interview1_date is not NULL
		) --select * from cand_1st_int_date

--Job app info with interview1_date within date of 10 days ago
, cand_10_day_int as (select *
		from cand_1st_int_date
		where 1=1
		and rn = 1 --get the latest 1st interview date
		and (interview1_date + interval '10 days')::date = (now())::date
		) --select * from cand_10_day_int

--MAIN SCRIPT
select c.id
, c.first_name
, c.last_name
, c.first_name_kana
, c.last_name_kana
, c.email as primary_email
, cand_brand.brand
, case active
		when 0 then 'Active'
		when 1 then 'Passive'
		when 2 then 'Do not contact'
		when 3 then 'Blacklist'
		else NULL end as active
, case status
		when 1 then 'MET'
		when 2 then 'NOT MET'
		else NULL end as met_notmet
, ca.job_title
, ca.company_id
, ca.company_name
, ca.contact_id
, ca.first_name contact_first_name
, ca.last_name contact_last_name
, contact_primary_email
, ca.job_app_id
, ca.interview1_date
from candidate c
join cand_brand on cand_brand.candidate_id = c.id
join cand_10_day_int ca on ca.candidate_id = c.id
where 1=1
and c.deleted_timestamp is NULL
and c.active in (0, 1) --status active or passive
--and c.status in (1) --only MET candidates
order by c.id