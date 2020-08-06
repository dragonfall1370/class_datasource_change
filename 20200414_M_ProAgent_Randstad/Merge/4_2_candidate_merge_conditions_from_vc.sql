/* CREATE TEMP TABLE FOR CANDIDATES
CREATE TABLE mike_tmp_pa_candidate_merged
(candidate_id bigint
, cand_ext_id character varying (1000)
, reg_date timestamp
, update_date timestamp
, primary_email character varying (1000)
, work_email character varying (1000)
, update_by character varying (1000)
, update_by_user character varying (1000)
, candidate_owner_id character varying (1000)
, candidate_owner character varying (1000)
, registration_route character varying (1000)
)

select * from mike_tmp_pa_candidate_merged
*/

with pa_candidate as (select id, email
		, substring(email, position('_' in email) + 1, length(email)) as pa_email
		, m.primary_email pa_email_original
		, insert_timestamp, external_id
		, m.update_date
		from candidate c
		join mike_tmp_pa_candidate_merged m on m.candidate_id = c.id
		where deleted_timestamp is NULL
		and external_id ilike 'CDT%'
		and m.primary_email is not NULL --PA candidate not having primary email
		)

, pa_candidate_rn as (select id
		, lower(trim(pa_email)) pa_email
		, row_number() over(partition by lower(trim(pa_email)) order by update_date desc, insert_timestamp desc) rn --add pa last_update
		, external_id
		, insert_timestamp
		, update_date
		from pa_candidate) --select * from pa_candidate_rn | 157788

, vc_candidate as (select id, email, first_name, last_name
		, substring(email, position(']' in email) + 1, length(email)) as vc_email
		, insert_timestamp, external_id
		, ce.last_activity_date
		from candidate c
		join candidate_extension ce on ce.candidate_id = c.id
		where deleted_timestamp is NULL
		and (external_id is NULL or external_id not ilike 'CDT%')
		and email ilike '%_@_%.__%') --select * from vc_candidate | 77636
		
, vc_candidate_rn as (select id
		, lower(trim(vc_email)) vc_email
		, row_number() over(partition by lower(trim(vc_email)) order by coalesce(last_activity_date, insert_timestamp) desc, insert_timestamp desc) rn
		--add last_activity_date, if null get reg_date instead
		, insert_timestamp
		, last_activity_date
		from vc_candidate
		where vc_email ilike '%_@_%.__%') --select * from vc_candidate_rn
--select * from vc_candidate_rn

--MAIN SCRIPT
select pa.id as vc_pa_candidate_id
, pa.external_id as cand_ext_id
, pa.pa_email
, pa.rn
, pa.insert_timestamp as vc_pa_reg_date
, pa.update_date as vc_pa_update_date
, coalesce(pa.update_date, pa.insert_timestamp) as vc_pa_latest_date
, vc.id as vc_candidate_id
, vc.vc_email
, vc.insert_timestamp as vc_reg_date
, vc.last_activity_date
, coalesce(vc.last_activity_date, vc.insert_timestamp) as vc_latest_date
--into mike_tmp_candidate_dup_check
from pa_candidate_rn pa
join (select * from vc_candidate_rn where rn = 1) vc on vc.vc_email = pa.pa_email