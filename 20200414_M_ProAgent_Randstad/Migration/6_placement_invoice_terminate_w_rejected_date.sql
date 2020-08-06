---offer_personal_inf > end_date if not existing
with jobapp as (select id as job_app_id, candidate_id, candidate_id_bkup, status, rejected_date
	from position_candidate
	where 1=1
	and candidate_id_bkup in (select id from candidate where external_id ilike 'CDT%')
	and rejected_date is not NULL
	and status > 300) --188
	
/*--AUDIT OFFER DATA	
, offer_info as (select offer_id, start_date, end_date
	from offer_personal_info
	where offer_id in (select id from offer where position_candidate_id in (select job_app_id from jobapp))
	)
*/

, offer_rejected as (select id as offer_id
	, o.position_candidate_id
	, jobapp.rejected_date
	from offer o
	join jobapp on jobapp.job_app_id = o.position_candidate_id) --select * from offer_rejected --188 rows


--UPDATE OFFER_PERSONAL_INFO > END_DATE
update offer_personal_info opi
set end_date = o.rejected_date
from offer_rejected o
where o.offer_id = opi.offer_id
and opi.end_date is NULL --111 rows


---invoice > status=0 | renewal_flow_status = 2 
with jobapp as (select id as job_app_id, candidate_id, candidate_id_bkup, status, rejected_date
	from position_candidate
	where 1=1
	and candidate_id_bkup in (select id from candidate where external_id ilike 'CDT%')
	and rejected_date is not NULL
	and status > 300) --188
	
--UPDATE INVOICE > status=0 | renewal_flow_status = 2 
update invoice
set status=0 --rejected
, renewal_flow_status = 2 --terminated
from jobapp
where invoice.position_candidate_id = jobapp.job_app_id