--#CF: Office in charge | Drop Down | NEW 20200219
select j.[PANO ] as job_ext_id
, j.RS担当オフィス
, case when j.RS担当オフィス not in (select pa_office from LP_Office) then 'Impossible to distribute'
	when j.RS担当オフィス in (select pa_office from LP_Office) then l.vc_office_en
	else NULL end as field_value
, 'add_job_info' as additional_type
, 1003 as form_id
, 11313 as field_id
, current_timestamp as insert_timestamp
from csv_job j
left join LP_Office l on l.pa_office = j.RS担当オフィス and l.category = 'job'
where nullif(j.RS担当オフィス, '') is not NULL


/* NEW VERSION: USED ALL OFFICE IN CHARGE
--#CF: Office in charge | Drop Down
select j.[PANO ] as job_ext_id
, j.RS担当オフィス
, case when j.RS担当オフィス not in (select pa_office from LP_Office where category = 'job') then 'Impossible to distribute'
	when j.RS担当オフィス in (select pa_office from LP_Office where category = 'job') then l.vc_office_en
	else NULL end as job_office
, 'add_job_info' as additional_type
, 1003 as form_id
, 9999 as field_id
from csv_job j
left join LP_Office l on l.pa_office = j.RS担当オフィス and l.category = 'job'
where nullif(j.RS担当オフィス, '') is not NULL
*/