--Communications temp table
---#1 Company Split
with t0 as (select __pk as communication_id
	, case when _fk_company_list = 'NO RESULT' then NULL
		else replace(replace(_fk_company_list, '.0', ''), char(11), ',') end as _fk_company_list
	from [20191030_153228_communications]
	)

, t1 as (select communication_id, value as company_id
	from t0
	cross apply string_split(_fk_company_list,','))

, t2 as (select communication_id
	, company_id
	from t1 where company_id <> '') --15831 rows

select __pk as communication_id
	, c._fk_company_list
	, c._fk_consultant
	, c._fk_contact_from
	, c._fk_contact_to_list
	, c._fk_email
	, c._fk_event
	, c._fk_job
	, c.ae_name_from
	, c.ae_name_to
	, email_attachments
	, replace(replace(email_body, char(11), char(10)), concat('Ê', char(10)), '') as email_body
	, email_from_name
	, email_recipient_names
	, email_subject
	, in_or_out
	, convert(datetime, stamp_created, 103) as stamp_created
	, [subject] as communication_subject
	, [type] as communication_type
	, case
	      when t2.communication_id is not null then iif(t2.company_id = 'NO RESULT', NULL, t2.company_id)
	      else coalesce(nullif(_fk_company_list, ''), null)
	      end as company_id
	, c._fk_job as job_id
--into company_split --#temp table
from [20191030_153228_communications] c
left join t2 on t2.communication_id = c.__pk
--where communication_id in (14189) --81167 rows

/* CLEANSE DATA
update communications
set company_id = NULL
where company_id in ('NO RESULT', char(11), '')
*/

/* AUDIT COMPANY SPLIT

select distinct __pk, _fk_company_list
from [20191030_153228_communications]
where len(_fk_company_list) > 10

*/
--COMMUNICATIONS TEMP TABLE
with t0 as (select communication_id
	, case when _fk_contact_to_list = 'NO RESULT' then NULL
		else replace(replace(_fk_contact_to_list, '.0', ''), char(11), ',') end as _fk_contact_to_list
	from company_split
	)

, t1 as (select communication_id, value as contact_id
	from t0
	cross apply string_split(_fk_contact_to_list,','))

, t2 as (select communication_id
	, contact_id
	from t1 where contact_id <> '') --102361 rows

select distinct c.communication_id
	, c._fk_company_list
	, c._fk_consultant
	, c._fk_contact_from
	, c._fk_contact_to_list
	, c._fk_email
	, c._fk_event
	, c._fk_job
	, c.ae_name_from
	, c.ae_name_to
	, email_attachments
	, c.email_body
	, email_from_name
	, email_recipient_names
	, email_subject
	, in_or_out
	, c.stamp_created
	, c.communication_subject
	, c.communication_type
	, c.company_id
	, c._fk_job as job_id
	, case
	      when t2.communication_id is not null then iif(t2.contact_id = 'NO RESULT', NULL, t2.contact_id)
	      else coalesce(nullif(_fk_contact_to_list, ''), null)
	      end as contact_id
--into communications --#inject 161895 rows
from company_split c
left join t2 on t2.communication_id = c.communication_id
--where c.communication_id in (24871)
