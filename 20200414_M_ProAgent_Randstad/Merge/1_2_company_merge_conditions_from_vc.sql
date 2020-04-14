/* CREATE TEMP TABLE FOR COMPANIES
CREATE TABLE mike_tmp_pa_company_merged
(company_id bigint
, com_ext_id character varying (1000)
, company_name character varying (1000)
, reg_date timestamp
, update_date timestamp
, update_by character varying (1000)
, update_by_user character varying (1000)
, company_owner_id character varying (1000)
, company_owner character varying (1000)
)

select * from mike_tmp_pa_company_merged
*/

with pa_company_rn as (select c.id
		, lower(trim(replace(replace(company_name, N'　', ''), ' ', ''))) pa_company_name
		, row_number() over(partition by lower(trim(replace(replace(company_name, N'　', ''), ' ', ''))) 
						order by update_date desc, insert_timestamp desc) rn --add pa last_update
		, com_ext_id
		, insert_timestamp
		, update_date
		from company c
		join mike_tmp_pa_company_merged m on m.company_id = c.id
		where deleted_timestamp is NULL
		and external_id ilike 'CPY%')

, vc_company as (select id, name
		, position('(' in name) begin_string
		, position(')' in name) end_string
		, insert_timestamp
		, ce.last_activity_date
		from company c
		join company_extension ce on ce.company_id = c.id
		where deleted_timestamp is NULL
		and (external_id is NULL or external_id not ilike 'CPY%'))

--3 cases with wrong company name ID (37906,15424,21300)
, vc_company_name as (select id, name
		, substring(name, begin_string + 1
			, case when end_string < begin_string then length(name) - begin_string - 1
				else end_string - begin_string - 1 end) as vc_company_name
		, insert_timestamp
		, last_activity_date
		from vc_company
		where 1=1
		and begin_string > 0 and end_string > 0
		--and end_string - begin_string < 1
		--and id = 37906
		)
		
, vc_company_name_rn as (select id
		, name vc_origin_company_name
		, lower(trim(replace(replace(vc_company_name, N'　', ''), ' ', ''))) vc_company_name
		, row_number() over(partition by lower(trim(replace(replace(vc_company_name, N'　', ''), ' ', ''))) 
					order by coalesce(last_activity_date, insert_timestamp) desc, coalesce(insert_timestamp, last_activity_date) desc) rn
		--add last_activity_date, if null get reg_date instead
		, insert_timestamp
		, last_activity_date
		from vc_company_name
		where vc_company_name is not NULL and vc_company_name <> ''
		)
--select * from vc_company_name_rn

--MAIN SCRIPT
select pa.id as vc_pa_company_id
, pa.com_ext_id
, pa.pa_company_name
, pa.rn
, pa.insert_timestamp as vc_pa_reg_date
, pa.update_date as vc_pa_update_date
, vc.id as vc_company_id
, vc.vc_company_name
, vc_origin_company_name
, vc.insert_timestamp as vc_reg_date
, coalesce(vc.last_activity_date, vc.insert_timestamp) as vc_latest_date
--into mike_tmp_company_dup_check
from pa_company_rn pa
join (select * from vc_company_name_rn where rn = 1) vc on vc.vc_company_name = pa.pa_company_name --4884 duplicated rows
where 1=1