--BACKUP CONTACT INDUSTRY
select *
into mike_tmp_contact_industry_20200417
from contact_industry
where insert_timestamp < '2020-04-14'


--MAIN SCRIPT
with con_industry as (select distinct m.vc_new_ind_id --industry ID
	, ci.contact_id
	--, row_number() over(partition by ci.company_id order by vc_new_ind_id) - 1 as seq
	from contact_industry ci
	join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = ci.industry_id
	
	UNION ALL
	select distinct m.vc_sub_ind_id --sub industry ID
	, ci.contact_id
	--, row_number() over(partition by ci.company_id order by vc_new_ind_id, vc_sub_ind_id) - 1 as seq
	from contact_industry ci
	join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = ci.industry_id --old company industry
) --select * from con_industry --13756

insert into contact_industry (industry_id, contact_id, insert_timestamp)
select distinct vc_new_ind_id industry_id
, contact_id
, current_timestamp insert_timestamp
from con_industry
on conflict on constraint contact_industry__pkey
	do nothing