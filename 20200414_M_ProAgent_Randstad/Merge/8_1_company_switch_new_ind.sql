--BACKUP COMPANY INDUSTRY
select *
into mike_tmp_company_industry_20200416
from company_industry
where insert_timestamp < '2020-02-19'


--MAIN SCRIPT
with com_industry as (select distinct m.vc_new_ind_id --industry ID
	, ci.company_id
	--, row_number() over(partition by ci.company_id order by vc_new_ind_id) - 1 as seq
	from company_industry ci
	join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = ci.industry_id
	where ci.insert_timestamp < '2020-02-19'
	
	UNION ALL
	select distinct m.vc_sub_ind_id --sub industry ID
	, ci.company_id
	--, row_number() over(partition by ci.company_id order by vc_new_ind_id, vc_sub_ind_id) - 1 as seq
	from company_industry ci
	join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = ci.industry_id
	where ci.insert_timestamp < '2020-02-19' --old company industry
	--order by ci.insert_timestamp --6375
) --select * from com_industry --11807

insert into company_industry (industry_id, company_id, insert_timestamp)
select distinct vc_new_ind_id industry_id
, company_id company_id
, current_timestamp insert_timestamp
from com_industry
on conflict on constraint company_industry__pkey
	do nothing


--MAPPING SCRIPT
select ci.industry_id
, ci.company_id
, m.vc_new_ind_id
, m.vc_new_ind_ja
, m.vc_sub_ind_id
, m.vc_sub_ind_name
, row_number() over(partition by ci.company_id order by vc_new_ind_id, vc_sub_ind_id) - 1 as seq
from company_industry ci
join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = ci.industry_id
where ci.insert_timestamp < '2020-02-19' --old company industry
--order by ci.insert_timestamp --6375


--AUDIT CHECK BEFORE CONVERTING
select *
from company_industry
where insert_timestamp < '2020-02-19' --6142

select *
from mike_tmp_vc_2_vc_new_ind
where vc_ind_id = 28765

select *
from mike_tmp_vc_2_vc_new_fe_sfe

select count(*), max(insert_timestamp)
from company_industry

select *
from vertical
where parent_id is not NULL

select *
from company_industry
where insert_timestamp between '2020-04-06' and '2020-04-11'