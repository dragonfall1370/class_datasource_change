--SWITCH TO NEW IND / FE/SFE FOR WORK HISTORY
with industry_mapping as (select *
		, row_number() over(partition by vc_ind_id order by vc_new_ind_id, vc_sub_ind_id desc) as rn
		from mike_tmp_vc_2_vc_new_ind
		)
		
, fesfe_mapping as (select *
		, row_number() over(partition by vc_fe_id, vc_sfe_id order by vcfeid, vcsfeid desc) as rn
		from mike_tmp_vc_2_vc_new_fe_sfe
		)

select candidate_id, company, job_title, current_employer, industry, sub_industry
, m.vc_new_ind_id as new_industry
, m.vc_sub_ind_id as new_sub_industry
, functional_expertise_id
, sub_function_id
, cb_employer
, current_employer_id
, start_date
, end_date
, address
, m2.vcfeid as new_fe
, m2.vcsfeid as new_sfe
, cwh.index
into mike_tmp_candidate_work_history_new_ind_fe_sfe
from candidate_work_history cwh
left join (select * from industry_mapping where rn = 1) m on m.vc_ind_id = cwh.industry
left join (select * from fesfe_mapping where rn = 1) m2 on m2.vc_fe_id = cwh.functional_expertise_id and m2.vc_sfe_id = cwh.sub_function_id
where candidate_id in (select id from candidate where (external_id is NULL or external_id not ilike 'CDT%'))


---BUILD NEW JSON WORK HISTORY
with cand_work_history_new as (SELECT candidate_id
			, json_agg(row_to_json((
					SELECT ColumnName 
						from (select company, job_title, current_employer, new_industry::varchar, new_sub_industry::varchar, new_fe::varchar, new_sfe::varchar
									, cb_employer, current_employer_id::varchar, start_date::varchar, end_date::varchar, address) 
						as ColumnName ("company", "jobTitle", "currentEmployer", "industry", "subIndustry", "functionalExpertiseId", "subFunctionId", "cbEmployer", "currentEmployerId", "dateRangeTo", "dateRangeFrom", "address")
					))  order by index asc) AS json
FROM mike_tmp_candidate_work_history_new_ind_fe_sfe
where 1=1
--and candidate_id = 40910 --40910, 105886
--and candidate_id in (select id from candidate where (external_id is NULL or external_id not ilike 'CDT%'))
GROUP BY candidate_id
)

update candidate c
set experience_details_json = cn.json
from cand_work_history_new cn
where c.id = cn.candidate_id