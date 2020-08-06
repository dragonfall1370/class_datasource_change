with 
--Due to 1 VC mapping links to multiple sub Industry, the priority sub will be selected
industry_mapping as (select *
		, row_number() over(partition by vc_ind_id order by vc_sub_ind_id desc) as rn
		from mike_tmp_vc_2_vc_new_ind
		)

--UPDATE CANDIDATE WORK HISTORY
update candidate_work_history cwh
set industry = m.vc_new_ind_id
, sub_industry = m.vc_sub_ind_id
from industry_mapping m
where cwh.industry = m.vc_ind_id
and m.rn = 1
and cwh.candidate_id in (select id from candidate where (external_id is NULL or external_id not ilike 'CDT%'))


/* CHECKING
--COUNT NO OF RECORDS 
select id from candidate_work_history 
where candidate_id in (select id from candidate where (external_id is NULL or external_id not ilike 'CDT%')) --178272
and industry is not NULL

--This can be used if update work history json directly
, cand_work_history_mapping as (select candidate_id, company, job_title, current_employer, industry::varchar, sub_industry::varchar
		, m.vc_new_ind_id as new_industry
		, m.vc_sub_ind_id as new_sub_industry
		, functional_expertise_id::varchar, sub_function_id::varchar
		, cb_employer, current_employer_id::varchar, start_date::varchar, end_date::varchar, address
		, cwh.index
		from candidate_work_history cwh
		left join (select * from industry_mapping where rn = 1) m on m.vc_ind_id = cwh.industry
		where cwh.candidate_id in (select id from candidate where (external_id is NULL or external_id not ilike 'CDT%'))
		and industry is not NULL
		--and candidate_id = 105886 ----cwh.candidate_id = 40910
)

SELECT candidate_id
			, json_agg(row_to_json((
					SELECT ColumnName 
						from (select company, job_title, current_employer, new_industry::varchar, new_sub_industry::varchar, functional_expertise_id::varchar, sub_function_id::varchar
									, cb_employer, current_employer_id::varchar, start_date::varchar, end_date::varchar, address) 
						as ColumnName ("company", "jobTitle", "currentEmployer", "industry", "subIndustry", "functionalExpertiseId", "subFunctionId", "cbEmployer", "currentEmployerId"
													, "dateRangeTo", "dateRangeFrom", "address")
					))  order by index asc) AS json
FROM cand_work_history_mapping
where candidate_id = 105886 --40910
GROUP BY candidate_id

select candidate_id, index, count(*) 
from cand_work_history_mapping
group by candidate_id, index
having count(*) > 1
*/

/* BACKUP WORK HISTORY
select *
into mike_tmp_candidate_work_history_20200424
from candidate_work_history

select id, first_name, last_name, email, external_id, insert_timestamp, deleted_timestamp, experience_details_json
into mike_tmp_experience_details_json_20200424
from candidate --242030
*/

--UPDATE CANDIDATE WORK HISTORY JSON
---Running from spoon for faster update
with cand_work_history_new as (SELECT candidate_id
			, json_agg(row_to_json((
					SELECT ColumnName 
						from (select company, job_title, current_employer, industry::varchar, sub_industry::varchar, functional_expertise_id::varchar, sub_function_id::varchar
									, cb_employer, current_employer_id::varchar, start_date::varchar, end_date::varchar, address) 
						as ColumnName ("company", "jobTitle", "currentEmployer", "industry", "subIndustry", "functionalExpertiseId", "subFunctionId", "cbEmployer", "currentEmployerId"
										, "dateRangeTo", "dateRangeFrom", "address")
					))  order by index asc) AS json
FROM candidate_work_history
where 1=1
--and candidate_id = 40910 --40910, 105886
and candidate_id in (select id from candidate where (external_id is NULL or external_id not ilike 'CDT%'))
GROUP BY candidate_id
)

update candidate c
set experience_details_json = cn.json
from cand_work_history_new cn
where c.id = cn.candidate_id
