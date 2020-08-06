--TEMP table for new PA-VC industry
create table mike_tmp_pa_industry2 (
pa_name character varying(3000)
, vc_new_ind_id int
, vc_ind_name character varying(3000) --industry
, pa_sub_ind character varying(3000)
, vc_sub_ind_id int
, vc_new_sub_ind character varying(3000) --sub_industry
, rn character varying(3000)
, pa_rn int
)

--MAIN SCRIPT | temp work history for building json
with wh_all as (select cand_ext_id
	, candidate_id
	, jobTitle
	, currentEmployer
	, NULL industry
	, NULL functionalExpertiseId
	, NULL subFunctionId
	, dateRangeFrom::date as dateRangeFrom
	, dateRangeTo::date as dateRangeTo
	, company
	, rn::int rn
	, 1 as rn_index
	from mike_tmp_wh_all
	where rn::int = 1
	
	UNION ALL
	select cand_ext_id
	, candidate_id
	, jobTitle
	, origin_employer as currentEmployer
	, NULL industry
	, NULL functionalExpertiseId
	, NULL subFunctionId
	, dateRangeFrom::date as dateRangeFrom
	, dateRangeTo::date as dateRangeTo
	, company
	, rn::int rn
	, 2 as rn_index
	from mike_tmp_wh_all
	where rn::int > 1) --select * from wh_all
	
, fe_sfe as (select fe.name as fe
	, fe.id as fe_id
	, sfe.name as sfe
	, sfe.id as sfe_id
	from sub_functional_expertise sfe
	left join functional_expertise fe on fe.id = sfe.functional_expertise_id --order by fe
	) 
	
, wh_fe_sfe as (select cand_ext_id
	, candidate_id
	, jobTitle
	, currentEmployer
	, dateRangeFrom::date as dateRangeFrom
	, dateRangeTo::date as dateRangeTo
	, company
	, v.vc_ind_name as vc_industry
	, v.vc_new_ind_id as industry
	, v.vc_new_sub_ind as vc_sub_industry
	, v.vc_sub_ind_id as subIndustry
	, vc_fe
	, fs.fe_id as functionalExpertiseId
	, vc_sfe
	, fs.sfe_id as subFunctionId
	, row_number() over(partition by candidate_id order by v.vc_new_ind_id, v.vc_sub_ind_id, fs.fe_id, fs.sfe_id) + 2 as rn
	, 3 as rn_index
	from (select * from mike_tmp_wh_industry_fe_sfe where coalesce(origin_industry, vc_sfe) is not NULL) m
	left join (select * from mike_tmp_pa_industry2 where pa_rn = 1) v on v.pa_name = m.origin_industry --update row_number with priority industry
	left join fe_sfe fs on fs.fe = m.vc_fe and fs.sfe = m.vc_sfe
	where 1=1
) --select * from wh_fe_sfe

, mike_tmp_final_wh_all as (
	select cand_ext_id
	, candidate_id
	, jobTitle
	, currentEmployer
	, industry
	, NULL subIndustry
	, functionalExpertiseId
	, subFunctionId
	, dateRangeFrom
	, dateRangeTo
	, company
	, rn
	, rn_index
	from wh_all
	
	UNION ALL
	
	select cand_ext_id
	, candidate_id
	, jobTitle
	, currentEmployer
	, industry::text
	, subIndustry::text
	, functionalExpertiseId::text as functionalExpertiseId
	, subFunctionId::text as subFunctionId
	, dateRangeFrom
	, dateRangeTo
	, company
	, rn
	, rn_index
	from wh_fe_sfe)

select cand_ext_id
	, candidate_id
	, jobTitle
	, currentEmployer
	, industry
	, subIndustry
	, functionalExpertiseId
	, subFunctionId
	, dateRangeFrom
	, dateRangeTo
	, company
	, rn
	, rn_index
	, row_number() over(partition by candidate_id order by rn_index asc, rn asc) as rn_rn_index
--into mike_tmp_final_wh_all3
from mike_tmp_final_wh_all
where 1=1
and candidate_id in (115536, 115148)


---BULDING JSON AND RUNNING FROM SPOON---
SELECT candidate_id
, json_agg(row_to_json((
        SELECT ColumnName 
			FROM ( SELECT company, jobtitle, currentemployer, industry, subIndustry, functionalexpertiseid, subfunctionid, daterangefrom, daterangeto ) 
            AS ColumnName (company, "jobTitle", "currentEmployer", industry, "subIndustry", "functionalExpertiseId", "subFunctionId", "dateRangeFrom", "dateRangeTo")
        ))  order by rn_rn_index asc ) AS cand_work_history_json
--into mike_tmp_final_wh_all_json --#TEMP TABLE
FROM mike_tmp_final_wh_all2
where candidate_id in (115536, 115148)
GROUP BY candidate_id --running from spoon