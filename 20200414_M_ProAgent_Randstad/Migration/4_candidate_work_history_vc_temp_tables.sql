--Merge candidate work history (latest) with wh industry/fe/sfe | Guideline
---Candidate work history tmp table
create table mike_tmp_wh_all(
candidate_id bigint
, cand_ext_id character varying(3000)
, currentEmployer character varying(3000)
, origin_employer character varying(3000)
, jobTitle character varying(3000)
, dateRangeFrom character varying(3000)
, dateRangeTo character varying(3000)
, company character varying
, rn character varying(3000)
)
---PA SCRIPT
select * from cand_work_history --174850 | 172177

---Candidate work history with industry/fe/sfe tmp table
create table mike_tmp_wh_industry_fe_sfe(
candidate_id bigint
, cand_ext_id character varying(3000)
, currentEmployer character varying(3000)
, jobTitle character varying(3000)
, origin_industry character varying(3000)
, industry character varying(3000)
, origin_fe character varying(3000)
, vc_fe character varying(3000)
, origin_sfe1 character varying(3000)
, vc_sfe character varying(3000)
, dateRangeFrom character varying(3000)
, dateRangeTo character varying(3000)
, company character varying
)
---PA SCRIPT
select *
from cand_work_history_fe_sfe
where 1=1
and coalesce(industry, vc_sfe) is not NULL --453227 | 435480

--Inject data to 2 temp tables
/* Sample work history JSON FORMAT
{
    "company": "",
    "jobTitle": "QA Manager",
    "currentEmployer": "Virgin Media/IBM/Adecco",
    "industry": "28746",
    "functionalExpertiseId": "3097",
    "subFunctionId": "46",
    "cbEmployer": "1",
    "currentEmployerId": "174325",
    "dateRangeFrom": "2019-10-18",
    "address": "",
    "dateRangeTo": ""
  }
*/

-->> MAIN SCRIPT <<--
with wh_all as (select cand_ext_id
	, candidate_id
	, jobTitle
	, currentEmployer
	, '' industry
	, '' functionalExpertiseId
	, '' subFunctionId
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
	, '' industry
	, '' functionalExpertiseId
	, '' subFunctionId
	, dateRangeFrom::date as dateRangeFrom
	, dateRangeTo::date as dateRangeTo
	, company
	, rn::int rn
	, 2 as rn_index
	from mike_tmp_wh_all
	where rn::int > 1)

/* Audit check
select * from abc
where cand_ext_id = 'CDT150572'
*/

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
	, coalesce('【PP】' || nullif(m.industry, ''), '') as vc_industry
	, coalesce(v.id::text, '') as industry
	, vc_fe
	, fs.fe_id as functionalExpertiseId
	, vc_sfe
	, fs.sfe_id as subFunctionId
	, row_number() over(partition by candidate_id order by v.id, fs.fe_id, fs.sfe_id) + 2 as rn
	, 3 as rn_index
	from (select * from mike_tmp_wh_industry_fe_sfe where coalesce(industry, vc_sfe) is not NULL) m
	left join vertical v on v.name = concat('【PP】', m.industry)
	left join fe_sfe fs on fs.fe = m.vc_fe and fs.sfe = m.vc_sfe
	where 1=1
)

, mike_tmp_final_wh_all as (
	select cand_ext_id
	, candidate_id
	, jobTitle
	, currentEmployer
	, industry
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
	, industry
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
	, functionalExpertiseId
	, subFunctionId
	, dateRangeFrom
	, dateRangeTo
	, company
	, rn
	, rn_index
	, row_number() over(partition by candidate_id order by rn_index asc, rn asc) as rn_rn_index
--into mike_tmp_final_wh_all
from mike_tmp_final_wh_all
where 1=1
--and candidate_id = 115080 --check case


------BUILDING JSON-------
SELECT candidate_id
, json_agg(row_to_json((
        SELECT ColumnName 
			FROM ( SELECT company, jobtitle, currentemployer, industry, functionalexpertiseid, subfunctionid, daterangefrom, daterangeto ) 
            AS ColumnName (company, "jobTitle", "currentEmployer", industry, "functionalExpertiseId", "subFunctionId", "dateRangeFrom", "dateRangeTo")
        ))  order by rn_rn_index asc ) AS cand_work_history_json
--into mike_tmp_final_wh_all_json --#TEMP TABLE
FROM mike_tmp_final_wh_all
where candidate_id = 264827
GROUP BY candidate_id


---UPDATE VC WORK HISTORY JSON---
update candidate c
set experience_details_json = m.cand_work_history_json::text
--from (SELECT candidate_id
--		, json_agg(row_to_json((
--				SELECT ColumnName 
--					FROM ( SELECT company, jobtitle, currentemployer, industry, functionalexpertiseid, subfunctionid, daterangefrom, daterangeto ) 
--					AS ColumnName (company, "jobTitle", "currentEmployer", industry, "functionalExpertiseId", "subFunctionId", "dateRangeFrom", "dateRangeTo")
--				))  order by rn_rn_index asc ) AS cand_work_history_json
--		FROM mike_tmp_final_wh_all
--		--where candidate_id = 264827
--		GROUP BY candidate_id) m
from mike_tmp_final_wh_all_json m
where m.candidate_id = c.id
and m.candidate_id between 115077 and 200000 --84924 
and m.candidate_id between 200000 and 250000 --50001
and m.candidate_id between 250000 and 286877 --36878

/* GET MIN MAX CANDIDATE ID
select min(id) --115077
, max(id) --286877
from candidate
where external_id ilike 'CDT%'
and deleted_timestamp is NULL
*/

/* NOT ORDER IF BUILDING JSON
SELECT candidate_id, array_to_json(array_agg(ele)) AS json_value
into mike_tmp_final_wh_all_json
	FROM 
		(
		SELECT candidate_id
		, json_build_object('company', company, 'jobTitle', jobTitle, 'currentEmployer', currentEmployer
										, 'industry', industry, 'functionalExpertiseId', functionalExpertiseId, 'subFunctionId', subFunctionId, 'dateRangeFrom', dateRangeFrom, 'dateRangeTo', dateRangeTo)::json as ele
			FROM mike_tmp_final_wh_all
		) tmp 
	GROUP BY candidate_id

select *
from mike_tmp_final_wh_all_json
where candidate_id = 264827
*/