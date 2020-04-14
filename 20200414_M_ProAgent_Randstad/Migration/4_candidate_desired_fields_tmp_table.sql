--Candidate desired industry | VC temp table
create table mike_tmp_desired_industry(
candidate_id bigint
, industry_id int
, insert_timestamp timestamp
)

--Candidate desired FE/SFE | VC temp table
create table mike_tmp_desired_fe_sfe(
candidate_id bigint
, fe character varying(3000)
, vc_fe character varying(3000)
, vc_fe_id integer
, sfe character varying(3000)
, vc_sfe character varying(3000)
, vc_sfe_id integer
)

-->> BUILD JSON FORMAT <<--

---INDUSTRY---
--Desired Industry --Slow***
select distinct candidate_id
, (select array_to_json(array_agg(row_to_json(value_json)))
  from (select industry_id as "desiredIndustryId"
		from mike_tmp_desired_industry i2
		where i2.candidate_id = i.candidate_id
	   ) as value_json)::text as field_value_json
from mike_tmp_desired_industry i
--where i.candidate_id = 245550

-->> Desired Industry --Fast
SELECT candidate_id, array_to_json(array_agg(ele)) AS json_value
	FROM 
		(
		SELECT candidate_id, json_build_object('desiredIndustryId', industry_id)::json as ele
			FROM mike_tmp_desired_industry
		) tmp 
	GROUP BY candidate_id

---FE/SFE---
--Desired FE/SFE --Slow***
select distinct candidate_id
, (select array_to_json(array_agg(row_to_json(value_json)))
  from (select vc_sfe_id as "desiredSubFunctionId"
		, vc_fe_id as "desiredFunctionalExpertiseId"
		from mike_tmp_desired_fe_sfe i2
		where i2.candidate_id = i.candidate_id
	   ) as value_json)::text as field_value_json
from mike_tmp_desired_fe_sfe i
where i.candidate_id = 220108


-->> Desired FE / SFE --Fast
SELECT candidate_id, array_to_json(array_agg(ele)) AS json_value
	FROM 
		(
		SELECT candidate_id
		, json_build_object('desiredSubFunctionId', vc_sfe_id, 'desiredFunctionalExpertiseId', vc_fe_id)::json as ele
			FROM mike_tmp_desired_fe_sfe
		) tmp 
	GROUP BY candidate_id --101384 rows
	

-->> UPDATE DESIRED INDUSTRY <<--
update candidate c
set desired_industry_json = m.field_value_json::text
from mike_tmp_desired_industry_json m
where m.candidate_id = c.id --46750 rows

-->> UPDATE DESIRED FE/SFE <<--
update candidate c
set desired_functional_expertise_json = m.json_value::text
from mike_tmp_desired_fe_sfe_json m
where m.candidate_id = c.id --101384 rows