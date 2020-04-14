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
, vc_sfe_id int
)

--INJECT DATA TO VINCERE TEMP TABLE

SELECT candidate_id
, json_agg(row_to_json((
        SELECT ColumnName 
			FROM (SELECT industry_id) 
            AS ColumnName ("desiredIndustryId")
        ))) AS json_value
--into mike_tmp_final_wh_all_json --#TEMP TABLE
FROM mike_tmp_desired_industry
--where candidate_id = 264827
GROUP BY candidate_id

-->> UPDATE DESIRED INDUSTRY <<--
update candidate c
set desired_industry_json = m.json_value::text
from mike_tmp_desired_industry_json m
where m.candidate_id = c.id --46750 rows



--FE/SFE
SELECT candidate_id
, json_agg(row_to_json((
        SELECT ColumnName 
			FROM ( SELECT vc_sfe_id, vc_fe_id) 
            AS ColumnName ("desiredSubFunctionId", "desiredFunctionalExpertiseId")
        ))  order by vc_fe_id, vc_sfe_id) AS json_value
--into mike_tmp_final_wh_all_json --#TEMP TABLE
FROM mike_tmp_desired_fe_sfe
--where candidate_id = 264827
GROUP BY candidate_id


-->> UPDATE DESIRED FE/SFE <<--
update candidate c
set desired_functional_expertise_json = m.json_value::text
from mike_tmp_desired_fe_sfe_json m
where m.candidate_id = c.id --101384 rows