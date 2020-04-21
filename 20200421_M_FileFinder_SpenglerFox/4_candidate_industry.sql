with split_idindustry_string_list AS (
	SELECT idperson
	, s.idindustry
	FROM personx px, UNNEST(string_to_array(px.idindustry_string_list, ',')) s(idindustry)
	where idindustry_string_list is not NULL
)

SELECT distinct idperson cand_ext_id
, trim(i.value) industry
FROM split_idindustry_string_list sisl
LEFT JOIN industry i ON sisl.idindustry = i.idindustry
where i.value is not NULL