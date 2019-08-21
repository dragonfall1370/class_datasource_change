SELECT
CONVERT(VARCHAR(MAX), field2) candidate_id,
CASE
	WHEN TRIM(field26) = 'London' THEN '1'
	WHEN TRIM(field26) = 'Local to Home' THEN '2'
	ELSE '3'
END locality,
'add_cand_info' additional_type,
1005 form_id,
1028 field_id,
'1028' constraint_id,
CURRENT_TIMESTAMP insert_timestamp

FROM [Candidates Database]
WHERE Field26 IS NOT NULL
AND field2 NOT IN ('681365062', '721299611', '808283641')