SELECT
field2 candidate_id,
concat(
	field2,
	'.doc'
) resume,
'CANDIDATE' entity_type,
'resume' document_type
FROM [Candidates Database]
WHERE field2 NOT IN ('681365062', '721299611', '808283641')