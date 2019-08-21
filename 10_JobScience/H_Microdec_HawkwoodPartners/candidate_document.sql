SELECT
parent_object_ref candidate_id,
CASE
	WHEN file_extension IS NOT NULL THEN CONCAT(parent_object_ref, '.', l.linkfile_ref, '.', file_extension) 
	ELSE CONCAT(parent_object_ref, '.', l.linkfile_ref) 
END document_name,
file_extension,
parent_object_name,
l.displayname,
'CANDIDATE' entity_type,
'resume' document_type
FROM linkfile l
JOIN person p ON l.parent_object_ref = p.person_ref
WHERE parent_object_name = 'person'