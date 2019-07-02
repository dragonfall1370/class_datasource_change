SELECT
parent_object_ref job_id,
CASE
	WHEN file_extension IS NOT NULL THEN CONCAT(parent_object_ref, '.', l.linkfile_ref, '.', file_extension) 
	ELSE CONCAT(parent_object_ref, '.', l.linkfile_ref) 
END document_name,
file_extension,
parent_object_name,
l.displayname,
'POSITION' entity_type,
'documents' document_type
FROM linkfile l
JOIN opportunity o ON l.parent_object_ref = o.opportunity_ref
WHERE parent_object_name = 'opportunity'