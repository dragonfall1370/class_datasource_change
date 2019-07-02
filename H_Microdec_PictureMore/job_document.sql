SELECT
parent_object_ref job_id,
CONCAT(parent_object_ref, '.', l.linkfile_ref, '.', file_extension) document_name,
file_extension,
parent_object_name,
l.displayname,
'POSITION' entity_type,
'job_description' document_type
FROM linkfile l
JOIN opportunity o ON l.parent_object_ref = o.opportunity_ref
WHERE parent_object_name = 'opportunity'