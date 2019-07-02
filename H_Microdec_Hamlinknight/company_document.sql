SELECT
parent_object_ref company_id,
CONCAT(parent_object_ref, '.', l.linkfile_ref, '.', file_extension) document_name,
file_extension,
parent_object_name,
l.displayname,
'COMPANY' entity_type,
'legal_document' document_type
FROM linkfile l
JOIN organisation o ON l.parent_object_ref = o.organisation_ref
WHERE parent_object_name = 'organisation'