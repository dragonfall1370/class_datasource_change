SELECT
parent_object_ref contact_id,
CONCAT(parent_object_ref, '.', l.linkfile_ref, '.', file_extension) document_name,
file_extension,
parent_object_name,
l.displayname,
'CONTACT' entity_type,
'documents' document_type
FROM linkfile l
JOIN "position" p ON l.parent_object_ref = p.person_ref
WHERE parent_object_name = 'contact'