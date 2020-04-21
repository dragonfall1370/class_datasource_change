select i.idintelligence
, i.createdon::timestamp as created_date
, ie.entityid as con_ext_id
, concat_ws(chr(10), '[Contact intelligence]'
		, coalesce('Created by: ' || nullif(REPLACE(i.createdby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Entity name: ' || nullif(REPLACE(ie.entityname, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Subject: ' || nullif(REPLACE(i.subject, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Intelligence comment: ' || nullif(REPLACE(i.intelligencecomment, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Informer: ' || nullif(REPLACE(i.informer, '\x0d\x0a', ' '), ''), NULL)
	) description
, cast('-10' as int) as user_account_id
, 'comment' as category
, 'contact' as type
from intelligenceentity ie
left join intelligence i on i.idintelligence = ie.idintelligence
where ie.idtablemd = '9203677c-ee73-4174-8b3a-969056adfe2f' --Person <> f82ea7aa-d88e-4aa8-ba68-211fa4fbfa0e | Company