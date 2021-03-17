select a.UniqueID as activity_external_id
  , "field 2" as com_ext_id
  , "field 1" as con_text_id
	, "field 3" as job_ext_id
	, f17."72 email add alphanumeric" as user_email
	, -10 as default_user_id
	, 'comment' as category
	, to_timestamp(concat_ws(' ', "post date", "post time"), 'DD/MM/YY HH24:MI') as insert_timestamp
	, c94.code
	, c94.description 
	, concat_ws(chr(10)
			, coalesce('Action: ' || c94.description, NULL)
			, coalesce('Created by: ' || f17."1 name alphanumeric" || ' - ' || f17."72 email add alphanumeric", NULL)
			, concat_ws(chr(10)
					, coalesce('[Notes 1]' || ' ' || nullif("notes 1", ''), NULL)
					, coalesce('[Notes 2]' || ' ' || nullif("notes 2", ''), NULL)
					, coalesce('[Notes 3]' || ' ' || nullif("notes 3", ''), NULL)
					, coalesce('[Notes 4]' || ' ' || nullif("notes 4", ''), NULL)
					, coalesce('[Notes 5]' || ' ' || nullif("notes 5", ''), NULL)
					, coalesce('[Notes 6]' || ' ' || nullif("notes 6", ''), NULL)
					, coalesce('[Notes 7]' || ' ' || nullif("notes 7", ''), NULL)
					)
				, case when a."field 5" = 'PC' then
						concat_ws(chr(10)
							, coalesce('Last contact date: ' || a3."9 last cont date", NULL)
							, coalesce('Job: ' || a3."11 job text note", NULL)
							)
					else NULL end
		) as activity_comment
from act as a
	join f02 as b on b.UniqueID = a."field 2"
LEFT JOIN (select * from codes where codegroup = '94') as c94 on c94.code = a."field 5"
LEFT JOIN F17 on f17.UniqueID = a."field 4"
LEFT JOIN (SELECT uniqueid, "1 name alphanumeric" from f01) as con on con.uniqueid = a."field 1"
LEFT JOIN f03 as job on job.uniqueid = a."field 3"
left join act_fn as a2 on a2." fn uniqueid"  = a.UniqueID
left join act_pc as a3 on a3." pc uniqueid" = a.UniqueID
where 1=1
--and [Field 2] is not NULL
and "field 5" in ('PH', 'PC1', 'PC', 'TAE', 'TAP')