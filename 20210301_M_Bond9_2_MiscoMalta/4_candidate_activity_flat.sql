--CANDIATE ACTIVITY FLATTEN BY NOTES
select a.UniqueID as activity_ext_id
  , "field 2" as com_ext_id
  , "field 1" as cand_ext_id
	, "field 3" as job_ext_id
	, f17."72 email add alphanumeric" as user_email
	, -10 as default_user_id
	, 'comment' as category
	, to_timestamp(concat_ws(' ', "post date", "post time"), 'DD/MM/YY HH24:MI') as insert_timestamp
	, c94.code
	, c94.description 
	, concat_ws(chr(10)
			, coalesce('Action: [' || c94.code || ']' || c94.description, NULL)
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
			, concat_ws(chr(10)
					, coalesce('Email note: ' || a2."21 email note", NULL)
					, coalesce('Subject note: ' || a2."22 subject note", NULL)
					, coalesce('Template note: ' || a2."23 template note", NULL)
					)
			, concat_ws(chr(10)
					, coalesce('Title note: ' || a3."8 cons title note", NULL)
					, coalesce('RC note: ' || a3."20 rc note note", NULL)
					)
		) as activity_comment
from act as a
	join (select * from f01 where "101 candidate codegroup  23" = 'Y') as b on b.UniqueID = a."field 1" --candidate
left join (select * from codes where codegroup = '94') as c94 on c94.code = a."field 5"
left join F17 on f17.UniqueID = a."field 4"
left join (SELECT uniqueid, "1 name alphanumeric" from f02) as com on com.uniqueid = a."field 2" --company
left join f03 as job on job.uniqueid = a."field 3" --job
left join act_spe a2 on a2."spe uniqueid" = a.uniqueid
left join act_spc a3 on a3."spc uniqueid" = a.uniqueid
where 1=1
--and c94.description not in ('Batch F/U', 'CV F/Up', 'Int F/Up', 'F/Up PP')