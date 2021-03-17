select f02.uniqueid as com_ext_id
--, f02."11 created by xref"
--, f02."9 created date"
--, f02."10 updated date"
--, f02."12 updated by xref"
--, f02."17 last p job date"
--, f02."19 last t job date"
--, f02."70 last cont date"
--, f02."79 tob date date"
--, f02."86 last con date"
--, f02."58 lpermj by xref"
--, f02."59 ltempj by xref"
, concat_ws(chr(10)
	, coalesce('Created date: ' || f02."9 created date", NULL)
	, coalesce('Created by: ' || f17."1 name alphanumeric" || ' - ' || f17."72 email add alphanumeric", NULL)
	, coalesce('Updated: ' || f02."10 updated date", NULL)
	, coalesce('Updated by: ' || u2."1 name alphanumeric" || ' - ' || u2."72 email add alphanumeric", NULL)
	, coalesce('Last permanent job: ' || f02."10 updated date", NULL)
	, coalesce('Last permanent job by: ' || u4."1 name alphanumeric" || ' - ' || u4."72 email add alphanumeric", NULL)
	, coalesce('Last temp job: ' || f02."10 updated date", NULL)
	, coalesce('Last temp job by: ' || u5."1 name alphanumeric" || ' - ' || u5."72 email add alphanumeric", NULL)
	, coalesce('Last contract date: ' || f02."86 last con date", NULL)
	, coalesce('Contract by: ' || u3."1 name alphanumeric" || ' - ' || u3."72 email add alphanumeric", NULL)
	, coalesce('Last contact date: ' || f02."70 last cont date", NULL)
	, coalesce('Last TOB: ' || f02."79 tob date date", NULL)
	) as activity_comment
, -10 as user_id
, 'company' as type
, 'comment' as category
, current_timestamp as insert_timestamp
from f02
left join f17 on f17.uniqueid = f02."11 created by xref"
left join f17 u2 on u2.uniqueid = f02."12 updated by xref"
left join f17 u3 on u3.uniqueid = f02."87 contr by xref"
left join f17 u4 on u4.uniqueid = f02."58 lpermj by xref"
left join f17 u5 on u5.uniqueid = f02."59 ltempj by xref"