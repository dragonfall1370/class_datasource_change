
--COMMENTS MAIN SCRIPT
select case when c.comment_class = 'Company' then concat('PRTR',c.class_parent_id)
	else NULL end as CompExtID
, case when c.class_parent_id in (select cn_id from candidate.Candidates where can_type = 2) then concat('PRTR',c.class_parent_id)
	else NULL end as ContactExtID
, case when c.class_parent_id in (select cn_id from candidate.Candidates where can_type = 1) then concat('PRTR',c.class_parent_id)
	else NULL end as CandidateExtID
, case when c.comment_class = 'Vacancy' then concat('PRTR',c.class_parent_id)
	else NULL end as JobExtID
, c.comment_date as Insert_timestamp
, concat_ws(char(10)
	, concat('[Comments] '
		, coalesce('Commented by: ' + nullif(ltrim(rtrim(u.usr_fullname)) + ' - ' + trim(' ' from u.usr_email),''),NULL))
	, coalesce('*** Comments: ' + nullif(c.comment,''),NULL)
	, coalesce('Comment ID: ' + nullif(convert(varchar(max),c.comment_id),''),NULL)
	, coalesce('Commented on: ' + nullif(convert(varchar(max),c.comment_date),''),NULL)
	) as Comment_activities
, 'comment' as category
, -10 as User_account_id
from common.Comments c
left join users.Users u on c.usr_id = u.usr_id
where c.comment_class in ('Company','Contact','Candidate','Vacancy')
--753172 rows