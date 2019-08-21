--TASK MAIN SCRIPT
select case when t.task_target_group = 'Company' then concat('PRTR',t.task_target_id)
	else NULL end as CompExtID
--, case when t.task_target_group = 'Contact' then concat('PRTR',t.task_target_id)
--	else NULL end as ContactExtID
--, case when t.task_target_group = 'Candidate' then concat('PRTR',t.task_target_id)
--	else NULL end as CandidateExtID
, case when t.task_target_id in (select cn_id from candidate.Candidates where can_type = 2) then concat('PRTR',t.task_target_id)
	else NULL end as ContactExtID
, case when t.task_target_id in (select cn_id from candidate.Candidates where can_type = 1) then concat('PRTR',t.task_target_id)
	else NULL end as CandidateExtID
, case when t.task_target_group = 'Vacancy' then concat('PRTR',t.task_target_id)
	else NULL end as JobExtID
, left(t.task,200) as Subject_task
, concat_ws(char(10)
	, concat('[Tasks] '
		, coalesce('Task assigned by: ' + nullif(ltrim(rtrim(u.usr_fullname)) + ' - ' + trim(' ' from u.usr_email),''),NULL))
	, coalesce('Task type: ' + nullif(t.tasktype,''),NULL)
	, coalesce('*** Details: ' + nullif(ltrim(t.task),''),'')
	, coalesce('Task ID: ' + convert(varchar(max),t.task_id),'')
	, coalesce('Task created date: ' + nullif(convert(varchar(max),t.task_created),''),NULL)
	, coalesce('Task completed date: ' + nullif(convert(varchar(max),t.task_complete_date),''),NULL)
	) as Task_comment_activities
, t.task_date as Insert_timestamp
, t.task_complete_date as Next_contact_date
, t.task_complete_date as Next_contact_to_date
, 'Asia/Bangkok' as Time_zone --depending on timezone
, 'task' as Category
, case 
	when t.task_target_group = 'Company' then 'company'
	when t.task_target_id in (select cn_id from candidate.Candidates where can_type = 2) then 'contact'
	when t.task_target_id in (select cn_id from candidate.Candidates where can_type = 1) then 'candidate'
	when t.task_target_group = 'Vacancy' then 'job'
	else NULL end as Type
, -10 as User_account_id 
from common.Tasks t
left join users.Users u on t.task_assigned_by = u.usr_id
where t.task_target_group in ('Company','Contact','Candidate','Vacancy')
and t.IsDeleted = 0
and t.task_target_id > 0
--58 rows