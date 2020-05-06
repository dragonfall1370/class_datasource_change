--APPLICANT ACTIONS ACTIVITIES
with userinfo as (
        select u.UserId,u.LoginName,u.UserName,u.UserFullName,u.JobTitle,u.Inactive
        from Users u)

select coalesce('NP'+ convert(varchar(max), aa.ApplicantId),NULL) as cand_ext_id
, coalesce('NP'+ convert(varchar(max), aa.JobId),NULL) as job_ext_id
, coalesce('NP'+ convert(varchar(max), aa.ClientContactId),NULL) as con_ext_id
, coalesce('NP'+ convert(varchar(max), aa.ClientId),NULL) as com_ext_id
, -10 as user_account_id
, aa.createdOn
, 'comment' as category
, concat_ws(char(10), '[Applicant Actions]'
	, coalesce('Consultants: '+ u.UserFullName,NULL)
	, coalesce('Created by: '+ u2.UserFullName,NULL)
	, coalesce('Status: '+ aas.Description,NULL)
	, coalesce('Status Date: '+ convert(varchar(max), aa.StatusDate, 120),NULL)
	, coalesce('Notes: '+ aa.Notes,NULL)
	) as comment_activities
from ApplicantActions aa
left join ApplicantActionStatus aas on aas.ApplicantActionStatusId = aa.StatusId
left join userinfo u on u.UserId = aa.ConsultantUserId --consultants
left join userinfo u2 on u2.UserId = aa.CreatedUserId --created
--where aa.ApplicantActionId = 113