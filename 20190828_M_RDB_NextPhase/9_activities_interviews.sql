--INTERVIEW ACTIVITIES
with userinfo as (
        select u.UserId,u.LoginName,u.UserName,u.UserFullName,u.JobTitle,u.Inactive
        from Users u)

select coalesce('NP'+ convert(varchar(max), nullif(a.ApplicantId,'')),NULL) as cand_ext_id
, coalesce('NP'+ convert(varchar(max), nullif(a.JobId,'')),NULL) as job_ext_id
, coalesce('NP'+ convert(varchar(max), nullif(a.ClientContactId,'')),NULL) as con_ext_id
, coalesce('NP'+ convert(varchar(max), nullif(a.ClientId,'')),NULL) as com_ext_id
, -10 as user_account_id
, convert(datetime,i.InterviewDate,120) + convert(datetime, i.InterviewTime,120) as interview_datetime
, 'comment' as category
, 'candidate' as type
, it.Description
, iot.Description
, concat_ws(char(10), '[Interviews]'
	, coalesce('Created by: '+ u.UserFullName,NULL)
	, coalesce('Updated by: '+ u2.UserFullName,NULL)
	, coalesce('Interview Type: '+ it.Description,NULL)
	, coalesce('Applicant Confirmed: '+ i.ApplicantConfirmed,NULL)
	, coalesce('Client Contact Confirmed: '+ i.ClientContactConfirmed,NULL)
	, coalesce('Interview Date/Time: '+ convert(varchar(max), (convert(datetime,i.InterviewDate,120) + convert(datetime, i.InterviewTime,120)), 120),NULL)
	, coalesce('Notes: '+ i.Notes,NULL)
	, coalesce('Interview Outcome: '+ iot.Description,NULL)
	) as comment_activities
from Interviews i
left join InterviewOutcomes iot on iot.InterviewOutcomeId = i.InterviewOutcomeId
left join InterviewTypes it on it.InterviewTypeId = i.InterviewTypeId
left join ApplicantActions a on a.ApplicantActionId = i.ApplicantActionId
left join userinfo u on u.UserId = i.CreatedUserId
left join userinfo u2 on u2.UserId = i.UpdatedUserId
where i.ApplicantActionId is not NULL
--and i.InterviewId = 13165
--total 13158 rows