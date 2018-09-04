--JOB APPLICATION HISTORY AS CANDIDATE & JOB ACTIVITIES
with JobAppOwners as (select m.Match_Number, m.Account_Manager_Person_Number, m.Recruiter_Person_Number, concat_ws(',',u.Email,u2.Email) as JobAppOwners
	from Match m
	left join AFR_User u on m.Account_Manager_Person_Number = u.UserID
	left join AFR_User u2 on m.Recruiter_Person_Number = u2.UserID)

--MAIN SCRIPT
, Activities as (select m.Candidate_Number
, m.Job_Order_Number
, concat_ws(char(10), coalesce('Match Number: ' + convert(varchar(10),s.Match_Number),'')
	, iif(s.Created_DTTM is NULL,NULL,concat('Created date: ', convert(varchar(20),s.Created_DTTM,120)))
	, coalesce('Created by: ' + concat(u.Firstname,' ',u.Lastname,' - ',u.Email),NULL)
	, coalesce('Stage Id: ' + convert(varchar(10),s.Stage_Id),NULL)
	, coalesce('Match Stage: ' + ms.Description,NULL)
	, coalesce('Candidate Number: ' + convert(varchar(10),m.Candidate_Number),NULL)
	, coalesce('Candidate Name: ' + coalesce(p.Name_First,'') + ' ' + coalesce(p.Name_Middle,'') + ' ' + coalesce(p.Name_Last,''),NULL)
	, coalesce('Job Order Number: ' + convert(varchar(10),m.Job_Order_Number),NULL)
	, coalesce('Job Title: ' + nullif(j.Position_Title,''),NULL)
	, coalesce('Company Number: ' + convert(varchar(10), m.Company_Number),NULL)
	, coalesce('Company Name: ' + nullif(c.Company_Name,''),NULL)
	, coalesce('Job Owners: ' + jao.JobAppOwners,NULL)
	) as Activities
, -10 as User_account_id
, 'comment' as Category
, 'candidate' as Type
, s.Created_DTTM as Insert_timestamp
--, convert(varchar(20),s.Created_DTTM,120) as Insert_timestamp
from Stage s --343854
left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id
left join AFR_User u on u.UserID = s.Created_By_Person_Number
left join Match m on m.Match_Number = s.Match_Number
left join Company c on c.Company_Number = m.Company_Number
left join Person p on p.Person_Number = m.Candidate_Number
left join Job_Order j on j.Job_Order_Number = m.Job_Order_Number
left join JobAppOwners jao on jao.Match_Number = m.Match_Number

UNION ALL

select m.Candidate_Number
, m.Job_Order_Number
, concat_ws(char(10), coalesce('Match Number: ' + convert(varchar(10),i.Match_Number),'')
	, iif(i.Interview_DT is NULL,NULL,concat('Interview date: ', convert(varchar(20),i.Interview_DT,120)))
	, coalesce('Interview with: ' + nullif(i.Interview_With,''),NULL)
	, coalesce('Stage Id: ' + convert(varchar(10),i.Stage_Id),NULL)
	, coalesce('Interview Number: ' + convert(varchar(10),i.Interview_Number),NULL)
	, coalesce('Interview Type: ' + it.Description,NULL)
	, coalesce('Interview Sequence Number: ' + convert(varchar(10),i.Interview_Sequence_Number),NULL)
	, coalesce('Interview Description: ' + i.Interview_Description,NULL)
	, coalesce('Candidate Number: ' + convert(varchar(10),m.Candidate_Number),NULL)
	, coalesce('Candidate Name: ' + coalesce(p.Name_First,'') + ' ' + coalesce(p.Name_Middle,'') + ' ' + coalesce(p.Name_Last,''),NULL)
	, coalesce('Job Order Number: ' + convert(varchar(10),m.Job_Order_Number),NULL)
	, coalesce('Job Title: ' + nullif(j.Position_Title,''),NULL)
	, coalesce('Company Number: ' + convert(varchar(10), m.Company_Number),NULL)
	, coalesce('Company Name: ' + nullif(c.Company_Name,''),NULL)
	) as Activities
, -10 as User_account_id
, 'comment' as category
, 'candidate' as Type
, i.Interview_DT as Insert_timestamp
from Interview i
left join Match m on m.Match_Number = i.Match_Number
left join Company c on c.Company_Number = m.Company_Number
left join Person p on p.Person_Number = m.Candidate_Number
left join Job_Order j on j.Job_Order_Number = m.Job_Order_Number
left join Interview_Type it on it.Interview_Type_Id = i.Interview_Type_Id)

select concat('AFR',Candidate_Number) as AFR_CandidateExtID
, concat('AFR',Job_Order_Number) as AFR_JobExtID
, STRING_AGG(Activities, concat(char(10),char(13))) within group (order by Insert_timestamp desc) as AFR_comment_activities
, max(Insert_timestamp) as AFR_Insert_timestamp
, max(User_account_id) as AFR_User_account_id
, max(category) as AFR_category
, max(Type) as AFR_type
from Activities
where Candidate_Number is not NULL and Job_Order_Number is not NULL
group by Candidate_Number, Job_Order_Number