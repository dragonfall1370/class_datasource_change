/* Update formatted for Match/Stage

--> Delete unformatted comments from activity
select count(*) from activity
where content like 'Match Number%' --308794
--and type = 'job' --154426
and type = 'candidate' --154368
limit 10

select * from interview

select * from position_candidate
limit 10

select * from position_candidate_view
limit 10

*/


with JobApp as (select s.Match_Number
	, m.Candidate_Number
	, m.Job_Order_Number
	, m.Company_Number
	, concat_ws('; '
		, coalesce('Stage: ' + ms.Description,NULL)
		, iif(s.Created_DTTM is NULL,NULL,concat('Created date: ', convert(varchar(20),s.Created_DTTM,120)))
		, coalesce('Created by: ' + concat(u.Firstname,' ',u.Lastname,' - ',u.Email),NULL)
		) as Stage
	, s.Created_DTTM as CreatedDate
	from Stage s --371126
	left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id --371126
	left join AFR_User u on u.UserID = s.Created_By_Person_Number --371126
	left join Match m on m.Match_Number = s.Match_Number --371126
	
	--UNION ALL
	
	--select i.Match_Number
	--, m.Candidate_Number
	--, m.Job_Order_Number
	--, m.Company_Number
	--, concat_ws('; '
	--	, concat('Stage: Interview - ',coalesce(it.Description,NULL))
	--	, iif(s.Created_DTTM is NULL,NULL,concat('Created date: ', convert(varchar(20),s.Created_DTTM,120)))
	--	, coalesce('Created by: ' + concat(u.Firstname,' ',u.Lastname,' - ',u.Email),NULL)
	--	) as Stage
	--, i.Interview_DT as InterviewDate
	--from Interview i --16130
	--left join (select Match_Number, Created_DTTM, Created_By_Person_Number
	--	, row_number() over(partition by Match_Number order by Created_DTTM desc) as rn
	--	from Stage
	--	where Match_Stage_Id = 4) as s on s.Match_Number = i.Match_Number
	--left join Match m on m.Match_Number = i.Match_Number
	--left join Interview_Type it on it.Interview_Type_Id = i.Interview_Type_Id
	--left join AFR_User u on u.UserID = s.Created_By_Person_Number
	--where s.rn = 1)
	)

, JobAppGroup as (select Match_Number, Candidate_Number, Job_Order_Number, Company_Number
	, string_agg(Stage,char(10)) within group (ORDER BY CreatedDate desc) as JobAppStage
	, max(CreatedDate) as CreatedDate
	from JobApp
	group by Match_Number, Candidate_Number, Job_Order_Number, Company_Number)

select ja.Match_Number
	, concat('AFR',ja.Candidate_Number) as AFR_CandidateExtID
	, concat('AFR',ja.Job_Order_Number) as AFR_JobExtID
	, ja.Company_Number
	, concat_ws(char(10), '[Historical Stages]'
		, concat(coalesce('Company Name: ' + nullif(c.Company_Name,''),NULL), '; ' ,coalesce('Job Title: ' + nullif(j.Position_Title,''),NULL))
		, concat(char(10),JobAppStage,char(10))
		, coalesce('Job Order Number: ' + convert(varchar(10),ja.Job_Order_Number),NULL)
		, coalesce('Company Number: ' + convert(varchar(10), ja.Company_Number),NULL)
		, coalesce('Match Number: ' + convert(varchar(10),ja.Match_Number),'')
		, coalesce('Candidate Number: ' + convert(varchar(10),ja.Candidate_Number),NULL)
		, coalesce('Candidate Name: ' + coalesce(p.Name_First,'') + ' ' + coalesce(p.Name_Middle,'') + ' ' + coalesce(p.Name_Last,''),NULL)
	) as AFR_comment_activities
	, -10 as AFR_User_account_id
	, CreatedDate as AFR_Insert_timestamp
	, 'comment' as AFR_category
	, 'candidate' as AFR_type
from JobAppGroup ja
left join Company c on c.Company_Number = ja.Company_Number
left join Person p on p.Person_Number = ja.Candidate_Number
left join Job_Order j on j.Job_Order_Number = ja.Job_Order_Number
--where ja.Match_Number = 73187