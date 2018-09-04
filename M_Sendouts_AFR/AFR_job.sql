--DUPLICATION REGCONITION
with dup as (select Job_Order_Number, Position_Title, row_number() over(partition by Position_title order by Job_Order_Number) as rn
	from Job_Order j)

, NewJobTitle as (select Job_Order_Number
	, case when rn > 1 then concat(Position_title, ' - ', Job_Order_Number)
	else Position_title end as NewRoleDescription
	from dup)

--JOB OWNERS
, Owners as (select j.Job_Order_Number, u.Email
	from Job_Order j
	left join AFR_User u on u.UserID = j.Account_Manager_Person_Number
	UNION ALL
	select Job_Number, RC_Primary
	from AFR_JobCSV3)

, DistinctOwner as (select distinct Job_Order_Number, Email
	from Owners)

, JobOwners as (select Job_Order_Number, STRING_AGG(Email,',') as JobOwners
	from DistinctOwner
	where Email is not NULL
	group by Job_Order_Number)

--JOB DOCUMENTS
, Documents as (select Job_Number, STRING_AGG(concat(Foldername,'_',replace(replace(Filename,'?',''),',','')),',') as Documents
	from AFR_JobCSV4
	group by Job_Number)

--MAIN SCRIPT
select concat('AFR',j.Job_Order_Number) as 'position-externalId'
, concat('AFR999',j.Company_Number) as 'position-contactId'
, n.NewRoleDescription as 'position-title'
, convert(nvarchar(10),j.Open_DT,120) as 'position-startDate'
--, convert(nvarchar(10),j.Closed_DT,120) as 'position-endDate' | updated req on 28/08/2018
, case when jot.Description <> 'Open' then getdate() - 7
	else NULL end as 'position-endDate'
, 'EUR' as 'position-currency'
, j.Positions_Total_Cnt as 'position-headcount'
, j.Comment as 'position-internalDescription'
, jo.JobOwners as 'position-owners'
, d.Documents as 'position-document'
--, case when isnumeric(a3.BilledOvertimeRate) = 1 then a3.BilledOvertimeRate
--	else NULL end as 'position-actualSalary' | removed on 28/08/2018
, concat_ws (char(10), concat('Job External ID: ', j.Job_Order_Number)
	, concat('Company External ID: ', j.Company_Number)
	) as 'position-note'
from Job_Order j
left join NewJobTitle n on n.Job_Order_Number = j.Job_Order_Number
left join JobOwners jo on jo.Job_Order_Number = j.Job_Order_Number
left join Documents d on d.Job_Number = j.Job_Order_Number
left join AFR_JobCSV3 a3 on a3.Job_Number = j.Job_Order_Number
left join Job_Order_Status jot on jot.Job_Order_Status_Id = j.Job_Order_Status_Id
order by j.Job_Order_Number