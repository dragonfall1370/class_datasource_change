--FILTER STAGE
with AFRStage as (
	select s.Match_Number, s.Stage_Id, s.Match_Stage_Id, ms.Description
	, s.Created_DTTM, s.Created_By_Person_Number, concat(u.Firstname,' ',u.Lastname) as UserFullname, u.Email
	, case when s.Match_Stage_Id in (-3,-2) then 6
		when s.Match_Stage_Id in (5) then 5
		when s.Match_Stage_Id in (4) then 3
		when s.Match_Stage_Id in (3) then 2
		when s.Match_Stage_Id in (1, 1001, 2) then 1
		else 0 end as VC_stage
	from Stage s
	left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id
	left join AFR_User u on u.UserID = s.Created_By_Person_Number
	where s.Match_Stage_Id not in (-5,-4,-1,34)
	
	UNION ALL
	
	select Match_Number, Stage_Id, Interview_Sequence_Number, 'Interview Sequence'
	, Interview_DT, 0, Interview_With, concat(coalesce(Interview_With,'nointerviewer'),'@intervieweremail.com')
	, case when Interview_Sequence_Number in (6, 5, 4, 3, 2) then 4
    when Interview_Sequence_Number = 1 then 3
    else 0 end as VC_stage
	from Interview
	)

, HighestStage as (
	select a.Match_Number, a.Stage_Id, a.Description, a.VC_stage, a.Created_DTTM, a.UserFullname, a.Email
	, m.Job_Order_Number, m.Candidate_Number
	, row_number() over(partition by m.Job_Order_Number, m.Candidate_Number order by a.VC_Stage desc) as rn
	from AFRStage a
	left join Match m on m.Match_Number = a.Match_Number)

--JOB APPLICATION WITH REJECTED
, VCStatus as (select distinct s.Match_Number, 'Rejected' as VC_status
	, row_number() over(partition by m.Job_Order_Number, m.Candidate_Number order by s.Match_Number desc) as rn
	from Stage s
	left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id
	left join Match m on m.Match_Number = s.Match_Number
	where s.Match_Stage_Id in (-5,-1))

--JOB APPLICATION WITH SUB STAGE: Contacted & Recruiter Interview
, SubStage as (select s.Match_Number
	, case when s.Match_Stage_Id = 1001 then 'Contacted'
		when s.Match_Stage_Id = 2 then 'Recruiter Interview'
		end as VCSubStage
	, m.Job_Order_Number, m.Candidate_Number
	, row_number() over(partition by m.Job_Order_Number, m.Candidate_Number order by s.Match_Number desc) as rn
	from Stage s
	left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id
	left join Match m on m.Match_Number = s.Match_Number
	where s.Match_Stage_Id in (1001, 2))

--JOB APPLICATION WITH SUB STATUS: CANCELLED
, SubStatus as (select distinct s.Match_Number
	, 'Cancelled' as VCSubStatus
	, row_number() over(partition by m.Job_Order_Number, m.Candidate_Number order by s.Match_Number desc) as rn
	from Stage s
	left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id
	left join Match m on m.Match_Number = s.Match_Number
	where s.Match_Stage_Id = -5)

--JOB APPLICATION OWNERS
, JobAppOwners as (select m.Match_Number, m.Account_Manager_Person_Number, m.Recruiter_Person_Number, concat_ws(',',u.Email,u2.Email) as JobAppOwners
	from Match m
	left join AFR_User u on m.Account_Manager_Person_Number = u.UserID
	left join AFR_User u2 on m.Recruiter_Person_Number = u2.UserID)

--MAIN SCRIPT
select hs.Match_Number
, concat('AFR',m.Job_Order_Number) as 'application-positionExternalId'
, concat('AFR',m.Candidate_Number) as 'application-candidateExternalId'
, m.Company_Number, c.Company_Name
, jao.JobAppOwners
, hs.Stage_Id, hs.Description
, case hs.VC_stage
	when 6 then 'PLACEMENT_PERMANENT'
	when 5 then 'OFFERED'
	--when 8 then 'SIXTH_INTERVIEW'
	--when 7 then 'FIFTH_INTERVIEW'
	--when 6 then 'FOURTH_INTERVIEW'
	--when 5 then 'THIRD_INTERVIEW'
	when 4 then 'SECOND_INTERVIEW'
	when 3 then 'FIRST_INTERVIEW'
	when 2 then 'SENT'
	when 1 then 'SHORTLISTED'
	end as 'application-stage'
, hs.VC_stage
, ss.VC_status as RejectedStatus
, convert(datetime,hs.Created_DTTM + 7,120) as RejectedDate
, case when hs.VC_stage = 1 then sub.VCSubStage 
	else NULL end as ConfigStage
, sst.VCSubStatus as SubStatus
, convert(datetime,hs.Created_DTTM,120) as 'application-date'
, hs.UserFullname, hs.Email
from HighestStage hs
left join VCStatus ss on ss.Match_Number = hs.Match_Number and ss.rn = 1
left join SubStage sub on sub.Match_Number = hs.Match_Number and sub.rn = 1
left join SubStatus sst on sst.Match_Number = hs.Match_Number and sst.rn = 1
left join Match m on m.Match_Number = hs.Match_Number
left join Company c on c.Company_Number = m.Company_Number
left join JobAppOwners jao on jao.Match_Number = m.Match_Number
where hs.rn = 1 --154260
and hs.Job_Order_Number in (select Job_Order_Number from Job_Order)
and hs.Candidate_Number in (select Person_Number from Person)
and hs.VC_stage = 1 and sub.VCSubStage is not NULL
order by m.Job_Order_Number, m.Candidate_Number desc