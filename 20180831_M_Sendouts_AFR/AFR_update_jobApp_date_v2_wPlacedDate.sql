--SHORTLISTED STAGE
with ShortlistedPlaced as (select s.Match_Number
	, p.Match_Number as Placed_Match_Number
	, m.Candidate_Number
	, m.Job_Order_Number
	, s.Stage_Id
	, s.Match_Stage_Id
	, s.Created_DTTM as CreatedDate
	, u.Email
	from Stage s --371126
	left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id --371126
	left join AFR_User u on u.UserID = s.Created_By_Person_Number --371126
	left join Match m on m.Match_Number = s.Match_Number --371126
	left join (select s.Match_Number
		from Stage s --371126
		left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id --371126
		left join AFR_User u on u.UserID = s.Created_By_Person_Number --371126
		left join Match m on m.Match_Number = s.Match_Number --371126
		where s.Match_Stage_Id in (-3,-2)) as p on p.Match_Number = s.Match_Number --PLACED candidates
	where s.Match_Stage_Id = 1)

, FilterShortlisted as (select Match_Number
	, Placed_Match_Number
	, Candidate_Number
	, Job_Order_Number
	, row_number() over (partition by Candidate_Number, Job_Order_Number order by Placed_Match_Number desc) as rn
	, Stage_Id
	, Match_Stage_Id
	, CreatedDate
	, Email
	from ShortlistedPlaced)

/* CHECK IF MORE THAN 1 MATCH NUMBER FOR SAME CANDIDATE/JOB
select Match_Number from FilterShortlisted
where rn = 1
group by Match_Number
having count(*) > 1
*/

, maxShortlisted as (select Candidate_Number
	, Job_Order_Number
	, Match_Number
	, Stage_Id
	, Match_Stage_Id
	, CreatedDate
	, Email as maxUserEmail
	from FilterShortlisted
	where rn = 1
	--group by Candidate_Number, Job_Order_Number
	)

--SENT STAGE
, SentStage as (select s.Match_Number
	, m.Candidate_Number
	, m.Job_Order_Number
	, s.Stage_Id
	, s.Match_Stage_Id
	, s.Created_DTTM as CreatedDate
	, u.Email
	from Stage s --371126
	left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id --371126
	left join AFR_User u on u.UserID = s.Created_By_Person_Number --371126
	left join Match m on m.Match_Number = s.Match_Number --371126
	where s.Match_Stage_Id = 3 
	)

--select Match_Number from SentStage
--group by Match_Number
--having count(*) > 1

, maxSentStage as (select Candidate_Number
	, Job_Order_Number
	, max(Match_Number) as maxMatchNumber
	, max(Stage_Id) as maxStageID
	, max(Match_Stage_Id) as maxMatchStageID
	, max(CreatedDate) as maxCreatedDate
	, max(Email) as maxUserEmail
	from SentStage
	group by Candidate_Number, Job_Order_Number)

--1ST INTERVIEW STAGE
, FirstInterview as (select s.Match_Number
	, m.Candidate_Number
	, m.Job_Order_Number
	, s.Stage_Id
	--, s.Match_Stage_Id
	, s.Created_DTTM as InterviewDate
	from Stage s --371126
	left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id --371126
	left join Match m on m.Match_Number = s.Match_Number --371126
	where s.Match_Stage_Id = 4

	UNION ALL

	select i.Match_Number
	, m.Candidate_Number
	, m.Job_Order_Number
	, i.Stage_Id
	--, i.Interview_Sequence_Number
	, i.Interview_DT
	from Interview i 
	left join Match m on m.Match_Number = i.Match_Number
	where i.Interview_Sequence_Number = 1
	)

--select Match_Number 
--from FirstInterview
--group by Match_Number
--having count(*) > 1 --13109

, maxFirstInterview as (select Candidate_Number
	, Job_Order_Number
	, max(Match_Number) as maxMatchNumber
	, max(Stage_Id) as maxStageID
	, max(InterviewDate) as maxInterviewDate
	from FirstInterview
	group by Candidate_Number, Job_Order_Number
	)

--select Match_Number 
--from maxFirstInterview
--group by Match_Number
--having count(*) > 1

--2ND INTERVIEW STAGE
, nd_Interview as (select i.Match_Number
	, m.Candidate_Number
	, m.Job_Order_Number
	, i.Stage_Id
	--, i.Interview_Sequence_Number
	, i.Interview_DT
	from Interview i 
	left join Match m on m.Match_Number = i.Match_Number
	where i.Interview_Sequence_Number in (6, 5, 4, 3, 2)
	)

--select Match_Number
--from nd_Interview
--group by Match_Number
--having count(*) > 1 --377

, maxnd_Interview as (select Candidate_Number
	, Job_Order_Number
	, max(Match_Number) as maxMatchNumber
	, max(Stage_Id) as maxStage
	, max(Interview_DT) as maxInterview_DT
	from nd_Interview
	group by Candidate_Number, Job_Order_Number
	)

--select Match_Number, maxInterview_DT, maxStage from maxnd_Interview
--group by Match_Number having count(*) > 1

--OFFER STAGE
, Offer as (select s.Match_Number
	, m.Candidate_Number
	, m.Job_Order_Number
	, s.Stage_Id
	, s.Match_Stage_Id
	, s.Created_DTTM as CreatedDate
	, u.Email
	from Stage s --371126
	left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id --371126
	left join AFR_User u on u.UserID = s.Created_By_Person_Number --371126
	left join Match m on m.Match_Number = s.Match_Number --371126
	where s.Match_Stage_Id = 5)

--select Match_Number from Offer
--group by Match_Number having count(*) > 1 --27

, maxOffer as (select Candidate_Number
	, Job_Order_Number
	, max(Match_Number) as maxMatchNumber
	, max(Stage_Id) as maxStageID
	, max(Match_Stage_Id) as maxMatchStageID
	, max(CreatedDate) as maxCreatedDate
	, max(Email) as maxUserEmail
	from Offer
	group by Candidate_Number, Job_Order_Number
	) -- 4230 offers

--PLACED STAGE
, Place as (select s.Match_Number
	, m.Candidate_Number
	, m.Job_Order_Number
	, s.Stage_Id
	, s.Match_Stage_Id
	, s.Created_DTTM as CreatedDate
	, u.Email
	from Stage s --371126
	left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id --371126
	left join AFR_User u on u.UserID = s.Created_By_Person_Number --371126
	left join Match m on m.Match_Number = s.Match_Number --371126
	where s.Match_Stage_Id in (-3,-2)
	) --having no more than 1 placement || 3431 placements

--select Match_Number from Place
--group by Match_Number
--having count(*) > 1

--MAIN SCRIPT
select sl.Match_Number
	, sl.Candidate_Number
	, ps.Name_First
	, ps.Name_Last
	, sl.Job_Order_Number
	, jo.Position_title
	, sl.CreatedDate as ShortlistedDate
	, sl.maxUserEmail as ShortlistedBy
	, ms.maxCreatedDate as SentDate_Origin
	, case 
		when ms.maxCreatedDate is NULL and p.CreatedDate is not NULL then p.CreatedDate
		when ms.maxCreatedDate is NULL and mo.maxCreatedDate is not NULL then mo.maxCreatedDate
		when ms.maxCreatedDate is NULL and mnd.maxInterview_DT is not NULL then mnd.maxInterview_DT
		when ms.maxCreatedDate is NULL and mf.maxInterviewDate is not NULL then mf.maxInterviewDate
		else ms.maxCreatedDate end as SentDate
	, ms.maxUserEmail as SentBy_Origin
	, case when ms.maxCreatedDate is not NULL and ms.maxUserEmail is NULL then sl.maxUserEmail
		else ms.maxUserEmail end as SentBy
	, mf.maxInterviewDate as FirstInterviewDate_Origin
	, case 
		when mf.maxInterviewDate is NULL and p.CreatedDate is not NULL then p.CreatedDate
		when mf.maxInterviewDate is NULL and mo.maxCreatedDate is not NULL then mo.maxCreatedDate
		when mf.maxInterviewDate is NULL and mnd.maxInterview_DT is not NULL then mnd.maxInterview_DT
		else mf.maxInterviewDate end as FirstInterviewDate
	, mnd.maxInterview_DT as SecondInterviewDate_Origin
	, case 
		when mnd.maxInterview_DT is NULL and p.CreatedDate is not NULL then p.CreatedDate
		when mnd.maxInterview_DT is NULL and mo.maxCreatedDate is not NULL then mo.maxCreatedDate
		else mnd.maxInterview_DT end as SecondInterviewDate
	, mo.maxCreatedDate as OfferedDate_Origin
	, case 
		when mo.maxCreatedDate is NULL and p.CreatedDate is not NULL then p.CreatedDate
		else mo.maxCreatedDate end as OfferedDate
	, p.CreatedDate as PlacedDate
from maxShortlisted sl
left join maxSentStage ms on ms.maxMatchNumber = sl.Match_Number --155224
left join maxFirstInterview mf on mf.maxMatchNumber = sl.Match_Number
left join maxnd_Interview mnd on mnd.maxMatchNumber = sl.Match_Number
left join maxOffer mo on mo.maxMatchNumber = sl.Match_Number
left join Place p on p.Match_Number = sl.Match_Number
left join Person ps on ps.Person_Number = sl.Candidate_Number
left join Job_Order jo on jo.Job_Order_Number = sl.Job_Order_Number
--where p.CreatedDate is not NULL --3415
--where mo.maxCreatedDate is not NULL or p.CreatedDate is not NULL
order by sl.Match_Number desc -- 154545


/* CHECK IF PLACED STAGE CAN BE KEPT
select * from Main
where 1=1 
and PlacedDate is not NULL
and Candidate_Number = 56336
and Job_Order_Number = 3355
*/

/* CHECK IF MORE THAN 1 JOB APPLICATION WITH DIFFERENT MATCH NUMBER
select * from Main
where Candidate_Number = 66875
and Job_Order_Number = 3772
*/

/* CHECK IF JOB APPLICATION WITH MORE THAN 1 ROW
select Candidate_Number, Job_Order_Number from Main
group by Candidate_Number, Job_Order_Number
having count(*) > 1
*/