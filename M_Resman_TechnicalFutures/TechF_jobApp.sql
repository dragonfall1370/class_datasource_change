with JobApp as (select CandidateID
, JobID
, case when [Acknowledge_application_CV_received__add_candidate_to_application_list] is not NULL then 1
	else 0 end as Stage1
, case when [Phone_screen] is not NULL then 1
	else 0 end as Stage2
, case when [Interview_by_consultant] is not NULL then 1
	else 0 end as Stage3
, case when [Reject_Candidate] is not NULL then 'Rejected'
	else NULL end as SubStatus
, case when [Candidate_summary_prepared_for_client] is not NULL then 2
	else 0 end as Stage4
, case when [References_sent] is not NULL then 4
	else 0 end as Stage5
, case when [_CV_sent_to_Client__by_itself__on_longlist__or_on_shortlist__] is not NULL then 2
	else 0 end as Stage6
, case when [Candidate_interviewed_by_client] is not NULL then 3
	else 0 end as Stage7
, case when [Offers_made_Negotiations] is not NULL then 5
	else 0 end as Stage8
, case when [Offer_accepted] is not NULL then 6
	else 0 end as Stage9
from JobApplicants)

, maxStage as (select CandidateID
	, JobID
	, (select max(maxStage) from (values (Stage1),(Stage2),(Stage3),(Stage4),(stage5),(stage6),(stage7),(stage8),(stage9)) AS Stage(maxStage)) as maxStage
	, SubStatus
	from JobApp)

select concat('TF',CandidateID) as 'application-candidateExternalId'
, concat('TF',JobID) as 'application-positionExternalId'
, case --Update job application based on job type
	when maxStage = 6 and j.jobType = 'PERMANENT' then 'PLACEMENT_PERMANENT'
	when maxStage = 6 and j.jobType = 'CONTRACT' then 'PLACEMENT_CONTRACT'
	when maxStage = 6 and j.jobType = 'TEMPORARY' then 'PLACEMENT_TEMP'
	when maxStage = 6 then 'PLACEMENT_PERMANENT'
	when maxStage = 5 then 'OFFERED'
	when maxStage = 4 then 'SECOND_INTERVIEW'
	when maxStage = 3 then 'FIRST_INTERVIEW'
	when maxStage = 2 then 'SENT'
	when maxStage = 1 then 'SHORTLISTED'
	end as 'application-stage'
, SubStatus as RejectedStage
from maxStage m--12306 rows
left join (select ActivityID
		, case when PermanentRqd = 'X' then 'PERMANENT'
			when ContractRqd = 'X' then 'CONTRACT'
			when TemporaryRqd = 'X' then 'TEMPORARY'
			else 'PERMANENT' end jobType
		from JobMaster) as j on j.ActivityID = m.JobID
where CandidateID in (select PersonID from CandidateMaster where status = 'A')
and JobID in (select ActivityID from JobMaster where ContactID in (select ContactID from ClientContacts where status = 'A')) --8217 rows
and maxStage > 0 --6494