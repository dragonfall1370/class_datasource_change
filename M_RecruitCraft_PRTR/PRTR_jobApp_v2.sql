with 
JobApp as (
--FROM Shortlist
select shortlist_id, can_id, s.vac_id
, case when ltrim(rtrim(movement)) in ('Approach','Direct Applicants','Shortlist','Interview') then 1 --SHORTLISTED
	when ltrim(rtrim(movement)) in ('Resume Sent') then 2 --SENT
	when ltrim(rtrim(movement)) in ('First Client Interview') then 3 --FIRST_INTERVIEW
	when ltrim(rtrim(movement)) in ('Second Client Interview') then 4 --SECOND_INTERVIEW
	when ltrim(rtrim(movement)) in ('Offer') then 5 --OFFERED
	when ltrim(rtrim(movement)) in ('Score') then 6 --PLACED
	else 0 end as Appstage
, case when ltrim(rtrim(movement)) in ('Shortlist') then NULL
	when ltrim(rtrim(movement)) in ('Interview') then 'PRTR INTERVIEW'
	else ltrim(rtrim(movement)) end as Substatus --custom mapping for SubStatus
, case when (timestamp is NULL or timestamp = '') and release_date is not NULL then release_date
	when (timestamp is NULL or timestamp = '') and (release_date is NULL or release_date = '') then v.vac_timestamp
	else timestamp end as timestamp
from vacancies.Shortlist s
left join (select distinct vac_id, vac_timestamp from vacancies.Vacancies) v on s.vac_id = v.vac_id --to get job created date for timestamp overwritten
where s.can_id > 0 and s.vac_id > 0 --470705

UNION ALL
-- FROM ShortlistTracking
select st.shortlist_id, s.can_id, s.vac_id
, case when ltrim(rtrim(st.mov_id)) in ('Approach','Candidate Shortlisted','Direct Applicants','Interview','INV','Not Interested','Shortlist','Shortlisted','TEL CC','TEL INV') then 1
	when ltrim(rtrim(st.mov_id)) in ('Resume Sent') then 2
	when ltrim(rtrim(st.mov_id)) in ('First Client Interview','1st CC IV') then 3
	when ltrim(rtrim(st.mov_id)) in ('2nd CC IV','2nd CCIV','Second Client Interview') then 4
	when ltrim(rtrim(st.mov_id)) in ('Offer') then 5
	when ltrim(rtrim(st.mov_id)) in ('Score') then 6
	else 0 end as Appstage
, case when ltrim(rtrim(st.mov_id)) in ('Candidate Shortlisted','Shortlist','Shortlisted','Remove From Shortlist') then NULL
	when ltrim(rtrim(st.mov_id)) in ('Interview','INV') then 'PRTR INTERVIEW'
	else ltrim(rtrim(st.mov_id)) end as Substatus --custom mapping for SubStatus
, case when st.timestamp is NULL then v.vac_timestamp
	else st.timestamp end as timestamp
from vacancies.ShortlistTracking st
left join vacancies.Shortlist s on s.shortlist_id = st.shortlist_id --484255
left join vacancies.Vacancies v on s.vac_id = v.vac_id --to get job created date for timestamp overwritten

UNION ALL

--FROM PLACEMENT
	select s.shortlist_id, p.can_id, p.vac_id
	, 6 as Appstage --PLACED
	, case when ltrim(rtrim(p.pl_status)) in ('Paid','Pending','Active') then 'INVOICED'
		when ltrim(rtrim(p.pl_status)) in ('Client Cancelled','Cancelled') then 'PLACEMENT CANCELLED'
		end as Substatus
	, case when p.pl_date_created is NULL or p.pl_date_created = '' then p.pl_date_start --THIS MAY CAUSE AN ISSUE IN PROD, because the associated date in the future date
	else p.pl_date_created end as timestamp
	from placements.Placements p
	left join vacancies.Shortlist s on p.can_id = s.can_id and p.vac_id = s.vac_id
	where p.can_id > 0 and p.vac_id > 0
	)

--HIGHEST STAGE FROM ALL TABLES
, HighestStage as (
	select shortlist_id
	, can_id
	, vac_id
	, Appstage
	, Substatus
	, timestamp
	--, row_number() over(partition by can_id, vac_id order by timestamp desc, Appstage desc, shortlist_id desc) as rn --get the latest update
	, row_number() over(partition by can_id, vac_id order by Appstage desc, timestamp desc, shortlist_id desc) as rn --get highest stage instead
	from JobApp
	where Appstage > 0 --895404
	)

/* CLARIFICATION
select * from HighestStage
where vac_id = 1086669 and can_id = 5078051
*/

select s.shortlist_id
	, concat('PRTR',s.can_id) as 'application-candidateExternalId'
	, concat('PRTR',s.vac_id) as 'application-positionExternalId'
	, case when v.vac_type = 'Full Time' and s.Appstage = 6 then 'PLACEMENT_PERMANENT'
		when v.vac_type = 'Part Time' and s.Appstage = 6 then 'PLACEMENT_PERMANENT'
		when v.vac_type = 'Contract' and s.Appstage = 6 then 'PLACEMENT_CONTRACT'
		when v.vac_type = 'Hiring' and s.Appstage = 6 then 'PLACEMENT_PERMANENT'
		when s.Appstage = 6 then 'PLACEMENT_PERMANENT'
		when s.Appstage = 5 then 'OFFERED'
		when s.Appstage = 4 then 'SECOND_INTERVIEW'
		when s.Appstage = 3 then 'FIRST_INTERVIEW'
		when s.Appstage = 2 then 'SENT'
		when s.Appstage = 1 then 'SHORTLISTED'
		end as 'Originalstage'
	, case 
		when s.Appstage = 6 then 'OFFERED' --to be updated as PLACED afterward
		when s.Appstage = 5 then 'OFFERED'
		when s.Appstage = 4 then 'SECOND_INTERVIEW'
		when s.Appstage = 3 then 'FIRST_INTERVIEW'
		when s.Appstage = 2 then 'SENT'
		when s.Appstage = 1 then 'SHORTLISTED'
		end as 'application-stage'
	, s.Substatus
	, s.timestamp as 'application-actionedDate'
from HighestStage s
left join vacancies.Vacancies v on v.vac_id = s.vac_id
where rn = 1 --465967
--and s.Appstage = 6 --13734
--and s.shortlist_id in (17797,45985) --test 2 cases for NULL/blank job type
--and s.vac_id = 1086669 --test case for NULL/blank timestamp