with --CANDIDATE OWNERS
 MergeCandidateOwners as (select c.ID
	, CONCAT_WS(';',u.NAME, HOTLIST__C, REGISTERED_BY_PICKLIST__C) as CandidateOwners
	from (select * from Candidate UNION ALL select * from CandidateDelta) as c
	left join SiriusUsers u on c.OWNERID = u.ID)

, SplitCandidateOwners as (select ID as CandidateID, CandidateOwners, value as SplitCandidateOwners
	from MergeCandidateOwners
	cross apply string_split(CandidateOwners,';'))

, CandidateOwners as (select CandidateID, ltrim(rtrim(SplitCandidateOwners)) as SplitCandidateOwners
	from SplitCandidateOwners
	group by CandidateID, SplitCandidateOwners)

, CandidateOwnersFinal as (select co.CandidateID, su.EMAIL as CandidateOwnersFinal
	from CandidateOwners co
	left join SiriusUsers su on su.NAME = co.SplitCandidateOwners)

, OwnerID as (select CandidateID
	, case when co.CandidateOwnersFinal = vua.email then vua.ID
	when co.CandidateOwnersFinal = 'indira@siriussupport.com.au' then 29079
	when co.CandidateOwnersFinal = 'kpantenburg@siriustechnology.com.au' then 
	when co.CandidateOwnersFinal = 'lnewton@siriustechnology.com.au' then
	when co.CandidateOwnersFinal = 'melissa@siriustechnology.com.au' then
	when co.CandidateOwnersFinal = 'sandee@siriusbusinesssupport.com.au' then 29084
	when co.CandidateOwnersFinal = 'yolande@siriusaf.com.au' then
	when co.CandidateOwnersFinal = 'ssmith@siriusrecruitment.com.au' then 29072
	end as OwnerID
from CandidateOwnersFinal co
left join VincereUserAccount vua on vua.email = co.CandidateOwnersFinal)

, CandidateOwnerOrder as (SELECT CandidateID, OwnerID, ROW_NUMBER() OVER(PARTITION BY CandidateID ORDER BY CandidateID ASC) AS rn 
FROM OwnerID
where OwnerID is not NULL)

select CandidateID
, string_agg(OwnerID,',') as OwnerIDs
, concat('[', string_agg(case when rn = 1 then concat('{"ownerId":"', OwnerID, '","primary":"true","ownership":"0"}')
	else concat('{"ownerId":"', OwnerID,'"}') end, ','),']') as CandidateOwners
from CandidateOwnerOrder
group by CandidateID