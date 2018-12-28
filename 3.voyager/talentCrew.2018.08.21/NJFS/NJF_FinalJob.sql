with
--DUPLICATE JOB title
temp as (select *,replace(replace(positionexternalId,'NJFS','NJF1S'),'NJFGTP','NJF2GTP') as externalId
from importJob)

, dup as (select positionexternalId, positiontitle, ROW_NUMBER() OVER(PARTITION BY positiontitle ORDER BY externalId ASC) AS rn
from temp)
--select * from importJob
--select * from ContactMaxID order by CompanyId

--MAIN SCRIPT
--insert into importJob
select 
positioncontactId as 'position-contactId'
, CompanyID-- as 'CompanyID'
, MainContactId-- as 'MainContactId'
, ContactID-- as 'ContactID'
, j.positionexternalId as 'position-externalId'
, positiontitleold as 'position-title(old)'
, iif(j.positionexternalId in (select positionexternalId from dup where dup.rn > 1)
		, case 
		when left(j.positionexternalId,4) like 'NJFS' then concat('NJF Search - ',dup.positiontitle)
		when left(j.positionexternalId,4) like 'NJFC' then concat('NJF Contract - ',dup.positiontitle)
		else concat('NJF GTP - ',dup.positiontitle)
		end
	, ltrim(j.positiontitle)) as 'position-title'
, positionheadcount as 'position-headcount'
, positioncurrency as 'position-currency'
, positiontype as 'position-type'
, positionowners as 'position-owners'
, positioninternalDescription as 'position-internalDescription'
, positioncomment as 'position-comment'-- this field is not supported importing so have to inject
, positionstartDate as 'position-startDate'
, positiondocument as 'position-document'
, positionendDate as 'position-endDate'
, positionnote as 'position-note'
from importJob j left join dup on j.positionexternalId = dup.positionexternalId