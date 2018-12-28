select x.userID, x.candidateID, x.type, x.skillIDList, x.businessSectorIDList, x.skillSet, x.customText15
from bullhorn1.Candidate x

select * from bullhorn1.Candidate x
where x.candidateID = 13157

select * from VCComs
select distinct [company-name] from VCComs

select * from VCCons

select * from VCJobs

select * from VCCans

select distinct [candidate-email] from VCCans

select
[candidate-externalId]
, [candidate-email]
, uc.email
, uc.email_old
, uc.email2
, uc.email3
from VCCans vcc
left join bullhorn1.BH_Candidate c on vcc.[candidate-externalId] = c.candidateID
left join bullhorn1.BH_UserContact uc on c.userID = uc.userID
where dbo.ufn_CheckEmailAddress([candidate-email]) = 0

select * from VCCans
--where isDeleted = 0
order by [candidate-externalId] -- this is a MUST there must be ORDER BY statement
-- the paging comes here
OFFSET     85000 ROWS       -- skip N rows
FETCH NEXT 30000 ROWS ONLY; -- take M rows

select * from VCApplications

select dbo.ufn_CheckEmailAddress('-0309-0762730198-moshomotlou@gmail.com')


select dbo.ufn_CheckEmailAddress('*joey@gmtafrica.com')

select dbo.ufn_CheckEmailAddress('0843322684-0812644469-brightwellsiza@gmail.com')

select * from bullhorn1.BH_Candidate x
left join bullhorn1.BH_UserContact y on x.userID = y.userID
where y.email like '%malapanenicol@gamil.com%'
--'1632|0727548500 0837222914|

--update VCCans
--set [candidate-email] = 'malapanenicol@gmail.com'
--where [candidate-externalId] = 66641

--update VCCans
--set [candidate-email] = 'career+kr@jadeventer.com'
--where [candidate-externalId] = 6860

--update VCCans
--set [candidate-email] = 'vanessa.ackermann864@gmail.com'
--where [candidate-externalId] = 52631

select * from VCJobs
where [position-title] like '%Front End Developer%'

select distinct [position-title]
from VCJobs

select * from VCCans
where [candidate-email] =
--'imtiyaz20504@gmail.com'
--'clydonw@gmail.com'
--'mpprince17@gmail.com'
--'smith.aloysius713@gmail.com'
--'vanessa.ackermann864@gmail.com'
--'samantha.wright0@icloud.com'
--'dharma.rsa@gmail.com'
--'jennydavis1981@gmail.com'
--'Warrengoldsmith@ymail.com'
--'vusileeuw@gmail.com'
--'pjastral@yahoo.fr'
'kagisoafrica@gmail.com'
or
[candidate-PersonalEmail] =
--'imtiyaz20504@gmail.com'
--'clydonw@gmail.com'
--'mpprince17@gmail.com'
--'smith.aloysius713@gmail.com'
--'vanessa.ackermann864@gmail.com'
--'samantha.wright0@icloud.com'
--'dharma.rsa@gmail.com'
--'jennydavis1981@gmail.com'
--'Warrengoldsmith@ymail.com'
--'vusileeuw@gmail.com'
--'pjastral@yahoo.fr'
'kagisoafrica@gmail.com'

select * from VCCans
--where [candidate-email] like '%rsa%g%'
where [candidate-externalId] = 70485

--update VCCans
--set [candidate-email] = 'feziwemooi@yahoo.com'
--, [candidate-PersonalEmail] = 'imtiyaz20504@gmail.com'
--where [candidate-externalId] = 12035

--update VCCans
--set [candidate-email] = 'tommie.potgieter@supergrp.com'
--, [candidate-PersonalEmail] = 'Clydonw@gmail.com'
--where [candidate-externalId] = 8781

--update VCCans
--set [candidate-email] = 'Clydonw@gmail.com'
--, [candidate-PersonalEmail] = 'tommie.potgieter@supergrp.com'
--where [candidate-externalId] = 8780

--update VCCans
--set [candidate-email] = 'smith.aloysius713+2@gmail.com'
----, [candidate-PersonalEmail] = 'tommie.potgieter@supergrp.com'
--where [candidate-externalId] = 59007

--update VCCans
--set [candidate-email] = 'mpprince17+2@gmail.com'
----, [candidate-PersonalEmail] = 'tommie.potgieter@supergrp.com'
--where [candidate-externalId] = 58114

--update VCCans
--set [candidate-email] = 'vanessa.ackermann864+2@gmail.com'
----, [candidate-PersonalEmail] = 'tommie.potgieter@supergrp.com'
--where [candidate-externalId] = 77689

--select * from bullhorn1.BH_Candidate x
--left join bullhorn1.BH_UserContact y on x.userID = y.userID
--where candidateID in (52631, 77689)

--update VCCans
--set [candidate-email] = '(2)samantha.wright0@icloud.com'
----, [candidate-PersonalEmail] = 'tommie.potgieter@supergrp.com'
--where [candidate-externalId] = 65695

--update VCCans
--set [candidate-email] = 'jennydavis1981+2@gmail.com'
----, [candidate-PersonalEmail] = 'tommie.potgieter@supergrp.com'
--where [candidate-externalId] = 87096

--update VCCans
--set [candidate-email] = '(2)Warrengoldsmith@ymail.com'
----, [candidate-PersonalEmail] = 'tommie.potgieter@supergrp.com'
--where [candidate-externalId] = 87102

--update VCCans
--set [candidate-email] = 'vusileeuw+2@gmail.com'
----, [candidate-PersonalEmail] = 'tommie.potgieter@supergrp.com'
--where [candidate-externalId] = 92443

--update VCCans
--set [candidate-email] = '(2)pjastral@yahoo.fr'
----, [candidate-PersonalEmail] = 'tommie.potgieter@supergrp.com'
--where [candidate-externalId] = 93506

--update VCCans
--set [candidate-email] = '(3)pjastral@yahoo.fr'
----, [candidate-PersonalEmail] = 'tommie.potgieter@supergrp.com'
--where [candidate-externalId] = 93507

--update VCCans
--set [candidate-email] = 'kagisoafrica+2@gmail.com'
----, [candidate-PersonalEmail] = 'tommie.potgieter@supergrp.com'
--where [candidate-externalId] = 93943

select * from VCApplications -- 5163
where 
[application-positionExternalId] = 16
and [application-candidateExternalId] = 20

select distinct
[application-positionExternalId]
, [application-candidateExternalId]
from VCApplications -- 5141

--select 5163 - 5141 -- 22

select * from bullhorn1.BH_JobPosting jp
where jp.jobPostingID = 1

select * from bullhorn1.BH_JobResponse jr
where jr.jobPostingID = 1
and isDeleted = 0
and status <> 'New Lead'

select * from VCApplications
where [application-positionExternalId] = 44
and [application-candidateExternalId] = 5382

select * from bullhorn1.BH_Candidate where candidateID = 5382

select * from bullhorn1.BH_JobResponse jr
where jr.jobPostingID = 44
and jr.userID = 5743
and isDeleted = 0


;with
source (ID,source) as (
	select
		candidateID
		, trim(isnull(source.value, '')) as source
	from bullhorn1.Candidate m CROSS APPLY STRING_SPLIT(trim(isnull(m.source, '')),',') AS source
)
select distinct trim(isnull(source, '')) from source where trim(isnull(source, '')) > 0

;with CanSources as (
	SELECT trim(isnull(source.value, '')) as sourceName
	from bullhorn1.Candidate m
	CROSS APPLY STRING_SPLIT(trim(isnull(m.source, '')),',') AS source
)

select
1 as source_type
, 11 as payment_style
, current_timestamp as insert_timestamp
, x.sourceName
from (
	select distinct(trim(isnull(sourceName, ''))) sourceName
	from CanSources
	where len(trim(isnull(sourceName, ''))) > 0
) x
order by x.sourceName

select
	  C.candidateID as 'candidate-externalId'
--       , Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
--       , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
       , trim(iif(
			charindex(',', trim(isnull(c.source, 'Other'))) > 0
			, left(trim(isnull(c.source, 'Other')), charindex(',', trim(isnull(c.source, 'Other'))) - 1)
			, trim(isnull(c.source, 'Other'))
		)) as 'candidate-source'
-- select count (*) -- select distinct ltrim(rtrim(source))
from bullhorn1.Candidate C
where source <> '' and C.isPrimaryOwner = 1

with
ind (clientCorporationID,ind) as (SELECT clientCorporationID, ind.value as ind FROM bullhorn1.BH_ClientCorporation m CROSS APPLY STRING_SPLIT( ltrim(rtrim( convert(varchar(100),m.businessSectorList) )), ';') AS ind)

select
       distinct ind as 'company-industry'
       , current_timestamp as insert_timestamp
from ind
where ind <> ''
order by ind

;with
inds1 as (
	select
	--clientCorporationID
	trim(' ;' from isnull(cast(businessSectorList as nvarchar(255)), '')) as indNames
	from bullhorn1.BH_ClientCorporation
	where len(trim(' ;' from isnull(cast(businessSectorList as nvarchar(255)), ''))) > 0 
)

, inds2 as (
	select distinct trim(' ;' from isnull(value, '')) as indName
	from inds1
	cross apply string_split(indNames, ';')
)

select
indName
, current_timestamp as insert_timestamp
from inds2
order by indName

-- BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry)
with
  BusinessSector0(userid, businessSectorID) as (SELECT userid, a.value as ind FROM  bullhorn1.Candidate m CROSS APPLY STRING_SPLIT( ltrim(rtrim( convert(varchar(100),m.businessSectorIDList) )), ',') AS a where isPrimaryOwner = 1 )
, BusinessSector(userId, BusinessSector) as (SELECT userId, ltrim(rtrim(BSL.name)) as BusinessSector from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID WHERE BusinessSector0.businessSectorID <> '' )
select distinct BusinessSector, current_timestamp as insert_timestamp from BusinessSector

;with
inds1 as (
	select
	--clientCorporationID
	trim(' ;' from isnull(cast(businessSectorIDList as nvarchar(255)), '')) as indIds
	from bullhorn1.Candidate
	where len(trim(' ;' from isnull(cast(businessSectorIDList as nvarchar(255)), ''))) > 0
	and isPrimaryOwner = 1
)

, inds2 as (
	select distinct trim(' ;' from isnull(value, '')) as indId
	from inds1
	cross apply string_split(indIds, ',')
)

, inds3 as (
	select
	*
	from inds2 x join bullhorn1.BH_BusinessSectorList y on x.indId = y.businessSectorID
)

select
indId
, current_timestamp as insert_timestamp
from inds2
order by indId

select * from bullhorn1.BH_BusinessSectorList
order by name