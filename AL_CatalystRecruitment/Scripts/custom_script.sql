-- company
with
companyIds as (
	select contactId from company
)

--select top 1 * from note -- 12,480

-- contactId
-- date
-- appUserId
-- text

--select
----count(*)
--*
--from note n join companyIds cs on n.contactId = cs.contactId -- 52

--select
--count(1)
----top 1 *
--from emailHistory -- 1,469,746

-- contactId
-- appUserId
-- date
-- body

select
count(*)
--*
from emailHistory n join companyIds cs on n.contactId = cs.contactId

-- 31

-- contact
declare @NewLineChar as char(2) = char(13) + char(10);

with
ContactIndexs AS (
	SELECT
	contactId
	FROM person
	WHERE (SELECT COUNT(VALUE) FROM STRING_SPLIT(tags, '|') WHERE UPPER(VALUE) = UPPER('client'))  = 1
)

--select * from person p where p.contactId = 44253

--select * from [person.tag] pts where pts.contactId in (

--	select p.contactId from person p where p.tags is null

--)

--select * from note n where n.contactId in (

--	select p.contactId from person p where p.tags is null

--)

--select * from [Persons.FirstConversationnotesID206] -- 13 recs
--select GETUTCDATE()
 --_Person_ID_
 --_Form_ID_
 --_Red_flags_or_important_notes_

--select n.* from [Persons.FirstConversationnotesID206] n join ContactIndexs pis on n._Person_ID_ = pis.contactId

-- 0

--select * from note -- 12,480

--select n.* from note n join ContactIndexs pis on n.contactId = pis.contactId -- 6,058

--select * from [Persons.ExtraInformationID205] -- 11,326 --
-- _Person_ID_
-- _Form_ID_
-- _Extra_Information_
-- _Where_did_you_hear_about_us__

--select
--_Person_ID_
--, concat_ws(@NewLineChar
--	, 'Extra Information: ' + trim(coalesce(_Extra_Information_, ''))
--	, 'Where did you hear about us: ' + trim(coalesce(_Where_did_you_hear_about_us__, '')))
--from [Persons.ExtraInformationID205] n join ContactIndexs pis on n._Person_ID_ = pis.contactId

-- 12

--select * from [Persons.clientvisitnotesID211] -- 110
-- _Person_ID_
-- _Form_ID_
-- _date_of_visit_
-- _client_visit_notes_

--select
--n._Person_ID_ as CR_extId
--, isnull(n._date_of_visit_, GETUTCDATE()) as CR_insertTimestamp
--, 'Client Visit Notes: ' + trim(isnull(n._client_visit_notes_, '')) as CR_content
--from [Persons.clientvisitnotesID211] n join ContactIndexs pis on n._Person_ID_ = pis.contactId

--select * from [Persons.ArrivaltraveldetailsID212] -- 43
-- _Person_ID_
-- _Form_ID_
-- _Arrival_date_
-- _Trip_details_

--select
--n._Person_ID_ as CR_extId
--, isnull(n._Arrival_date_, GETUTCDATE()) as CR_insertTimestamp
--, 'Trip details: ' + trim(isnull(n._Trip_details_, '')) as CR_content
--from [Persons.ArrivaltraveldetailsID212] n join ContactIndexs pis on n._Person_ID_ = pis.contactId

-- 0

--select * from [person.stepHistory] -- 59,098
-- contactId
-- appUserId
-- date
-- jobId
-- workflow
-- step
-- stepID

--select
--n.contactId as CR_extId
--, isnull(n.date, GETUTCDATE()) as CR_insertTimestamp
--, concat_ws(@NewLineChar
--	, 'Job ID: ' + trim(coalesce(cast(n.jobId as varchar(20)), ''))
--	, 'Workflow: ' + trim(coalesce(n.workflow, ''))
--	, 'Step: ' + trim(coalesce(n.step, ''))
--	, 'Step ID: ' + trim(coalesce(cast(n.stepID as varchar(20)), ''))
--) as CR_content
--from [person.stepHistory] n join ContactIndexs pis on n.contactId = pis.contactId

-- 565

select
--n.*
count(*)
from  [emailHistory] n join ContactIndexs pis on n.contactId = pis.contactId -- 2

-- Candidate

with
CandidateIndexs AS (
	SELECT
	contactId
	FROM person
	WHERE (SELECT COUNT(VALUE) FROM STRING_SPLIT(tags, '|') WHERE UPPER(VALUE) = UPPER('client'))  = 0
)
--select * from [Persons.FirstConversationnotesID206] -- 13

-- _Person_ID_
-- _Form_ID_
-- _Red_flags_or_important_notes_

--select
--_Person_ID_ as CR_extId
--, GETUTCDATE() as CR_insertTimestamp
--, 'Red Flags Or Important Notes: ' + trim(isnull(n._Red_flags_or_important_notes_, '')) as CR_content
----, u.emailAddress as CR_userEmail
--, -10 as CR_UserId
--from [Persons.FirstConversationnotesID206] n
--join CandidateIndexs pis on n._Person_ID_ = pis.contactId
----join [user] u on n.appUserId = u.appUserId

--select * from [Persons.ExtraInformationID205] -- 11,326 --
-- _Person_ID_
-- _Form_ID_
-- _Extra_Information_
-- _Where_did_you_hear_about_us__

--select count(*) from  [Persons.ExtraInformationID205] n join CandidateIndexs pis on n._Person_ID_ = pis.contactId -- 11,314

--select
--n._Person_ID_ as CR_extId
--, GETUTCDATE() as CR_insertTimestamp
--, concat_ws(@NewLineChar
--	, 'Extra Information: ' + trim(coalesce(_Extra_Information_, ''))
--	, 'Where did you hear about us: ' + trim(coalesce(_Where_did_you_hear_about_us__, ''))
--) as CR_content
----, u.emailAddress as CR_userEmail
--, -10 as CR_UserId
--from
--[Persons.ExtraInformationID205] n
--join CandidateIndexs pis on n._Person_ID_ = pis.contactId
----join [user] u on n.appUserId = u.appUserId

-- 11,314

--select * from note -- 12,480

--select
--n.contactId as CR_extId
--, n.date as CR_insertTimestamp
--, n.text as CR_content
----, u.emailAddress as CR_userEmail
--, -10 as CR_UserId
--from
--note n
--join CandidateIndexs pis on n.contactId = pis.contactId
----join [user] u on n.appUserId = u.appUserId

-- 6,370

--select
--n._Person_ID_ as CR_extId
--, isnull(n._date_of_visit_, GETUTCDATE()) as CR_insertTimestamp
--, 'Client Visit Notes: ' + trim(isnull(n._client_visit_notes_, '')) as CR_content
--from [Persons.clientvisitnotesID211] n join CandidateIndexs pis on n._Person_ID_ = pis.contactId

-- 5

--select * from [Persons.ArrivaltraveldetailsID212] -- 43


--select
--n._Person_ID_ as CR_extId
--, isnull(n._Arrival_date_, GETUTCDATE()) as CR_insertTimestamp
--, 'Trip details: ' + trim(isnull(n._Trip_details_, '')) as CR_content
----, u.emailAddress as CR_userEmail
--, -10 as CR_UserId
--from  [Persons.ArrivaltraveldetailsID212] n
--join CandidateIndexs pis on n._Person_ID_ = pis.contactId
----join [user] u on n.appUserId = u.appUserId

-- _Person_ID_
-- _Form_ID_
-- _Arrival_date_
-- _Trip_details_

-- 43

--select * from [person.stepHistory] -- 59,098

--select
--n.contactId as CR_extId
--, isnull(n.date, GETUTCDATE()) as CR_insertTimestamp
--, concat_ws(@NewLineChar
--	, 'Job ID: ' + trim(coalesce(cast(n.jobId as varchar(20)), ''))
--	, 'Workflow: ' + trim(coalesce(n.workflow, ''))
--	, 'Step: ' + trim(coalesce(n.step, ''))
--	, 'Step ID: ' + trim(coalesce(cast(n.stepID as varchar(20)), ''))
--) as CR_content
----, u.emailAddress as CR_userEmail
--, -10 as CR_UserId
--from [person.stepHistory] n
--join CandidateIndexs pis on n.contactId = pis.contactId
----join [user] u on n.appUserId = u.appUserId

-- 58,533

--select
--top 1 *
----count(*)
--from emailHistory -- 1,469,746

select
n.contactId as CR_extId
, n.date as CR_insertTimestamp
, n.body as CR_content
--, u.emailAddress as CR_userEmail
, -10 as CR_UserId
from
emailHistory n
join CandidateIndexs cs on n.contactId = cs.contactId
--join [user] u on n.appUserId = u.appUserId

-- contactId
-- appUserId
-- date
-- body

-- 1,447,498

--SELECT
--	contactId
--	FROM person
--	WHERE (SELECT COUNT(VALUE) FROM STRING_SPLIT(tags, '|') WHERE UPPER(VALUE) = UPPER('client'))  = 0

--	select top 1 * from company where len(tags) > 10

--	select top 1 * from person where len(tags) > 255

--	select count(*) from company where tags is null or len(trim(tags)) = 0

--	select
--	--count(*)
--	*
--	from person where tags is null or len(trim(tags)) = 0

--	select count(*) from [person.tag] pt where pt.contactId not in (select contactId from person)

--		select count(*) from [person.tag] pt where pt.tag is null or len(trim(pt.tag)) = 0

--with
--PersonTags as (
--	select
--	[contactId],
--	string_agg(pt.tag, '|') tags
--	from
--	[dbo].[person.tag] pt
--	group by [contactId]
--)

--select tags from PersonTags pts where pts.contactId in (
--	select
--	p.contactId
--	from person p where p.tags is null or len(trim(tags)) = 0
--)

--update person
--set tags = (
--	select tags from PersonTags pts where pts.contactId = contactId
--)
--where contactId in (
--	select
--	p.contactId
--	from person p where p.tags is null or len(trim(tags)) = 0
--)

--select * from [dbo].[person.tag] where contactId in(
--	select
--	p.contactId
--	from person p where p.tags is null or len(trim(tags)) = 0
--)

--select * from [dbo].[user] where emailAddress in (
--	'ahopkins@catalystrecruitment.co.nz'
--	,'pponder@catalystrecruitment.co.nz'
--)

---- company
--with
--companyIds as (
--	select contactId from company
--)

--select
--n.contactId as CR_extId
--, n.body as CR_content
--, n.date as CR_insertTimestamp
--, u.emailAddress as CR_userEmail
--from
--emailHistory n
--join companyIds cs on n.contactId = cs.contactId
--join [user] u on n.appUserId = u.appUserId

--select * from person where CHARINDEX(',', tags, 1) > 0

--select top 10 * from person-- where contactType <> 'person'

--select string_split(p.tags, ',') from person p

select count(1) from emailHistory

select * from note where contactId = 31156