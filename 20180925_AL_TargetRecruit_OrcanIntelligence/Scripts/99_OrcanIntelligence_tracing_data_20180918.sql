select count(*) from Account
--where len(trim(isnull(OwnerId, ''))) = 0
where OwnerId not in (
	select Id from [User]
)

select count(*) from [User]
where Id not in (
	select OwnerId from Account
)

select count(*) from AVTRRT__Job__c
where
--(len(trim(isnull(AVTRRT__Account_Job__c, ''))) = 0
--and len(trim(isnull(AVTRRT__Hiring_Manager__c, ''))) = 0)
--or
(len(trim(isnull(AVTRRT__Hiring_Manager__c, ''))) = 0
and len(trim(isnull(AVTRRT__Account_Job__c, ''))) > 0
and AVTRRT__Account_Job__c in (
	select Id from Account
where Id not in (
	select AccountId from [Contact]
)
))

select Id, AVTRRT__Job_Title__c, AVTRRT__Account_Job__c, AVTRRT__Hiring_Manager__c from AVTRRT__Job__c
where
(len(trim(isnull(AVTRRT__Account_Job__c, ''))) = 0
and len(trim(isnull(AVTRRT__Hiring_Manager__c, ''))) = 0)
or
(len(trim(isnull(AVTRRT__Hiring_Manager__c, ''))) = 0
and len(trim(isnull(AVTRRT__Account_Job__c, ''))) > 0
and AVTRRT__Account_Job__c in (
	select Id from Account
where Id not in (
	select AccountId from [Contact]
)
))

select Id from Account
where Id not in (
	select AccountId from [Contact]
)

select * from Contact where AccountId = '001b0000043FDC0AAO'

select Id from AVTRRT__Job__c where
len(trim(isnull(AVTRRT__Account_Job__c, ''))) = 0
and IsDeleted = 0

select Id from AVTRRT__Job__c where
len(trim(isnull(AVTRRT__Account_Job__c, ''))) = 0
and len(trim(isnull(AVTRRT__Hiring_Manager__c, ''))) = 0

select * from AVTRRT__Job__c where Id in (
	'a0Fb000000YDxFoEAL',
	'a0Fb000000YDybNEAT',
	'a0Fb000000c3JCxEAM'
)

select * from [User] where Id = '005b0000006KZLxAAO'

--select * from [FCMS__CMSProfile__c] where Id = '00eb00000011j50AAA'

select count(*) from Contact
where
RecordTypeId =
--'012b0000000J2REAA0' -- contact
'012b0000000J2RDAA0' -- candidate
and AccountId not in (
	select Id from Account
)

select * from RecordType

select count(*) from AVTRRT__Job__c
where OwnerId not in (
	select Id from [User]
)

select count(*) from AVTRRT__Job__c
where
len(trim(isnull(AVTRRT__Hiring_Manager__c, ''))) = 0
and 
AVTRRT__Hiring_Manager__c not in (
	select Id from [Contact]
)

select Id, AVTRRT__Hiring_Manager__c from AVTRRT__Job__c
where
len(trim(isnull(AVTRRT__Hiring_Manager__c, ''))) > 0
and 
AVTRRT__Hiring_Manager__c in (
	select Id from [Contact]
	where RecordTypeId =
	--'012b0000000J2RE' -- contact
	'012b0000000J2RD' -- candidate
)




select * from RecordType

select * from Contact where Id = '0030X000023FUeCQAW'

select distinct RecordTypeId from Contact

--012b0000000J2RDAA0
--012b0000000J2REAA0

--012b0000000J2RD
--012b0000000J2RE

select count(*) from AVTRRT__Job__c

select top 10 * from AVTRRT__Job__c

select * from Document where Id = '069b0000003LJn6AAG'

select * from ContentDocumentLink

select count(*) from Contact
where Id in (
	select LinkedEntityId from ContentDocumentLink
)

select * from AVTRRT__Job_Applicant__c where Id = '0F9b00000004stsCAA'

--select * from DATA__c

select * from Document

select * from [User]

select top 10 * from Account

select top 10 * from ContentVersion
where VersionNumber <> 1
-- 0
-- 11922

select top 10 * from ContentVersion

select * from Document

select count(*) from ContentDocumentLink
-- 23837

--select count(*) from Document
--where AuthorId not in (
--	select Id from [User]
--)

select top 10 * from ContentDocumentLink
select * from ContentDocumentLink where LinkedEntityId = ''
select count(*) from ContentDocumentLink
where LinkedEntityId in (
	select Id from [User]
)
-- in User 11922
-- not in 11915
-- total 23837

select * from [Case]


select * from Attachment
-- 16409

select count(*) from Attachment
where ParentId in (
	select Id from [Note]
)
-- Job Application 1114
-- Contact 12063
-- Task 3070
-- Event 0

select 1114 + 12063 -- 13177
select 16049 - 13177 -- 2872

select top 10 * from Task

select * from TaskPriority

select * from TaskStatus

select top 10 * from EmailMessage

select * from Document where Id = '069b0000003LJn6AAG'

select
AccountId,
STRING_AGG(
	replace(
		replace(trim(isnull([Name], '')), ',', '_')
		, ' ', '_'
	)
	, ',') as Docs
from Attachment
where
len(trim(isnull(AccountId, ''))) > 0
and trim(isnull(AccountId, '')) <> '000000000000000AAA'
group by AccountId


--select Id from Attachment
--where

--	len(trim(isnull(ParentId, ''))) = 0
--	or trim(isnull(ParentId, '')) = '000000000000000AAA'

select count(*) from Attachment

select * from Attachment
where ParentId in (
	select Id from Contact
	where RecordTypeId =
	--'012b0000000J2RE'
	'012b0000000J2RD'
)

select count(*) from Contact c
where
len(trim(isnull(c.AccountID, ''))) = 0
or trim(isnull(c.AccountID, '')) = '000000000000000AAA'
or trim(isnull(c.AccountID, '')) = '000000000000000000'

select * from Contact c
where
(len(trim(isnull(c.AccountID, ''))) = 0
or trim(isnull(c.AccountID, '')) = '000000000000000AAA'
or trim(isnull(c.AccountID, '')) = '000000000000000000'
)
and RecordTypeId = '012b0000000J2RE'

select count(*) from AVTRRT__Job__c
where AVTRRT__Hiring_Manager__c in (
select Id from Contact c
where
(len(trim(isnull(c.AccountID, ''))) = 0
or trim(isnull(c.AccountID, '')) = '000000000000000AAA')
and RecordTypeId = '012b0000000J2RE'
)

select * from Contact c
where
(len(trim(isnull(c.AccountID, ''))) = 0
or trim(isnull(c.AccountID, '')) = '000000000000000AAA')
and RecordTypeId = '012b0000000J2RD'

select * from Contact where RecordTypeId = '012b0000000J2RE'

select distinct AccountId from Contact
where AccountId is null

	SELECT
		[ParentId]
		, string_agg([Name], ',') as Docs
	from [Attachment]
	WHERE [IsDeleted] = '0'
	GROUP BY [ParentId]

with
--TaskIdxs as (
--	select Id from Task
--)
ContactIdxs as (
	select Id from Contact
	where RecordTypeId = '012b0000000J2RE'
)

select
a.AccountId,
STRING_AGG(
	concat(a.AccountId, '-',
		replace(
			replace(trim(isnull(a.[Name], '')), ',', '_')
			, ' ', '_'
		)
	)
	, ',') as Docs
from Attachment a -- 16409
--left join TaskIdxs tis on a.ParentId = tis.Id
join ContactIdxs cis on a.ParentId = cis.Id
--join AccountIdxs ais on a.AccountId = ais.Id
where ais.Id <> '001b00000044tF3AAI'
group by AccountId

select count(*) from Contact c
where
[RecordTypeId] =
--'012b0000000J2RD' --candidate -- 9049 => 8844 has attachment
'012b0000000J2RE' -- contact -- 8427 => 5 has attachment
--and cd.Docs is not null
and
(len(trim(isnull(c.AccountID, ''))) > 0
and trim(isnull(c.AccountID, '')) <> '000000000000000AAA'
)

select count(*) from Contact c
where
c.RecordTypeId = '012b0000000J2RE'
and len(trim(isnull(c.AccountID, ''))) > 0
and trim(isnull(c.AccountID, '')) <> '000000000000000AAA'
and trim(isnull(c.AccountID, '')) <> '001b00000044tF3AAI'

select top 10 * from ContentVersion

select * from Attachment
where ParentId in (
	select Id from AVTRRT__Job__c
)

select * from Attachment
where ParentId in (
	select Id from AVTRRT__Job_Applicant__c
)

select * from Contact
where RecordTypeId = '012b0000000J2RE'

select * from AVTRRT__Job__c

AVTRRT__Country_Locale__c
Belgium
AVTRRT__Job_Description__c
In manchester, doing a migration into a new server ... 

AVTRRT__Job_Term__c
12 Months Contract

AVTRRT__Job_Summary__c


with
--TaskIdxs as (
--	select Id from Task
--)
AppIdxs as (
	select Id from AVTRRT__Job_Applicant__c
	where IsDeleted = 0
)

select
cis.Id as AppId,
STRING_AGG(
	concat(a.Id, '-',
		replace(
			replace(trim(isnull(a.[Name], '')), ',', '_')
			, ' ', '_'
		)
	)
	, ',') as Docs
from Attachment a -- 16409

select * from Attachment
--left join TaskIdxs tis on a.ParentId = tis.Id
join AppIdxs cis on a.ParentId = cis.Id
--join AccountIdxs ais on a.AccountId = ais.Id
--where ais.Id <> '001b00000044tF3AAI'
group by cis.Id

  select count(id) from ContentVersion
  select count(id) from Attachment
  select count(id) from Document

  select 11922 + 16409 + 20 - 28337
   -- 28351
   -- 28337
   -- 14