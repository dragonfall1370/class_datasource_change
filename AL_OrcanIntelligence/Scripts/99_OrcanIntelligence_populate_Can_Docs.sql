with
--TaskIdxs as (
--	select Id from Task
--)
ContactIdxs as (
	select Id
	--, AVTRRT__Resume_Attachment_Id__c
	--, AVTRRT__Resume_Name__c
	from Contact
	where RecordTypeId =
	--'012b0000000J2RE' -- Contact
	'012b0000000J2RD' -- Candidate
)

select
cis.Id as ContactId,
--a.Id,
--cis.AVTRRT__Resume_Attachment_Id__c,
--cis.AVTRRT__Resume_Name__c,
--a.Name
STRING_AGG(
	concat(a.Id, '-',
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
--where ais.Id <> '001b00000044tF3AAI'
group by cis.Id
--9938

--select * from ContentDocumentLink

--select * from ContentVersion

select
Id
, AVTRRT__Resume_Name__c
, AVTRRT__Resume_Attachment_Id__c
--, AVTRRT__Cover_Letter__c
--, AVTRRT__Parse_Resume_Migration_Id__c
--, AVTRRT__Resume__c
--, AVTRRT__Resume_Link__c
, AVTRRT__Resume_Received_Date__c
--, AVTRRT__ResumeRich__c
--, AVTRRT__Video_Resume_Link__c
--, AVTRRT__Phone__c
--, AVTRRT__Cell_Phone__c
--, AVTRRT__Online_Profile_Link__c
from Contact
where RecordTypeId =
	--'012b0000000J2RE' -- Contact
	'012b0000000J2RD' -- Candidate
and Id = '0030X000024W7rDQAS'

select * from Attachment
where ParentId = '0030X000024W7rDQAS'

--select count(*) from Attachment a -- 16409
--join Contact c on a.Id = c.AVTRRT__Resume_Attachment_Id__c
--where c.RecordTypeId =
--	--'012b0000000J2RE' -- Contact
--	'012b0000000J2RD' -- Candidate
---- 9854

--select count(*) from Contact c
--where c.RecordTypeId =
--	--'012b0000000J2RE' -- Contact
--	'012b0000000J2RD' -- Candidate
--and AVTRRT__Resume_Name__c is null
----and (c.AVTRRT__Resume_Attachment_Id__c is null and
-- 9854

-- 10172
--1536

--select 10172 - 1536 -- 8636

--select Id, AVTRRT__Cover_Letter__c from Contact where AVTRRT__Cover_Letter__c is not null
--and RecordTypeId =
--	--'012b0000000J2RE' -- Contact
--	'012b0000000J2RD' -- Candidate