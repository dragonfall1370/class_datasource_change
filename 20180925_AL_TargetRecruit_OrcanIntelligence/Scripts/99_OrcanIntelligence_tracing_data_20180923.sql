select count(*) from EmailMessage x -- 44453
where
x.IsDeleted = 0 and
(
	len(trim(isnull(x.ActivityId, ''))) > 0
	and trim(isnull(x.ActivityId, '')) <> '000000000000000AAA'
	-- 11965
	and trim(isnull(x.ActivityId, '')) in (
		select Id from
		Task -- 37517
		--[Event] -- 0
		--Account -- 4
		--Contact where RecordTypeId = '012b0000000J2RE' -- 0
		--Contact where RecordTypeId = '012b0000000J2RD' -- 0
		--AVTRRT__Job__c -- 11961
	)
)
--order by x.CreatedDate

--select 44453 - 37517 -- 6936

select * from AVTRRT__ETCObject__c x
where
len(trim(isnull(x.AVTRRT__Candidate__c, ''))) > 0
	and trim(isnull(x.AVTRRT__Candidate__c, '')) <> '000000000000000AAA'

-- 10784

select count(*) from Task x
where
x.IsDeleted = 0 and
(
	len(trim(isnull(x.WhatId, ''))) > 0
	and trim(isnull(x.WhatId, '')) <> '000000000000000AAA'
)
and WhatId in (
	select Id from Account -- 744
	--select Id from AVTRRT__Job__c -- 23899
)

select count(*) from Task x
where
x.IsDeleted = 0 and
(
	len(trim(isnull(x.WhoId, ''))) > 0
	and trim(isnull(x.WhoId, '')) <> '000000000000000AAA'
)
and WhoId in (
	--select Id from Contact where RecordTypeId = '012b0000000J2RE' -- 11997
	select Id from Contact where RecordTypeId = '012b0000000J2RD' -- 61956
)

