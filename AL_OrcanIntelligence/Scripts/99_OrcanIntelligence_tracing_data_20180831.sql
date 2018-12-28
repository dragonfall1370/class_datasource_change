select * from AVTRRT__References__c

select * from AVTRRT__Search__c

select top 10 * from AVTRRT__Job_Applicant__c

select count(*) from Note

select count(*) from AVTRRT__Interview__c

select top 5 * from AVTRRT__Interview__c
order by Name desc

select 240 - 225

select * from Task t
where t.WhoID in (
	select Id from Contact c
	where c.RecordTypeID = '012b0000000J2RD'
)
select * from RecordType
where Id = '012b0000000J2RG'

select count(*) from Event

select count(*) from EventRelation

select count(*) from Task

select count(*) from TaskRelation

select count(*) from EmailMessage

select 

--select * from VincereCountryCodeDic
--where [Name] like 'Austria%'

--select count(*) from Contact
--where len(trim(isnull(MailingCountry, ''))) = 0

select
Id, PathOnClient
from ContentVersion
where IsDeleted = 0
order by PathOnClient

select
Id, Name
from Attachment

select
_Id_, _Name_
from [Document] 

select * from Contact
where Id = '003b0000020VInSAAW'

select * from AVTRRT__Job__c
where AVTRRT__Hiring_Manager__c = '003b0000020VInSAAW'

--update Contact
--set Email = 'notparsed' + cast(checksum(Id) as varchar(20)) + '@na.com'
--where Email = 'notparsed@na.com'

select id, FirstName, LastName, Email, AVTRRT__New_Email__c, AVTRRT__Home_Email__c, AVTRRT__Other_Emails__c from Contact
where len(trim(isnull(email, ''))) = 0 and 'NoEmail-' + cast(checksum(id) as varchar(20)) + '@noemail.com' =
'NoEmail-1714285970@noemail.com'
--'pierlorenzo.paracchini@gmail.com'
--'niklas.domander@dice.se'
--'stefanie.wandt@mistralbs.com'
--'itsolkas@gmail.com'
and [RecordTypeId] =
--'012b0000000J2RD' --candidate -- 9049 => 8844 has attachment
'012b0000000J2RE'

--select email from contact where email like 'noemail%'
select * from contact where id = '003b000000Ri7wxAAB'
select * from contact where id = '003b000000Ri7y1AAB'


select checksum('003b000000Ri7wxAAB')
select checksum('003b000000Ri7y1AAB')


--select * from AVTRRT__Job__c
--where
--len(trim(isnull(AVTRRT__Job_Title__c, ''))) = 0
--AVTRRT__Job_Title__c = 'Senior oracle PL/SQL developer'

select * from AVTRRT__Job__c
where Id = 'a0F0X00000crdw6UAA'

--create table ApplicationStageOrder (
--	Stage varchar(255),
--	[Order] int
--)

--insert into ApplicationStageOrder values('SHORTLISTED', 1)
--insert into ApplicationStageOrder values('SENT', 2)
--insert into ApplicationStageOrder values('FIRST_INTERVIEW', 3)
--insert into ApplicationStageOrder values('SECOND_INTERVIEW', 4)
--insert into ApplicationStageOrder values('OFFERED', 5)
--insert into ApplicationStageOrder values('PLACEMENT_PERMANENT', 6)
--insert into ApplicationStageOrder values('PLACEMENT_CONTRACT', 7)
--insert into ApplicationStageOrder values('PLACEMENT_TEMP', 8)
--insert into ApplicationStageOrder values('ONBOARDING', 9)


--alter table Contact
--add EmailCorrected varchar(max) null

--with
--CandidateDupCheck as (
--	--select * from (
--	select Id, Email, row_number() over(partition by Email order by CreatedDate) as RowNum
--	from Contact
--	where
--	[RecordTypeId] = '012b0000000J2RD' and len(trim(isnull(Email, ''))) > 0
--	--) abc where abc.RowNum > 1
--)

select * from Account
where Id = '001b00000044tF3AAI'

select * from AVTRRT__Interview__c
where Id = 'a0Fb000000VaXYAEA3'

select AVTRRT__CITY__C from AVTRRT__Job_Applicant__c
where Id = 'a0Fb000000VaXYAEA3'

select * from AVTRRT__Job__c
where Id = 'a0Fb000000VaXYAEA3'

select * from TaskRelation
where TaskId = '00T0X00002n2cNZUAY'

select count(*) from Contact

select count(*) from TaskRelation

--select count(*) from TaskRelation
--where IsWhat = 0

select count(*) from Contact c
where
c.RecordTypeId =
--'012b0000000J2RE'
'012b0000000J2RD'
and c.Id in (
	select RelationId from TaskRelation
)

select count(*) from TaskRelation

select count(*) from TaskRelation
where RelationId in (
	select Id from Contact
	where RecordTypeId =
	'012b0000000J2RE'
	--'012b0000000J2RD'
)
-- Con: 139

-- Can: 122

select count(*) from Task
--70116

select count(*) from Task
where WhoId in (
	select Id from Contact
	where RecordTypeId =
	--'012b0000000J2RE'
	'012b0000000J2RD'
)

select * from Task
where WhoId not in (
	select Id from Contact
)

select count(*) from Task
where WhoId in (
	select Id from Lead
)

select count(*) from Task
where WhatId in (
	select Id from Account
)

select count(*) from Task
where WhatId = '000000000000000AAA' or len(trim(isnull(WhatId, ''))) = 0

select count(*) from Task
where WhoId = '000000000000000AAA' or len(trim(isnull(WhatId, ''))) = 0

-- Job: 24095
-- App: 23
-- Acc: 726
-- null or dumb: 45272
--select 24095 + 23 + 726 -- 24844
--select 45272 + 24844 -- 70116



select * from EntityHistory

select * from AVTRRT__ETCObject__c
where Id = '001b000000j1ooyAAA'

-- Unknow: 1337

-- null or dumb: 1159

-- Con: 11130

-- Can: 57649

--select 11130 + 57649 -- 68779

--select 70116 - 68779 -- 1337

--select 139 + 122 -- 261

select count(*) from EmailMessage
where RelatedToId = '000000000000000AAA' or len(trim(isnull(RelatedToId, ''))) = 0

-- total: 41104
-- Related null or dumb: 30549
-- good link: 10555
--select 41104 - 30549

select count(*) from EmailMessage
where ParentId = '000000000000000AAA' or len(trim(isnull(ParentId, ''))) = 0

-- Parrent null or dumb: 41104

select count(*) from EmailMessage
where ActivityId = '000000000000000AAA' or len(trim(isnull(ActivityId, ''))) = 0

select count(*) from EmailMessage