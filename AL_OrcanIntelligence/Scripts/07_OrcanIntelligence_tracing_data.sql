--select * from Note
--where
--AccountId = '000000000000000AAA'

select * from Contact
where Id = '000000000000000AAA'

--select count(*) from EmailMessage

select * from Note
where Id = '0020X00000DL3MVQA1'

select Id, RecordTypeId from Contact
where Id = 'a0F0X00000fDLx5UAG'

select Id, Name from Account
where Id = 'a0F0X00000fDLx5UAG'

select Id, Username from [User]
where Id = 'a0F0X00000fDLx5UAG'

select Id, AVTRRT__Job_Title__c from [AVTRRT__Job__c]
where Id = 'a0F0X00000fDLx5UAG'

select * from Note
where ParentId in (
	select id from Account
)

select * from Task
where len(trim(isnull(WhoId, ''))) = 0

select * from Task
where WhoId in (
	select id from Contact
)

select count(*) from TaskRelation

select * from TaskRelation
where RelationId in (
	select id from Account
)

select * from TaskRelation
where RelationId in (
	select id from Contact
)

select * from TaskRelation
where RelationId in (
	select id from AVTRRT__Job__c
)

select * from EmailMessage
where Id = 'a0Fb000000EfQQtEAN'

select top 10 * from EmailMessage

select * from Event

select * from Event where WhoId in (
	select Id from Contact where RecordTypeId = '012b0000000J2RE'
)

select * from EventRelation

select * from AVTRRT__Interview__c
where Id = 'a0C0X00000ocHW3UAM'

select * from RecordType
where Id = '012b0000000J2RG'

select * from Contact
where Id = '0030X000024VivqQAC'

select * from Contact
where Id = (select top 1 RelationId from EventRelation where EventId = '00U0X00000URp9kUAD')

select * from Account where Id = '001b00000044tF3AAI'
select * from Account where Id = '001b000003TWDHHAA5'