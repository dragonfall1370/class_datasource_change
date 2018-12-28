select replace(replace(trim('.,!/ '  from '+46 (0) 21-10 13 00 / +46 (0) 16-15 36 0 ..'), ' ', ''), '/', ',')

select * from Account where charindex('://', WEBSITE)

select charindex('://', '://www.exellent.be/')
select
left(
	right(
		'https://www.bordet.be/en/homepage'
		, len('https://www.bordet.be/en/homepage') - charindex('://', 'https://www.bordet.be/en/homepage') - 2)
	, charindex(
		'/',
		right(
			'https://www.bordet.be/en/homepage'
			, len('https://www.bordet.be/en/homepage') - charindex('://', 'https://www.bordet.be/en/homepage') - 2
		)
	) - 1
)
select charindex('://', 'http://www.revalidatie-friesland.nl')

select * from VC_Countries where Name like 'united%'

select count(*) from Account where len(trim(isnull(website, ''))) = 0 -- 2986

select count(*) from Account -- 3362

select 3362 - 2986 -- 376

select * from Attachment
where ParentId in (
	select Id from Account
)

select * from Document
where AuthorId in (
	select Id from Account
)

-- \/:*?"<>|

select replace('\/:*?"<>|', '?', '_')

--select * from RecordType

select * from [User]

select * from Account

select * from Document

select * from Account -- 3362
select distinct [Name] from Account -- 3291
--select 3362 - 3291 -- 71

select * from Account where [Name] = 'Bonprix'

select Id, [Name], PARENTID, PHONE, FAX, AVTRRT__VMS_WEBSITE__C, AVTRRT__USER__C, AVTRRT__TERMS__C, AVTRRT__SYMBOL__C, ACCOUNTSOURCE
from Account
where len(trim(isnull(phone, ''))) = 0

select * from Account where id = '0010X000045iDeCQAU'

+ 33 1 46 17 70 00 , 0805 800 023

+44 (0) 20 7873 3000.


select trim('.,!/ '  from '+31 20 240 1660 ..')

select 