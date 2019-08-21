select count(*) from EmailMessage
--44453

where ActivityId is not null
and
ActivityId <> '000000000000000AAA'
-- 43963 

--select 44453 - 43963 -- 490

select
--count(*)
*
from EmailMessage
where RelatedToId is not null
and
RelatedToId <> '000000000000000AAA'
--11965
and RelatedToId in (
	select Id from Account -- 4
	--select Id from Contact -- 0
	--select Id from AVTRRT__Job__c -- 11961
)

select
--count(*)
*
from EmailMessage
where RelatedToId is not null
and
ActivityId <> '000000000000000AAA'
--43943
and ActivityId in (
	--select Id from Task -- 37517
	--select Id from Event -- 0
)

--select 43943 - 37517 -- 6426

select * from Task
where Id in (
	'00T0X00002eIYmwUAG',
'00T0X00002eIYCxUAO',
'00T0X00002eIYKuUAO',
'00T0X00002eIYXAUA4'
)

select
Id
, Subject
, TextBody
from
EmailMessage
where


select top 10 * from EmailMessage

select top 10 * from Task

select top 10 * from [Event]