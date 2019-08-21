
select a.reference as external_id,
concat(
nullif(concat('Activity Type: ',a.reftype,(char(13)+char(10))),concat('Acitivity Type: ',(char(13)+char(10)))),
'Candidate Name: ', a.cand_last, ' ' , a.cand_first, (char(13)+char(10)),
nullif(concat('Quick Result Comment: ',a.result,(char(13)+char(10))),concat('Quick Result Comment: ',(char(13)+char(10)))),
nullif(concat('Ref Notes: ',(char(13)+char(10)),a.RefNotes),concat('Ref Notes: ',(char(13)+char(10))))
) as 'Content',
a.DateEnter as insert_timestamp,
'comment' as 'category', 
'job' as 'type', 
-10 as 'user_account_id'
from refs a where a.reference <> 0
