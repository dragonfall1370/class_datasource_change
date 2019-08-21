select ClntAgencyNotes.clientid as external_id, 
chunk as 'content',
ClntAgencyNotes.timestamp as 'insert_timestamp',
'comment' as 'category', 
'company' as 'type', 
-10 as 'user_account_id'

from ClntAgencyNotes left join longtextcache on ClntAgencyNotes.agencynotes = longtextcache.id