select
concat('BNS_',ac.id) as ContactExternalId, -10 as userId
		, c.date_entered as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'contact' as type-- ,1 as RelatedStatus
		,ltrim(rtrim(concat('CV TEXT:',char(10),cc.cv_c))) as 'CommentContent'
from accounts_contacts ac left join contacts c on ac.contact_id = c.id
					 left join contacts_cstm cc on c.id = cc.id_c
where ac.contact_id is not null and cc.cv_c is not null and cc.cv_c <> ''