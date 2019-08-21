-- get notes to injecct to Contact Activities tab as comments
-- select * from notes n left join accounts_contacts ac on n.contact_id = ac.contact_id
-- where n.contact_id is not null and ac.contact_id is not null

select
concat('BNS_',tc.id) as CandidateExternalId, -10 as userId
		, n.date_entered as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'candidate' as type-- ,1 as RelatedStatus
        -- , n.parent_type, n.parent_id, n.contact_id, tc.id, tc.first_name, tc.last_name
		,ltrim(rtrim(concat(
				'Migrated from: NOTES',
                if(n.name = '' or n.name is null,'', concat(char(10), 'Subject: ', n.name)),
				if(n.date_entered is null,'', concat(char(10), 'Date/Time Created: ', n.date_entered)),
				if(uiv.username = '' or uiv.username is null,'', concat(char(10), 'Author: ', uiv.username)),
				if(uiv1.username = '' or uiv1.username is null,'', concat(char(10), 'Record Owner: ',  uiv1.username)),
				if(n.parent_type = '' or n.parent_type is null,'', concat(char(10),'Data Set Type: ', n.parent_type)),
                -- if(n.parent_type = 'Accounts', if(a.name = '' or a.name is null,'',concat(char(10), 'Company: ', a.name)), concat(char(10), 'Contact: ', if(c.first_name = ''  or c.first_name is null, c.last_name, if(c.last_name = '' or c.last_name is null, c.first_name, concat(c.first_name,' ',c.last_name))))),
				if(n.description = '' or n.description is null,'', concat(char(10), 'Content: ',char(10), n.description))
			))) as 'CommentContent'
from notes n left join Temp_Candidates tc on n.contact_id = tc.id
			left join user_info_view uiv on n.created_by = uiv.id
			left join user_info_view uiv1 on n.assigned_user_id = uiv1.id
			-- left join accounts a on n.parent_id = a.id
			-- left join contacts c on n.parent_id = c.id
where n.contact_id is not null and tc.id is not null
-- where n.parent_id is not null and tc.id is not null

-- name: subject
-- date_entered: date/time created
-- created by: author
-- description: content
-- assigned_user_id: record owner
-- parent_type: use to determine data set
-- parent_id: candidate id
-- contact_id: 

-- select * from notes n left join accounts_contacts ac on n.parent_id = ac.contact_id
-- where n.parent_id is not null and ac.contact_id is not null