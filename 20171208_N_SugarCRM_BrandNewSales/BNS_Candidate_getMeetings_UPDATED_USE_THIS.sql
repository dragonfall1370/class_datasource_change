select
concat('BNS_',c.id) as CandidateExternalId, -10 as userId
		, n.date_entered as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'candidate' as type-- ,1 as RelatedStatus
        -- , n.parent_type, n.parent_id, tc.id, tc.first_name, tc.last_name
		,ltrim(rtrim(concat(
				'Migrated from: MEETINGS',
                if(n.name = '' or n.name is null,'', concat(char(10), 'Subject: ', n.name)),
				if(n.date_entered is null,'', concat(char(10), 'Date/Time Created: ', n.date_entered)),
				if(uiv.username = '' or uiv.username is null,'', concat(char(10), 'Author: ', uiv.username)),
				if(uiv1.username = '' or uiv1.username is null,'', concat(char(10), 'Record Owner: ',  uiv1.username)),
				if(n.parent_type = '' or n.parent_type is null,'', concat(char(10),'Type: ', n.parent_type)),
                if(n.parent_type = 'Accounts', if(a.name = '' or a.name is null,'',concat(char(10), 'Company: ', a.name)), concat(char(10), 'Candidate: ', if(c.first_name = ''  or c.first_name is null, c.last_name, if(c.last_name = '' or c.last_name is null, c.first_name, concat(c.first_name,' ',c.last_name)))))
				, if(mc.type_gesprek_c = '' or mc.type_gesprek_c is null,'', concat(char(10), 'Meeting Type (Type Gesprek): ', mc.type_gesprek_c))
                , if(n.description = '' or n.description is null,'', concat(char(10), 'Content: ',char(10), n.description))
                -- , if(n.description = '' or n.description is null,'', concat(char(10), 'Content: ',char(10), n.description))
			))) as 'CommentContent'
from meetings_contacts mcs left join meetings n on mcs.meeting_id = n.id
			left join contacts c on mcs.contact_id = c.id
			left join user_info_view uiv on n.created_by = uiv.id
			left join user_info_view uiv1 on n.assigned_user_id = uiv1.id
			left join accounts a on n.parent_id = a.id
			-- left join contacts c on mcs.contact_id = c.id
            left join meetings_cstm mc on n.id = mc.id_c
where -- n.parent_id is not null and 
c.id is not null and n.date_entered is not null-- and n.parent_type in ('Accounts','Leads','Contacts')

-- select *
-- from meetings_contacts mc left join meetings m on mc.meeting_id = m.id
-- where mc.contact_id = '46c74018-5404-d32a-5340-58ac8527d464'
