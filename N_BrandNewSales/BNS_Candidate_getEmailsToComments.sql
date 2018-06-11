-- SELECT e.id, e.name as Email_Subject, e.date_sent, concat(tc.first_name,' ',tc.last_name) as candidate_name, tc.id
-- from emails e left join emails_beans eb on e.id = eb.email_id
-- left join temp_candidates tc on eb.bean_id = tc.id
-- where tc.id is not null
-- ORDER BY e.date_sent DESC
-- limit 10

select concat('BNS_',tc.id) as CandidateExternalId, -10 as userId
		 , e.date_entered as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'candidate' as type-- ,1 as RelatedStatus
		 ,ltrim(concat(
		 	'Migrated from: EMAILS',
		 	if(e.name = '' or e.name is null, '', concat(char(10), 'Subject: ', e.name)),
		 	if(e.name = '' or e.name is null, '', concat(char(10), 'Date Sent: ', e.date_sent))))
		 	as 'CommentContent'
from emails e left join emails_beans eb on e.id = eb.email_id
left join temp_candidates tc on eb.bean_id = tc.id
where tc.id is not null
-- order by e.date_sent desc