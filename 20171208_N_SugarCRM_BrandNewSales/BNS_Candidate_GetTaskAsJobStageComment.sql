-- CREATE TABLE temp_can_jobapp_updated2 (
-- candidateExternalId CHAR(60),
-- userId int,
-- commentTimestamp DATETIME,
-- insertTimeStamp DATETIME,
-- assignedUserId int,
-- relatedStatus int,
-- commentContent NVARCHAR(max)
-- )
-- 
-- insert into temp_can_jobapp_updated2
select concat('BNS_',c.id) as CandidateExternalId, -10 as userId, t.date_entered as CommentTimestamp
		, t.date_entered as InsertTimeStamp, -10 as AssignedUserId, 1 as RelatedStatus
		,ltrim(concat(
			'PRESELECTION' 
			, if(t.name = '' or t.name is null, '', concat(char(10),'Task Name: ', t.name))
         , if(t.date_entered is null,'', concat(char(10),'Date Entered: ', t.date_entered))
         , if(pp.name = '' or pp.name is null,'', concat(char(10),'Preselectie Name: ', pp.name))
			, if(o.name = '' or o.name is null,'', concat(char(10),'Opportunity Name: ', o.name))
		))
		as 'CommentContent'
from tasks_cstm tc left join tasks t on tc.task_id_c = t.id
						left join prmn_preselectie pp on tc.project_id_c = pp.id
						left join opportunities o on tc.opportunity_id_c = o.id
						left join contacts c on t.contact_id = c.id
where ((tc.project_id_c is not null and tc.project_id_c <> '') or (tc.opportunity_id_c is not null and tc.opportunity_id_c <> ''))
and (t.contact_id is not null and t.contact_id <> '') 