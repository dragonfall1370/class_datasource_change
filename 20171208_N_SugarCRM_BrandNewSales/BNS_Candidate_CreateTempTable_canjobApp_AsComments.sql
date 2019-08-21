CREATE TABLE temp_can_jobapp_updated2 (
candidateExternalId CHAR(60),
userId int,
commentTimestamp DATETIME,
insertTimeStamp DATETIME,
assignedUserId int,
relatedStatus int,
commentContent NVARCHAR(max)
)

insert into temp_can_jobapp_updated2
select concat('BNS_',c.id) as CandidateExternalId, -10 as userId, rr.date_entered as CommentTimestamp
		, rr.date_entered as InsertTimeStamp, -10 as AssignedUserId, 1 as RelatedStatus
		,ltrim(concat(
			'PRESELECTION',
			if(rr.name = '' or rr.name is null, '', concat(char(10),'Recruitment Name: ', rr.name)),
			if(rr.date_entered is null,'', concat(char(10),'Date Entered: ', rr.date_entered)),
			if(rr.date_modified is null,'', concat(char(10),'Date Modified: ', rr.date_modified)),
			if(rr.assigned_user_id = '' or rr.assigned_user_id is null,'', concat(char(10),'Assigned User: ', u.username)),
			if(rrc.sourcing_status_c = '' or rrc.sourcing_status_c is null,'', concat(char(10),'Sourcing Status: ', rrc.sourcing_status_c)),
			if(pp.id = '' or pp.id is null,'', concat(char(10),'Preselectie_ida: ', pp.id)),
			if(pp.id = '' or pp.id is null,'', concat(char(10),'Talent Pool (Preselectie Name): ', pp.name)),
			if(rrc.account_id_c = '' or rrc.account_id_c is null,'', concat(char(10),'Company Name: ', a.name)),
			if(o.id = '' or o.id is null,'', concat(char(10),'Job Title: ', o.name)),
			if(rrc.datum_geplaatst_c is null,'', concat(char(10),'Datum Geplaatst: ', rrc.datum_geplaatst_c)),
			if(rrc.status_sollicitatieprocedure_c = '' or rrc.status_sollicitatieprocedure_c is null,'', concat(char(10),'Job Stage (Status Sollicitatieprocedure): ', rrc.status_sollicitatieprocedure_c)),
			if(rrc.status_kandidaat_c = '' or rrc.status_kandidaat_c is null,'', concat(char(10),'Status Kandidaat: ', rrc.status_kandidaat_c)),
			if(rr.description = '' or rr.description is null,'', concat(char(10),'Description: ',char(10), rr.description))
		) )
		as 'CommentContent'
from recru_recruitment rr 
		left join recru_recruitment_cstm rrc on rr.id = rrc.id_c
        left join prmn_preselectie pp on rrc.project_id_c = pp.id
        left join contacts c on rrc.kandidaat_rel_name_c = concat(c.first_name, ' ', last_name)
        left join opportunities o on rrc.opportunity_id1_c = o.id
        left join accounts a on rrc.account_id_c = a.id
		left join user_info_view u on rr.assigned_user_id = u.id
where -- rrc.opportunity_id1_c is not null and 
rrc.kandidaat_rel_name_c is not null and c.id not in (select contact_id from accounts_contacts) -- limit 10
order by rrc.kandidaat_rel_name_c

select * from temp_can_jobapp_updated2