select rrc.opportunity_id1_c,rrc.status_sollicitatieprocedure_c ,rrc.kandidaat_rel_name_c,c.first_name, c.last_name, c.id
from recru_recruitment_cstm rrc 
left join contacts c on rrc.kandidaat_rel_name_c = concat(c.first_name, ' ', c.last_name)
where opportunity_id1_c is not null and rrc.kandidaat_rel_name_c is not null
order by rrc.opportunity_id1_c


select * from contacts
select * from recru_recruitment_cstm where opportunity_id1_c is not null
select distinct(kandidaat_rel_name_c) from recru_recruitment_cstm
where opportunity_id1_c is not null


create view temp_jobApp as
(select rr.id,rrc.opportunity_id1_c,rrc.kandidaat_rel_name_c, max(rr.date_modified) as latestAction-- , rrc.opportunity_id1_c,rrc.status_sollicitatieprocedure_c ,rrc.kandidaat_rel_name_c
from recru_recruitment rr left join recru_recruitment_cstm rrc on rr.id = rrc.id_c
where opportunity_id1_c is not null and rrc.kandidaat_rel_name_c is not null
group by rrc.opportunity_id1_c,rrc.kandidaat_rel_name_c)

select * from temp_jobApp -- where opportunity_id1_c in (select id from opportunities)
order by kandidaat_rel_name_c

select tj.*, rrc.status_sollicitatieprocedure_c, c.first_name, c.last_name, c.id as candidateId
from temp_jobApp tj 
	left join recru_recruitment_cstm rrc on tj.id = rrc.id_c
	left join contacts c on tj.kandidaat_rel_name_c = concat(c.first_name, ' ', c.last_name)
    

select concat('BNS_',tj.opportunity_id1_c) as 'application-positionExternalId',
		concat('BNS_',c.id) as 'application-candidateExternalId',
        case rrc.status_sollicitatieprocedure_c
			when 'Voorgesteld' then 'SENT'
            when 'Eerstegesprek' then '1ST_INTERVIEW'
            when 'Tweedegesprek' then '2ND_INTERVIEW'
            when 'Derdegesprek' then '2ND_INTERVIEW'
            when 'Arbeidsvoorwaardelijk' then 'OFFERED'
            when 'Test_Assesment' then 'OFFERED'
            else 'SHORTLISTED' END AS 'application-stage'
from temp_jobApp tj
	left join recru_recruitment_cstm rrc on tj.id = rrc.id_c
	left join contacts c on tj.kandidaat_rel_name_c = concat(c.first_name, ' ', c.last_name)
where c.id not in (select contact_id from accounts_contacts)

-- -------------------------------
select * from temp_candidates
select * from prmn_preselectie


select pprr.*, rr.name recruitmentname, pp.name as preselectie, pp1.name as project, rrc.kandidaat_rel_name_c candidateName
from prmn_preselectie_recru_recruitment_c pprr
left join recru_recruitment rr on pprr.prmn_preselectie_recru_recruitmentrecru_recruitment_idb = rr.id
left join recru_recruitment_cstm rrc on rr.id = rrc.id_c
left join prmn_preselectie pp on pprr.prmn_preselectie_recru_recruitmentprmn_preselectie_ida = pp.id
left join prmn_preselectie pp1 on rrc.project_id_c = pp1.id
left join contacts c on rrc.kandidaat_rel_name_c = concat(c.first_name, ' ', c.last_name)
where -- rrc.opportunity_id1_c is not null and 
kandidaat_rel_name_c is not null
order by rr.name

select o.name, pp.name
from opportunities o left join prmn_preselectie pp on o.id = pp.id

create view temp_Can_Job_app_Comment as
(select c.id as canId, rrc.kandidaat_rel_name_c as canName, c.first_name, c.last_name, o.id as jobId, o.name as jobtitle, rrc.status_sollicitatieprocedure_c as jobStage,
 rrc.datum_geplaatst_c as datumGeplaatst, rr.id as recruId, rr.name as recruName, rr.date_entered, rr.date_modified, rr.description, rr.assigned_user_id,
 rrc.sourcing_status_c as sourcingStatus, pp.id as preselectieId, pp.name as preselectieName, rrc.account_id_c as CompId, a.name as companyName, rrc.status_kandidaat_c as statusCan
from recru_recruitment rr 
		left join recru_recruitment_cstm rrc on rr.id = rrc.id_c
        left join prmn_preselectie pp on rrc.project_id_c = pp.id
        left join contacts c on rrc.kandidaat_rel_name_c = concat(c.first_name, ' ', last_name)
        left join opportunities o on rrc.opportunity_id1_c = o.id
        left join accounts a on rrc.account_id_c = a.id
where -- rrc.opportunity_id1_c is not null and 
rrc.kandidaat_rel_name_c is not null and c.id not in (select contact_id from accounts_contacts)
order by rrc.kandidaat_rel_name_c)

select * from temp_Can_Job_app_Comment
        
select 
from prmn_preselectie_recru_recruitment_c pprr 
left join recru_recruitment_cstm rrc on prmn_preselectie_recru_recruitmentrecru_recruitment_idb = rrc.id_c
where rrc.kandidaat_rel_name_c is not null