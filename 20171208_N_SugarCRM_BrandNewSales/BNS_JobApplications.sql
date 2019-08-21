create view temp_jobApp as
(select rr.id,rrc.opportunity_id1_c,rrc.kandidaat_rel_name_c, max(rr.date_modified) as latestAction-- , rrc.opportunity_id1_c,rrc.status_sollicitatieprocedure_c ,rrc.kandidaat_rel_name_c
from recru_recruitment rr left join recru_recruitment_cstm rrc on rr.id = rrc.id_c
where opportunity_id1_c is not null and rrc.kandidaat_rel_name_c is not null
group by rrc.opportunity_id1_c,rrc.kandidaat_rel_name_c)  
-- select * from temp_jobApp

select concat('BNS_',tj.opportunity_id1_c) as 'application-positionExternalId',
		tj.kandidaat_rel_name_c, -- just to reference
		concat('BNS_',tc.id) as 'application-candidateExternalId',
		rrc.status_sollicitatieprocedure_c, -- just to reference
        case rrc.status_sollicitatieprocedure_c
			when 'Voorgesteld' then 'SENT'
            when 'Eerstegesprek' then '1ST_INTERVIEW'
            when 'Tweedegesprek' then '2ND_INTERVIEW'
            when 'Derdegesprek' then '2ND_INTERVIEW'
            when 'Arbeidsvoorwaardelijk' then 'OFFERED'
            when 'Test_Assesment' then 'OFFERED'
            when 'Aangenomen' then 'PLACED'
            else 'SHORTLISTED' END AS 'application-stage'
		, tj.kandidaat_rel_name_c, concat(tc.first_name, ' ', tc.last_name) as candidateName
from temp_jobApp tj
	left join recru_recruitment_cstm rrc on tj.id = rrc.id_c 
	left join temp_candidates tc on tj.kandidaat_rel_name_c = concat(tc.first_name, ' ', tc.last_name)
   -- where tc.id is not null: there is 1 row returns so we can remove that row from the csv later, this will make the script execution faster