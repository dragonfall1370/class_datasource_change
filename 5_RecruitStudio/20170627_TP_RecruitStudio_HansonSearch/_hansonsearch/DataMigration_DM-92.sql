-- COUNTING FUNCTIONALL EXPERTISE
with t as (select c.id as vincere_id,c.external_id, c.first_name, c.last_name
               --, cfe.* --, cfe.functional_expertise_id, cfe.sub_functional_expertise_id 
               , fe.name as fe, sfe.name as sfe
               , ci.name as ind
           from candidate_functional_expertise cfe
           left join candidate c on c.id = cfe.candidate_id
           left join functional_expertise fe on fe.id = cfe.functional_expertise_id
           left join (select ci.candidate_id, v.name from candidate_industry ci left join vertical v on ci.vertical_id = v.id ) ci on ci.candidate_id = c.id
           left join sub_functional_expertise sfe on sfe.id = cfe.sub_functional_expertise_id
           where c.external_id is not null 
           --where c.external_id in ('110387-4971-1110','100593-8845-12136') -- ('110998-3207-1554','110452-3164-11130','110393-3899-1662','110387-4971-1110','110362-5188-1540','110256-8229-9337','110245-1034-8294','110206-7622-13100','110129-5026-15322','110046-5356-15347','')
           --where fe.name = 'Corporate PR' --and c.first_name = 'William'
           --limit 10
          )
--select * from t where sfe = ''
--select distinct external_id from t where sfe = ''
--select fe,count(*) as AMOUNT from t group by fe Order By fe

--select ind,count(*) as AMOUNT from t where sfe like '%Medical Education%' group by ind Order By ind 
select * from t where sfe like '%edical%ducation%' --and ind is null