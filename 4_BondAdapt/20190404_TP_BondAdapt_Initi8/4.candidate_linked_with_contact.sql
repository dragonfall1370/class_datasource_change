
-- CONTRACT
with
  contact0 (CLIENT,name,CONTACT,rn) as (SELECT cc.CLIENT, cg.name, cc.CONTACT,ROW_NUMBER() OVER(PARTITION BY CONTACT ORDER BY cc.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cc left join PROP_CLIENT_GEN cg on cg.reference = cc.client )
, contact as (select CLIENT, name, CONTACT from contact0 where rn = 1)

, con as (
       select
                ccc.CONTACT as 'contact-externalId', pg.person_id
              , replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
              , case when (replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
       from contact ccc
       left join PROP_PERSON_GEN pg on ccc.CONTACT = pg.reference
       --where pg.REFERENCE in (66096, 71174, 44530, 116689764500) or pg.person_id in (1136780)
       --where pg.REFERENCE in (116674167980) --(116656592765,144326,64844,76453,45315,116658192116,107089,81194,148714,192845,45276,41656,70761,44521,116674156720,74244,60062,51006,116656277811,74451,70958,116656572087,716701,80356,46641,114507,43472,69091,74449,82366,76386,388433,45090,113921,92851,218777,44486,116657199270,77507)
)
--select count(*) from con


-- CANDIDATE
, can as (
       select
                pg.REFERENCE as 'candidate-externalId', pg.person_id
              , Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'candidate-firstName'
              , Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'candidate-lastName'
              , cp.P_TEMP, cp.P_PERM, cp.P_CONTR
       from PROP_PERSON_GEN pg --where pg.person_id = 1136371 --44889 rows
       left join PROP_CAND_PREF cp on pg.REFERENCE = cp.REFERENCE
       --where pg.reference in (116674167980)
       --where pg.person_id in (1105381,1096967,1131988, 1094197,1131624, 1131780, 1131833, 1098818,1103933,1120519, 1092301, 1097091, 1103039, 1110897, 1124389, 1134289)
)
--select count(*) from can




select
       con.*
       , can.*
--select count(*)      
from con
inner join can on can.person_id = con.person_id
where can.person_id is not null
	
	
	
	