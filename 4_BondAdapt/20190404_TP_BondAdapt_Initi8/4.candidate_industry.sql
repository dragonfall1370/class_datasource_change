/*
select
distinct trim(source.DESCRIPTION) as 'Source'
      , 1 as source_type
      , current_timestamp as insert_timestamp
      , 11 as payment_style
from PROP_PERSON_GEN pg --where pg.person_id = 1136371 --44889 rows
left join (SELECT REFERENCE, string_agg(MN.DESCRIPTION,',') as DESCRIPTION FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.SOURCE where MN.ID is not null and LANGUAGE = 10010 GROUP BY REFERENCE) source on pg.REFERENCE = source.REFERENCE --21159
where source.REFERENCE is not null
*/


with
 indsec as (select REFERENCE,DESCRIPTION from PROP_IND_SECT indsec INNER JOIN MD_MULTI_NAMES MN ON MN.ID = indsec.industry where LANGUAGE = 10010)



select --top 123
         pg.REFERENCE as 'candidate-externalId', pg.person_id
	, Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'candidate-firstName'
	, Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'candidate-lastName'
       , indsec.description
from PROP_PERSON_GEN pg --where pg.person_id = 1136371 --44889 rows
left join PROP_CAND_PREF cp on pg.REFERENCE = cp.REFERENCE
left join indsec ON indsec.REFERENCE = pg.reference -- industry
where indsec.REFERENCE is not null


