--list of industry
select distinct DESCRIPTION as industry
from PROP_IND_SECT ins INNER JOIN MD_MULTI_NAMES MN ON MN.ID = ins.INDUSTRY
where reference in (select REFERENCE from PROP_CAND_GEN) and mn.LANGUAGE=1 and DESCRIPTION is not null and DESCRIPTION <> ''

--list of candidate's industries
select concat('BB',ins.REFERENCE) as candexternalId, DESCRIPTION as industry
from PROP_IND_SECT ins INNER JOIN MD_MULTI_NAMES MN ON MN.ID = ins.INDUSTRY
where reference in (select REFERENCE from PROP_CAND_GEN) and LANGUAGE =1
order by ins.REFERENCE

--list of FE
select distinct DESCRIPTION as FE
from PROP_JOB_CAT JOB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JOB_CAT.JOB_CATEGORY
where reference in (select REFERENCE from PROP_CAND_GEN) and LANGUAGE=1

--list of candidate's FEs
select concat('BB',JOB_CAT.REFERENCE) as candexternalId, DESCRIPTION as FE
from PROP_JOB_CAT JOB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JOB_CAT.JOB_CATEGORY
where reference in (select REFERENCE from PROP_CAND_GEN) and LANGUAGE=1
order by JOB_CAT.REFERENCE
