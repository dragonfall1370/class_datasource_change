
/*
-- COMMENTS
select top 50
       cg.REFERENCE as external_id
       , cg.name as 'company-name'
       , cast('-10' as int) as 'userid'
       , 'comment' as 'category'
       , 'company' as 'type'
       , doc.CREATED_DATE as 'insert_timestamp'
       , ltrim(replace(replace( [dbo].[udf_StripHTML](convert(varchar(max),convert(varbinary(max),DOCUMENT))) ,'Â',''),'ï»¿','')) as 'content'
       --, doc.*
from PROP_CLIENT_GEN cg
left join ( select OWNER_ID, DOC_CATEGORY, DOC_NAME, DOC_DESCRIPTION, FILE_EXTENSION, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY, NOTES, "DEFAULT", SIZE, STATUS, OWNER_TYPE, PREVIEW_TYPE, DOCUMENT from DOCUMENTS where FILE_EXTENSION in ('txt','rtf') ) doc on doc.OWNER_ID = cg.REFERENCE
--where (doc.doc is not null and doc.doc <> '') 
where cg.REFERENCE in (44563,74269,101104,161384,438706,440299,668791)
and doc.UPDATED_DATE is not null
--cg.name = 'PwC - PricewaterhouseCoopers Ltd (Hong Kong)'
*/


-- JOURNAL
with t (REFERENCE,type,ID,name) as (
              select REFERENCE, 'Company: ' as 'type', convert(varchar(max),client_id) as 'ID', name from PROP_CLIENT_GEN where REFERENCE is not null -- COMPANY
       UNION ALL
             select distinct ccc.CONTACT as REFERENCE, 'Contact: ' as 'type', convert(varchar(max),pg.person_id) as 'ID', pg.fullname from PROP_X_CLIENT_CON ccc left join PROP_PERSON_GEN pg on pg.REFERENCE = ccc.CONTACT where ccc.CONTACT is not null --and pg.REFERENCE = 63816--CONTACT
       UNION ALL
              select cp.REFERENCE
                     , case 
                            when P_PERM = 'Y' then 'Permanent Candidate: '
                            when P_CONTR = 'Y' then 'Contract Candidate: '
                            when P_TEMP = 'Y' then 'Temporary Candidate: '
                            end as 'type'
                     , convert(varchar(max),pg.person_id) as 'ID'
                     , pg.fullname
              -- select count(*) --29387   
              from PROP_CAND_PREF cp 
              left join PROP_PERSON_GEN pg on pg.REFERENCE = cp.REFERENCE 
              where cp.REFERENCE is not null --and cp.P_TEMP = 'Y' --CANDIDATE
       UNION ALL
              select cj.JOB
                     , case
                           when jobtype.DESCRIPTION in ('Contract','Lead Contract','Lead Temp','Temp Regular','Temp Shift') then 'Contract Job: '
                           when jobtype.DESCRIPTION in ('Direct','Lead Direct Job') then 'Permanent Job: '
                           end as 'type'
                     , convert(varchar(max),jg.job_id) as 'ID'
                     , jg.job_title 
              -- select count(*) --8076
              from PROP_X_CLIENT_JOB cj
              left join PROP_JOB_GEN jg on jg.REFERENCE  = cj.JOB
              left join (SELECT REFERENCE, string_agg( MN.DESCRIPTION, ',') as DESCRIPTION FROM PROP_JOB_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_JOB_GEN.JOB_TYPE where MN.ID is not null and LANGUAGE = 10010 GROUP BY REFERENCE) jobtype on jobtype.REFERENCE = cj.JOB
              where jg.REFERENCE is not null --JOB
)
--select * from t where REFERENCE in (63816)
--select top 10 * from t where ENTITY_ID = 63816 
--select count(*) from t


, content as (
       SELECT
                J.JOURNAL_ID, J.ENTITY_ID, t0.ID
              , JE.creation_date as 'insert_timestamp'             
              , Stuff(   + Coalesce('Workflow Name: ' + NULLIF(convert(nvarchar(max),bo.description), '') + char(10), '')
                         + Coalesce('User: ' + NULLIF(convert(nvarchar(max),eg.name ), '') + char(10), '')
                         + Coalesce('Notes: ' + NULLIF(convert(nvarchar(max),[dbo].[udf_StripHTML](JE.J_NOTES) ), '') + char(10), '')
                         --+ Coalesce('Job: ' + NULLIF(convert(nvarchar(max),jg.job_title ), '') + char(10), '')
                         --+ Coalesce('Contact: ' + NULLIF(pg2.fullname, '') + char(10), '')
                         --+ Coalesce('Candidate: ' + NULLIF(pg3.fullname, '') + char(10), '')
                         --+ Coalesce('External Interview: ' + NULLIF(convert(nvarchar(max),eg.name ), '') + char(10), '')
                         --+ Coalesce('Client: ' + NULLIF(convert(nvarchar(max),cg.name ), '') + char(10), '')
                         --+ Coalesce('Progress: ' + NULLIF(convert(nvarchar(max),eg.name ), '') + char(10), '')
                         --+ Coalesce('Documents: ' + NULLIF(convert(nvarchar(max),eg.name ), '') + char(10), '')
                         + Coalesce( t1.type + NULLIF(convert(nvarchar(max),t1.name), '') + char(10), '')
                         + Coalesce( t2.type + NULLIF(convert(nvarchar(max),t2.name), '') + char(10), '')
                         + Coalesce( t3.type + NULLIF(convert(nvarchar(max),t3.name), '') + char(10), '')
                         + Coalesce( t4.type + NULLIF(convert(nvarchar(max),t4.name), '') + char(10), '')
                         + Coalesce( t5.type + NULLIF(convert(nvarchar(max),t5.name), '') + char(10), '')
                         + Coalesce( t6.type + NULLIF(convert(nvarchar(max),t6.name), '') + char(10), '')
                       , 1, 0, '') as 'content'
              , je.entity_id_1, je.entity_id_2, je.entity_id_3, je.entity_id_4, je.entity_id_5, je.entity_id_6 
              , t1.reference as t1r, t2.reference as t2r, t3.reference as t3r, t4.reference as t4r                       
       -- select COUNT(*) --46128 -- select *
       FROM LK_ENTITIES_JOURNAL J
       left join JOURNAL_ENTRIES JE ON JE.ID = J.journal_id
       left join dbo.JOURNAL_NOTES jn on jn.journal_id = J.journal_id --where J.JOURNAL_ID in (4341050649)
       left join prop_employee_gen eg on eg.user_ref = je.creator_id 
       left join (select ID, description from MD_MULTI_NAMES MN where LANGUAGE = 10010) bo on bo.id = je.bo_id 
       left join (select distinct REFERENCE, ID from t) t0 on t0.REFERENCE = J.ENTITY_ID
       left join t t1 on t1.REFERENCE = je.entity_id_1
       left join t t2 on t2.REFERENCE = je.entity_id_2
       left join t t3 on t3.REFERENCE = je.entity_id_3
       left join t t4 on t4.REFERENCE = je.entity_id_4
       left join t t5 on t5.REFERENCE = je.entity_id_5
       left join t t6 on t6.REFERENCE = je.entity_id_6
)
--select top 100 * from content where ID = 1098818 --ENTITY_ID = 63816 


select --top 3
       c.JOURNAL_ID, c.ENTITY_ID, c.ID
       ,  case 
--              when cg.reference is not null then cg.reference
--              when cc.contact is not null then cc.contact
              when jg.reference is not null then jg.reference
--              when ca.candidate is not null then ca.candidate
              end as 'external_ID'
        , case 
--              when cg.reference is not null then 'company'
--              when cc.contact is not null then 'contact'
              when jg.reference is not null then 'job'
--              when ca.candidate is not null then 'candidate'
              end as 'type'
       , cast('-10' as int) as 'user_account_id'
       , 'comment' as 'category'
       , c.insert_timestamp, c.content
       , c.entity_id_1, c.entity_id_2, c.entity_id_3, c.entity_id_4, c.entity_id_5, c.entity_id_6
       , c.t1r, c.t2r, c.t3r, c.t4r
from content c
--left join PROP_CLIENT_GEN cg on cg.reference = c.ENTITY_ID --COMPANY
--left join (select distinct contact from PROP_X_CLIENT_CON) cc on cc.contact = c.ENTITY_ID --CONTACT
left join PROP_JOB_GEN jg on jg.reference = c.ENTITY_ID --JOB
--left join PROP_X_CAND_AVAIL ca on ca.candidate = c.ENTITY_ID --CANDIDATE

where
--cg.reference is not null and 
--cc.contact is not null and 
jg.reference is not null and 
--ca.candidate is not null and 

--c.JOURNAL_ID in (4341050649)
--c.ENTITY_ID in (64019)
--c.content like '%Contact: %' and

c.ID in ('887856')
--cg.client_id in (1096903)
--cc.client in (1094197,1107874)
--jg.JOB_ID in (881576)
--ca.candidate in ()


/*
pg3.person_id = 1102307
WHERE cg0.reference is not null and cg.reference is not null
--and je.entity_id_4 is not null and je.entity_id_4 <> 0
--and jn.journal_id is not null
and J.ENTITY_ID = 116658770254 


select * from PROP_CLIENT_GEN where reference in (38078,40404,40421); -- COMPANY
select distinct client from PROP_X_CLIENT_CON where client in (38078,40404,40421) -- select * from PROP_X_CLIENT_CON --; select * from PROP_CONT_GEN where reference in (260350,114534);
select * from PROP_PERSON_GEN where reference in (38078,40404,40421) --or person_id in (1094197); --person
select * from PROP_JOB_GEN jg where reference in (38078,40404,40421);  --JOB


select *
select distinct je.entity_id_6
FROM LK_ENTITIES_JOURNAL J 
left join dbo.JOURNAL_NOTES jn on jn.journal_id = J.journal_id
INNER JOIN JOURNAL_ENTRIES JE ON JE.ID = J.JOURNAL_ID 
where j.entity_id = 116658770254
--where jn.journal_id is not null
where je.j_notes like '%Cristiano Cimino%' or je.j_document like '%Cristiano Cimino%' or jn.j_document like '%Cristiano Cimino%'

*/

